from pyspark.shell import spark

"""
sample:- fiscal_calendar output. 
+-------------+--------------------+-------------------------------+--------------+-------------------------+-----------+-----------+--------------+-------------+------------------------+-----------------------+---------------------------+--------------------------+
|calendar_date|calendar_day_of_week|calendar_day_of_week_short_name|calendar_month|calendar_month_short_name|fiscal_year|fiscal_week|fiscal_quarter|fiscal_period|first_day_of_fiscal_week|last_day_of_fiscal_week|first_day_of_fiscal_quarter|last_day_of_fiscal_quarter|
+-------------+--------------------+-------------------------------+--------------+-------------------------+-----------+-----------+--------------+-------------+------------------------+-----------------------+---------------------------+--------------------------+
|   2022-01-31|              Monday|                            Mon|      February|                      Feb|       2022|          1|             1|            1|                       1|                      0|                          1|                         0|
|   2022-02-01|             Tuesday|                            Tue|      February|                      Feb|       2022|          1|             1|            1|                       0|                      0|                          0|                         0|
|   2022-02-02|           Wednesday|                            Wed|      February|                      Feb|       2022|          1|             1|            1|                       0|                      0|                          0|                         0|
|   2022-02-03|            Thursday|                            Thu|      February|                      Feb|       2022|          1|             1|            1|                       0|                      0|                          0|                         0|
|   2022-02-04|              Friday|                            Fri|      February|                      Feb|       2022|          1|             1|            1|                       0|                      0|                          0|                         0|
+-------------+--------------------+-------------------------------+--------------+-------------------------+-----------+-----------+--------------+-------------+------------------------+-----------------------+---------------------------+--------------------------+

"""

spark.sql("""
create or replace temp view normal_calendar AS
SELECT
 date as calendar_date
,date_format(date, 'EEEE') as calendar_day_of_week
,date_format(date, 'E') as calendar_day_of_week_short_name
,date_format(date, 'MMMM') as calendar_month  
,date_format(date, 'MMM') as calendar_month_short_name 
,date_part('year',date) as fiscal_year 
,date_part('week',date) as fiscal_week
,CASE when  date_part('week',date) between 1 and 26 then 'FH' else 'BH' END AS fiscal_half
,CASE
        WHEN date_part('week',date) BETWEEN 1  AND 13  THEN 1
        WHEN date_part('week',date) BETWEEN 14  AND 26  THEN 2
        WHEN date_part('week',date) BETWEEN 27  AND 39  THEN 3
        ELSE 4  
END AS fiscal_quarter
,CASE
        WHEN date_part('week',date) between 1 and 8 THEN 1
        WHEN date_part('week',date) between 9 and 17  THEN 2
        WHEN date_part('week',date) between 18 and 26  THEN 3
        WHEN date_part('week',date) between 27 and 34  THEN 4
        WHEN date_part('week',date) between  35 and 43 THEN 5
        else 6
END AS fiscal_period
,CASE when date_format(date, 'E') == 'Mon' Then 1 else 0 END as first_day_of_fiscal_week
,CASE when date_format(date, 'E') == 'Sun' Then 1 else 0 END as last_day_of_fiscal_week
,case 
    when (date_format(to_date(date_trunc('quarter',date)),'E') == 'Mon') 
    AND to_date(date) == to_date(date_trunc('quarter',date)) then 1
    when (date_format(date_trunc('quarter',date) + interval 1 day,'E') == 'Mon') 
    AND to_date(date) == to_date(date_trunc('quarter',date)  + interval 1 day ) then 1
    when (date_format(date_trunc('quarter',date) + interval 2 day,'E') == 'Mon') 
    AND to_date(date) == to_date(date_trunc('quarter',date)  + interval 2 day ) then 1
    when (date_format(date_trunc('quarter',date) + interval 3 day,'E') == 'Mon') 
    AND to_date(date) == to_date(date_trunc('quarter',date)  + interval 3 day ) then 1
    when (date_format(date_trunc('quarter',date) + interval 4 day,'E') == 'Mon') 
    AND to_date(date) == to_date(date_trunc('quarter',date)  + interval 4 day ) then 1
    when (date_format(date_trunc('quarter',date) + interval 5 day,'E') == 'Mon') 
    AND to_date(date) == to_date(date_trunc('quarter',date)  + interval 5 day ) then 1
    when (date_format(date_trunc('quarter',date) + interval 6 day,'E') == 'Mon') 
    AND to_date(date) == to_date(date_trunc('quarter',date)  + interval 6 day ) then 1
     else 0 end as first_day_of_fiscal_quarter
FROM 
(
    select explode(date) as date from 
    (SELECT sequence(to_date('2010-12-01'), to_date('2041-02-01'), interval 1 day) as date) temp
) temp1
""")

#  feb month as start of the fiscal week.
spark.sql("""
create or replace temp view fiscal_calendar as 
select * from (select 
     calendar_date
    ,calendar_day_of_week
    ,calendar_day_of_week_short_name
    ,CASE WHEN fiscal_week between 1 and 4 THEN 'February' 
      WHEN fiscal_week between 5 and 8 THEN 'March'
      WHEN fiscal_week between 9 and 13  THEN 'April' 
      WHEN fiscal_week between 14 and 17  THEN 'May'
      WHEN fiscal_week between 18 and 21  THEN 'June' 
      WHEN fiscal_week between 22 and 26  THEN 'July'
      WHEN fiscal_week between 27 and 30  THEN 'August' 
      WHEN fiscal_week between 31 and 34  THEN 'September'
      WHEN fiscal_week between 35 and 39  THEN 'October' 
      WHEN fiscal_week between 40 and 43  THEN 'November'
      WHEN fiscal_week between 44 and 47  THEN 'December' 
      ELSE 'January' END 
     as calendar_month
    ,CASE WHEN fiscal_week between 1 and 4 THEN 'Feb' 
          WHEN fiscal_week  between 5 and 8 THEN 'Mar'
          WHEN fiscal_week  between 9 and 13 THEN 'Apr' 
          WHEN fiscal_week between 14 and 17  THEN 'May'
          WHEN fiscal_week between 18 and 21 THEN 'Jun' 
          WHEN fiscal_week between 22 and 26 THEN 'Jul'
          WHEN fiscal_week between 27 and 30 THEN 'Aug' 
          WHEN fiscal_week between 31 and 34 THEN 'Sep' 
          WHEN fiscal_week between 35 and 39 THEN 'Oct' 
          WHEN fiscal_week between 40 and 43 THEN 'Nov'
          WHEN fiscal_week between 44 and 47 THEN 'Dec' 
          ELSE 'Jan' END 
      as calendar_month_short_name
    , CASE WHEN fiscal_week = 1 and date_part('year',calendar_date) > fiscal_year then (fiscal_year + 1)
          WHEN fiscal_week = 52 and date_part('year',calendar_date) = fiscal_year then (fiscal_year - 1)
          WHEN fiscal_week = 53 and date_part('year',calendar_date) = fiscal_year then (fiscal_year - 1)
          ELSE fiscal_year
    END as fiscal_year
    ,fiscal_week
    ,fiscal_quarter
    ,fiscal_period
    ,first_day_of_fiscal_week
    ,last_day_of_fiscal_week
    ,first_day_of_fiscal_quarter 
    ,lead(first_day_of_fiscal_quarter) over (order by calendar_date) as last_day_of_fiscal_quarter
  from (select 
           calendar_date
          ,calendar_day_of_week 
          ,calendar_day_of_week_short_name  
          ,lag(fiscal_year, 28) over (order by calendar_date) as fiscal_year
          ,lag(fiscal_week, 28) over (order by calendar_date) as fiscal_week
          ,lag(fiscal_quarter, 28) over (order by calendar_date) as fiscal_quarter
          ,lag(fiscal_period, 28) over (order by calendar_date) as fiscal_period
          ,lag(first_day_of_fiscal_week, 28) over (order by calendar_date) as first_day_of_fiscal_week
          ,lag(last_day_of_fiscal_week, 28) over (order by calendar_date) as last_day_of_fiscal_week
          ,lag(first_day_of_fiscal_quarter, 28) over (order by calendar_date) as first_day_of_fiscal_quarter
        from normal_calendar 
) s ) t where fiscal_year > 2010 and fiscal_year < 2041
""")

spark.sql("""select * from fiscal_calendar where fiscal_year = 2022 """).show(365)
