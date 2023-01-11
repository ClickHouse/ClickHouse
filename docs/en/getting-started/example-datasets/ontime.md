---
sidebar_label: OnTime Airline Flight Data
description: Dataset containing the on-time performance of airline flights
---

# OnTime 

This dataset contains data from Bureau of Transportation Statistics.

## Creating a table

``` sql
CREATE TABLE `ontime`
(
    `Year`                            UInt16,
    `Quarter`                         UInt8,
    `Month`                           UInt8,
    `DayofMonth`                      UInt8,
    `DayOfWeek`                       UInt8,
    `FlightDate`                      Date,
    `Reporting_Airline`               LowCardinality(String),
    `DOT_ID_Reporting_Airline`        Int32,
    `IATA_CODE_Reporting_Airline`     LowCardinality(String),
    `Tail_Number`                     LowCardinality(String),
    `Flight_Number_Reporting_Airline` LowCardinality(String),
    `OriginAirportID`                 Int32,
    `OriginAirportSeqID`              Int32,
    `OriginCityMarketID`              Int32,
    `Origin`                          FixedString(5),
    `OriginCityName`                  LowCardinality(String),
    `OriginState`                     FixedString(2),
    `OriginStateFips`                 FixedString(2),
    `OriginStateName`                 LowCardinality(String),
    `OriginWac`                       Int32,
    `DestAirportID`                   Int32,
    `DestAirportSeqID`                Int32,
    `DestCityMarketID`                Int32,
    `Dest`                            FixedString(5),
    `DestCityName`                    LowCardinality(String),
    `DestState`                       FixedString(2),
    `DestStateFips`                   FixedString(2),
    `DestStateName`                   LowCardinality(String),
    `DestWac`                         Int32,
    `CRSDepTime`                      Int32,
    `DepTime`                         Int32,
    `DepDelay`                        Int32,
    `DepDelayMinutes`                 Int32,
    `DepDel15`                        Int32,
    `DepartureDelayGroups`            LowCardinality(String),
    `DepTimeBlk`                      LowCardinality(String),
    `TaxiOut`                         Int32,
    `WheelsOff`                       LowCardinality(String),
    `WheelsOn`                        LowCardinality(String),
    `TaxiIn`                          Int32,
    `CRSArrTime`                      Int32,
    `ArrTime`                         Int32,
    `ArrDelay`                        Int32,
    `ArrDelayMinutes`                 Int32,
    `ArrDel15`                        Int32,
    `ArrivalDelayGroups`              LowCardinality(String),
    `ArrTimeBlk`                      LowCardinality(String),
    `Cancelled`                       Int8,
    `CancellationCode`                FixedString(1),
    `Diverted`                        Int8,
    `CRSElapsedTime`                  Int32,
    `ActualElapsedTime`               Int32,
    `AirTime`                         Int32,
    `Flights`                         Int32,
    `Distance`                        Int32,
    `DistanceGroup`                   Int8,
    `CarrierDelay`                    Int32,
    `WeatherDelay`                    Int32,
    `NASDelay`                        Int32,
    `SecurityDelay`                   Int32,
    `LateAircraftDelay`               Int32,
    `FirstDepTime`                    Int16,
    `TotalAddGTime`                   Int16,
    `LongestAddGTime`                 Int16,
    `DivAirportLandings`              Int8,
    `DivReachedDest`                  Int8,
    `DivActualElapsedTime`            Int16,
    `DivArrDelay`                     Int16,
    `DivDistance`                     Int16,
    `Div1Airport`                     LowCardinality(String),
    `Div1AirportID`                   Int32,
    `Div1AirportSeqID`                Int32,
    `Div1WheelsOn`                    Int16,
    `Div1TotalGTime`                  Int16,
    `Div1LongestGTime`                Int16,
    `Div1WheelsOff`                   Int16,
    `Div1TailNum`                     LowCardinality(String),
    `Div2Airport`                     LowCardinality(String),
    `Div2AirportID`                   Int32,
    `Div2AirportSeqID`                Int32,
    `Div2WheelsOn`                    Int16,
    `Div2TotalGTime`                  Int16,
    `Div2LongestGTime`                Int16,
    `Div2WheelsOff`                   Int16,
    `Div2TailNum`                     LowCardinality(String),
    `Div3Airport`                     LowCardinality(String),
    `Div3AirportID`                   Int32,
    `Div3AirportSeqID`                Int32,
    `Div3WheelsOn`                    Int16,
    `Div3TotalGTime`                  Int16,
    `Div3LongestGTime`                Int16,
    `Div3WheelsOff`                   Int16,
    `Div3TailNum`                     LowCardinality(String),
    `Div4Airport`                     LowCardinality(String),
    `Div4AirportID`                   Int32,
    `Div4AirportSeqID`                Int32,
    `Div4WheelsOn`                    Int16,
    `Div4TotalGTime`                  Int16,
    `Div4LongestGTime`                Int16,
    `Div4WheelsOff`                   Int16,
    `Div4TailNum`                     LowCardinality(String),
    `Div5Airport`                     LowCardinality(String),
    `Div5AirportID`                   Int32,
    `Div5AirportSeqID`                Int32,
    `Div5WheelsOn`                    Int16,
    `Div5TotalGTime`                  Int16,
    `Div5LongestGTime`                Int16,
    `Div5WheelsOff`                   Int16,
    `Div5TailNum`                     LowCardinality(String)
) ENGINE = MergeTree
  ORDER BY (Year, Quarter, Month, DayofMonth, FlightDate, IATA_CODE_Reporting_Airline);
```

## Import from Raw Data {#import-from-raw-data}

Downloading data:

``` bash
wget --no-check-certificate --continue https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{1987..2022}_{1..12}.zip
```

Loading data with multiple threads:

``` bash
ls -1 *.zip | xargs -I{} -P $(nproc) bash -c "echo {}; unzip -cq {} '*.csv' | sed 's/\.00//g' | clickhouse-client --input_format_csv_empty_as_default 1 --query='INSERT INTO ontime FORMAT CSVWithNames'"
```

(if you will have memory shortage or other issues on your server, remove the `-P $(nproc)` part)

## Import from a saved copy

Alternatively, you can import data from a saved copy by the following query:

```
INSERT INTO ontime SELECT * FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/ontime/csv_by_year/*.csv.gz', CSVWithNames) SETTINGS max_insert_threads = 40;
```

The snapshot was created on 2022-05-29.

## Queries {#queries}

Q0.

``` sql
SELECT avg(c1)
FROM
(
    SELECT Year, Month, count(*) AS c1
    FROM ontime
    GROUP BY Year, Month
);
```

Q1. The number of flights per day from the year 2000 to 2008

``` sql
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE Year>=2000 AND Year<=2008
GROUP BY DayOfWeek
ORDER BY c DESC;
```

Q2. The number of flights delayed by more than 10 minutes, grouped by the day of the week, for 2000-2008

``` sql
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2000 AND Year<=2008
GROUP BY DayOfWeek
ORDER BY c DESC;
```

Q3. The number of delays by the airport for 2000-2008

``` sql
SELECT Origin, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2000 AND Year<=2008
GROUP BY Origin
ORDER BY c DESC
LIMIT 10;
```

Q4. The number of delays by carrier for 2007

``` sql
SELECT IATA_CODE_Reporting_Airline AS Carrier, count(*)
FROM ontime
WHERE DepDelay>10 AND Year=2007
GROUP BY Carrier
ORDER BY count(*) DESC;
```

Q5. The percentage of delays by carrier for 2007

``` sql
SELECT Carrier, c, c2, c*100/c2 as c3
FROM
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c
    FROM ontime
    WHERE DepDelay>10
        AND Year=2007
    GROUP BY Carrier
) q
JOIN
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c2
    FROM ontime
    WHERE Year=2007
    GROUP BY Carrier
) qq USING Carrier
ORDER BY c3 DESC;
```

Better version of the same query:

``` sql
SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay>10)*100 AS c3
FROM ontime
WHERE Year=2007
GROUP BY Carrier
ORDER BY c3 DESC
```

Q6. The previous request for a broader range of years, 2000-2008

``` sql
SELECT Carrier, c, c2, c*100/c2 as c3
FROM
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c
    FROM ontime
    WHERE DepDelay>10
        AND Year>=2000 AND Year<=2008
    GROUP BY Carrier
) q
JOIN
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c2
    FROM ontime
    WHERE Year>=2000 AND Year<=2008
    GROUP BY Carrier
) qq USING Carrier
ORDER BY c3 DESC;
```

Better version of the same query:

``` sql
SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay>10)*100 AS c3
FROM ontime
WHERE Year>=2000 AND Year<=2008
GROUP BY Carrier
ORDER BY c3 DESC;
```

Q7. Percentage of flights delayed for more than 10 minutes, by year

``` sql
SELECT Year, c1/c2
FROM
(
    select
        Year,
        count(*)*100 as c1
    from ontime
    WHERE DepDelay>10
    GROUP BY Year
) q
JOIN
(
    select
        Year,
        count(*) as c2
    from ontime
    GROUP BY Year
) qq USING (Year)
ORDER BY Year;
```

Better version of the same query:

``` sql
SELECT Year, avg(DepDelay>10)*100
FROM ontime
GROUP BY Year
ORDER BY Year;
```

Q8. The most popular destinations by the number of directly connected cities for various year ranges

``` sql
SELECT DestCityName, uniqExact(OriginCityName) AS u
FROM ontime
WHERE Year >= 2000 and Year <= 2010
GROUP BY DestCityName
ORDER BY u DESC LIMIT 10;
```

Q9.

``` sql
SELECT Year, count(*) AS c1
FROM ontime
GROUP BY Year;
```

Q10.

``` sql
SELECT
   min(Year), max(Year), IATA_CODE_Reporting_Airline AS Carrier, count(*) AS cnt,
   sum(ArrDelayMinutes>30) AS flights_delayed,
   round(sum(ArrDelayMinutes>30)/count(*),2) AS rate
FROM ontime
WHERE
   DayOfWeek NOT IN (6,7) AND OriginState NOT IN ('AK', 'HI', 'PR', 'VI')
   AND DestState NOT IN ('AK', 'HI', 'PR', 'VI')
   AND FlightDate < '2010-01-01'
GROUP by Carrier
HAVING cnt>100000 and max(Year)>1990
ORDER by rate DESC
LIMIT 1000;
```

Bonus:

``` sql
SELECT avg(cnt)
FROM
(
    SELECT Year,Month,count(*) AS cnt
    FROM ontime
    WHERE DepDel15=1
    GROUP BY Year,Month
);

SELECT avg(c1) FROM
(
    SELECT Year,Month,count(*) AS c1
    FROM ontime
    GROUP BY Year,Month
);

SELECT DestCityName, uniqExact(OriginCityName) AS u
FROM ontime
GROUP BY DestCityName
ORDER BY u DESC
LIMIT 10;

SELECT OriginCityName, DestCityName, count() AS c
FROM ontime
GROUP BY OriginCityName, DestCityName
ORDER BY c DESC
LIMIT 10;

SELECT OriginCityName, count() AS c
FROM ontime
GROUP BY OriginCityName
ORDER BY c DESC
LIMIT 10;
```

You can also play with the data in Playground, [example](https://play.clickhouse.com/play?user=play#U0VMRUNUIERheU9mV2VlaywgY291bnQoKikgQVMgYwpGUk9NIG9udGltZQpXSEVSRSBZZWFyPj0yMDAwIEFORCBZZWFyPD0yMDA4CkdST1VQIEJZIERheU9mV2VlawpPUkRFUiBCWSBjIERFU0M7Cg==).

This performance test was created by Vadim Tkachenko. See:

-   https://www.percona.com/blog/2009/10/02/analyzing-air-traffic-performance-with-infobright-and-monetdb/
-   https://www.percona.com/blog/2009/10/26/air-traffic-queries-in-luciddb/
-   https://www.percona.com/blog/2009/11/02/air-traffic-queries-in-infinidb-early-alpha/
-   https://www.percona.com/blog/2014/04/21/using-apache-hadoop-and-impala-together-with-mysql-for-data-analysis/
-   https://www.percona.com/blog/2016/01/07/apache-spark-with-air-ontime-performance-data/
-   http://nickmakos.blogspot.ru/2012/08/analyzing-air-traffic-performance-with.html

[Original article](https://clickhouse.com/docs/en/getting_started/example_datasets/ontime/) <!--hide-->
