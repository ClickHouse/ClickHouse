---
toc_priority: 21
toc_title: OnTime
---

# OnTime {#ontime}

This dataset can be obtained in two ways:

-   import from raw data
-   download of prepared partitions

## Import from Raw Data {#import-from-raw-data}

Downloading data:

``` bash
wget --no-check-certificate --continue https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{1987..2021}_{1..12}.zip
```

Creating a table:

``` sql
CREATE TABLE `ontime`
(
    `Year`                            UInt16,
    `Quarter`                         UInt8,
    `Month`                           UInt8,
    `DayofMonth`                      UInt8,
    `DayOfWeek`                       UInt8,
    `FlightDate`                      Date,
    `Reporting_Airline`               String,
    `DOT_ID_Reporting_Airline`        Int32,
    `IATA_CODE_Reporting_Airline`     String,
    `Tail_Number`                     String,
    `Flight_Number_Reporting_Airline` String,
    `OriginAirportID`                 Int32,
    `OriginAirportSeqID`              Int32,
    `OriginCityMarketID`              Int32,
    `Origin`                          FixedString(5),
    `OriginCityName`                  String,
    `OriginState`                     FixedString(2),
    `OriginStateFips`                 String,
    `OriginStateName`                 String,
    `OriginWac`                       Int32,
    `DestAirportID`                   Int32,
    `DestAirportSeqID`                Int32,
    `DestCityMarketID`                Int32,
    `Dest`                            FixedString(5),
    `DestCityName`                    String,
    `DestState`                       FixedString(2),
    `DestStateFips`                   String,
    `DestStateName`                   String,
    `DestWac`                         Int32,
    `CRSDepTime`                      Int32,
    `DepTime`                         Int32,
    `DepDelay`                        Int32,
    `DepDelayMinutes`                 Int32,
    `DepDel15`                        Int32,
    `DepartureDelayGroups`            String,
    `DepTimeBlk`                      String,
    `TaxiOut`                         Int32,
    `WheelsOff`                       Int32,
    `WheelsOn`                        Int32,
    `TaxiIn`                          Int32,
    `CRSArrTime`                      Int32,
    `ArrTime`                         Int32,
    `ArrDelay`                        Int32,
    `ArrDelayMinutes`                 Int32,
    `ArrDel15`                        Int32,
    `ArrivalDelayGroups`              Int32,
    `ArrTimeBlk`                      String,
    `Cancelled`                       UInt8,
    `CancellationCode`                FixedString(1),
    `Diverted`                        UInt8,
    `CRSElapsedTime`                  Int32,
    `ActualElapsedTime`               Int32,
    `AirTime`                         Nullable(Int32),
    `Flights`                         Int32,
    `Distance`                        Int32,
    `DistanceGroup`                   UInt8,
    `CarrierDelay`                    Int32,
    `WeatherDelay`                    Int32,
    `NASDelay`                        Int32,
    `SecurityDelay`                   Int32,
    `LateAircraftDelay`               Int32,
    `FirstDepTime`                    String,
    `TotalAddGTime`                   String,
    `LongestAddGTime`                 String,
    `DivAirportLandings`              String,
    `DivReachedDest`                  String,
    `DivActualElapsedTime`            String,
    `DivArrDelay`                     String,
    `DivDistance`                     String,
    `Div1Airport`                     String,
    `Div1AirportID`                   Int32,
    `Div1AirportSeqID`                Int32,
    `Div1WheelsOn`                    String,
    `Div1TotalGTime`                  String,
    `Div1LongestGTime`                String,
    `Div1WheelsOff`                   String,
    `Div1TailNum`                     String,
    `Div2Airport`                     String,
    `Div2AirportID`                   Int32,
    `Div2AirportSeqID`                Int32,
    `Div2WheelsOn`                    String,
    `Div2TotalGTime`                  String,
    `Div2LongestGTime`                String,
    `Div2WheelsOff`                   String,
    `Div2TailNum`                     String,
    `Div3Airport`                     String,
    `Div3AirportID`                   Int32,
    `Div3AirportSeqID`                Int32,
    `Div3WheelsOn`                    String,
    `Div3TotalGTime`                  String,
    `Div3LongestGTime`                String,
    `Div3WheelsOff`                   String,
    `Div3TailNum`                     String,
    `Div4Airport`                     String,
    `Div4AirportID`                   Int32,
    `Div4AirportSeqID`                Int32,
    `Div4WheelsOn`                    String,
    `Div4TotalGTime`                  String,
    `Div4LongestGTime`                String,
    `Div4WheelsOff`                   String,
    `Div4TailNum`                     String,
    `Div5Airport`                     String,
    `Div5AirportID`                   Int32,
    `Div5AirportSeqID`                Int32,
    `Div5WheelsOn`                    String,
    `Div5TotalGTime`                  String,
    `Div5LongestGTime`                String,
    `Div5WheelsOff`                   String,
    `Div5TailNum`                     String
) ENGINE = MergeTree
      PARTITION BY Year
      ORDER BY (IATA_CODE_Reporting_Airline, FlightDate)
      SETTINGS index_granularity = 8192;
```

Loading data with multiple threads:

``` bash
ls -1 *.zip | xargs -I{} -P $(nproc) bash -c "echo {}; unzip -cq {} '*.csv' | sed 's/\.00//g' | clickhouse-client --input_format_with_names_use_header=0 --query='INSERT INTO ontime FORMAT CSVWithNames'"
```

(if you will have memory shortage or other issues on your server, remove the `-P $(nproc)` part)

## Download of Prepared Partitions {#download-of-prepared-partitions}

``` bash
$ curl -O https://datasets.clickhouse.com/ontime/partitions/ontime.tar
$ tar xvf ontime.tar -C /var/lib/clickhouse # path to ClickHouse data directory
$ # check permissions of unpacked data, fix if required
$ sudo service clickhouse-server restart
$ clickhouse-client --query "select count(*) from datasets.ontime"
```

!!! info "Info"
    If you will run the queries described below, you have to use the full table name, `datasets.ontime`.

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

You can also play with the data in Playground, [example](https://gh-api.clickhouse.com/play?user=play#U0VMRUNUIERheU9mV2VlaywgY291bnQoKikgQVMgYwpGUk9NIG9udGltZQpXSEVSRSBZZWFyPj0yMDAwIEFORCBZZWFyPD0yMDA4CkdST1VQIEJZIERheU9mV2VlawpPUkRFUiBCWSBjIERFU0M7Cg==).

This performance test was created by Vadim Tkachenko. See:

-   https://www.percona.com/blog/2009/10/02/analyzing-air-traffic-performance-with-infobright-and-monetdb/
-   https://www.percona.com/blog/2009/10/26/air-traffic-queries-in-luciddb/
-   https://www.percona.com/blog/2009/11/02/air-traffic-queries-in-infinidb-early-alpha/
-   https://www.percona.com/blog/2014/04/21/using-apache-hadoop-and-impala-together-with-mysql-for-data-analysis/
-   https://www.percona.com/blog/2016/01/07/apache-spark-with-air-ontime-performance-data/
-   http://nickmakos.blogspot.ru/2012/08/analyzing-air-traffic-performance-with.html

[Original article](https://clickhouse.com/docs/en/getting_started/example_datasets/ontime/) <!--hide-->
