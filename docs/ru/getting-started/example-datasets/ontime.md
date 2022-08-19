---
sidebar_position: 21
sidebar_label: OnTime
---

# OnTime {#ontime}

Этот датасет может быть получен двумя способами:

-   импорт из сырых данных;
-   скачивание готовых партиций.

## Импорт из сырых данных {#import-iz-syrykh-dannykh}

Скачивание данных (из `https://github.com/Percona-Lab/ontime-airline-performance/blob/master/download.sh`):

``` bash
wget --no-check-certificate --continue https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{1987..2021}_{1..12}.zip
```

Создание таблицы:

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

Загрузка данных:

``` bash
ls -1 *.zip | xargs -I{} -P $(nproc) bash -c "echo {}; unzip -cq {} '*.csv' | sed 's/\.00//g' | clickhouse-client --input_format_with_names_use_header=0 --query='INSERT INTO ontime FORMAT CSVWithNames'"
```

## Скачивание готовых партиций {#skachivanie-gotovykh-partitsii}

``` bash
$ curl -O https://datasets.clickhouse.com/ontime/partitions/ontime.tar
$ tar xvf ontime.tar -C /var/lib/clickhouse # путь к папке с данными ClickHouse
$ # убедитесь, что установлены корректные права доступа на файлы
$ sudo service clickhouse-server restart
$ clickhouse-client --query "SELECT COUNT(*) FROM datasets.ontime"
```

:::info "Info"
    Если вы собираетесь выполнять запросы, приведенные ниже, то к имени таблицы
нужно добавить имя базы, `datasets.ontime`.

## Запросы: {#zaprosy}

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

Q1. Количество полетов в день с 2000 по 2008 года

``` sql
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE Year>=2000 AND Year<=2008
GROUP BY DayOfWeek
ORDER BY c DESC;
```

Q2. Количество полетов, задержанных более чем на 10 минут, с группировкой по дням неделе, за 2000-2008 года

``` sql
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2000 AND Year<=2008
GROUP BY DayOfWeek
ORDER BY c DESC;
```

Q3. Количество задержек по аэропортам за 2000-2008

``` sql
SELECT Origin, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2000 AND Year<=2008
GROUP BY Origin
ORDER BY c DESC
LIMIT 10;
```

Q4. Количество задержек по перевозчикам за 2007 год

``` sql
SELECT IATA_CODE_Reporting_Airline AS Carrier, count(*)
FROM ontime
WHERE DepDelay>10 AND Year=2007
GROUP BY Carrier
ORDER BY count(*) DESC;
```

Q5. Процент задержек по перевозчикам за 2007 год

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

Более оптимальная версия того же запроса:

``` sql
SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay>10)*100 AS c3
FROM ontime
WHERE Year=2007
GROUP BY Carrier
ORDER BY c3 DESC
```

Q6. Предыдущий запрос за более широкий диапазон лет, 2000-2008

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

Более оптимальная версия того же запроса:

``` sql
SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay>10)*100 AS c3
FROM ontime
WHERE Year>=2000 AND Year<=2008
GROUP BY Carrier
ORDER BY c3 DESC;
```

Q7. Процент полетов, задержанных на более 10 минут, в разбивке по годам

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

Более оптимальная версия того же запроса:

``` sql
SELECT Year, avg(DepDelay>10)*100
FROM ontime
GROUP BY Year
ORDER BY Year;
```

Q8. Самые популярные направления по количеству напрямую соединенных городов для различных диапазонов лет

``` sql
SELECT DestCityName, uniqExact(OriginCityName) AS u F
ROM ontime
WHERE Year>=2000 and Year<=2010
GROUP BY DestCityName
ORDER BY u DESC
LIMIT 10;
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

Бонус:

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

Данный тест производительности был создан Вадимом Ткаченко, статьи по теме:

-   https://www.percona.com/blog/2009/10/02/analyzing-air-traffic-performance-with-infobright-and-monetdb/
-   https://www.percona.com/blog/2009/10/26/air-traffic-queries-in-luciddb/
-   https://www.percona.com/blog/2009/11/02/air-traffic-queries-in-infinidb-early-alpha/
-   https://www.percona.com/blog/2014/04/21/using-apache-hadoop-and-impala-together-with-mysql-for-data-analysis/
-   https://www.percona.com/blog/2016/01/07/apache-spark-with-air-ontime-performance-data/
-   http://nickmakos.blogspot.ru/2012/08/analyzing-air-traffic-performance-with.html

