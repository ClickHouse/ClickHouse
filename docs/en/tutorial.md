---
sidebar_label: Tutorial
sidebar_position: 3
keywords: [clickhouse, install, tutorial]
---

# ClickHouse Tutorial 

## What to Expect from This Tutorial? 

In this tutorial, you will create a table and insert a large dataset (two million rows of the [New York taxi data](./en/example-datasets/nyc-taxi.md)). Then you will execute queries on the dataset, including an example of how to create a dictionary from an external data source and use it to perform a JOIN.

:::note
This tutorial assumes you have already the ClickHouse server up and running [as described in the Quick Start](./quick-start.mdx).
:::

## 1. Create a New Table

The New York City taxi data contains the details of millions of taxi rides, with columns like pickup and dropoff times and locations, cost, tip amount, tolls, payment type and so on. Let's create a table to store this data...

1. Either open your Play UI at [http://localhost:8123/play](http://localhost:8123/play) or startup the `clickhouse client` by running the following command from the folder where your `clickhouse` binary is stored:
    ```bash
    ./clickhouse client
    ```

2. Create the following `trips` table in the `default` database:
    ```sql
    CREATE TABLE trips
    (
        `trip_id` UInt32,
        `vendor_id` Enum8('1' = 1, '2' = 2, '3' = 3, '4' = 4, 'CMT' = 5, 'VTS' = 6, 'DDS' = 7, 'B02512' = 10, 'B02598' = 11, 'B02617' = 12, 'B02682' = 13, 'B02764' = 14, '' = 15),
        `pickup_date` Date,
        `pickup_datetime` DateTime,
        `dropoff_date` Date,
        `dropoff_datetime` DateTime,
        `store_and_fwd_flag` UInt8,
        `rate_code_id` UInt8,
        `pickup_longitude` Float64,
        `pickup_latitude` Float64,
        `dropoff_longitude` Float64,
        `dropoff_latitude` Float64,
        `passenger_count` UInt8,
        `trip_distance` Float64,
        `fare_amount` Float32,
        `extra` Float32,
        `mta_tax` Float32,
        `tip_amount` Float32,
        `tolls_amount` Float32,
        `ehail_fee` Float32,
        `improvement_surcharge` Float32,
        `total_amount` Float32,
        `payment_type` Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4),
        `trip_type` UInt8,
        `pickup` FixedString(25),
        `dropoff` FixedString(25),
        `cab_type` Enum8('yellow' = 1, 'green' = 2, 'uber' = 3),
        `pickup_nyct2010_gid` Int8,
        `pickup_ctlabel` Float32,
        `pickup_borocode` Int8,
        `pickup_ct2010` String,
        `pickup_boroct2010` FixedString(7),
        `pickup_cdeligibil` String,
        `pickup_ntacode` FixedString(4),
        `pickup_ntaname` String,
        `pickup_puma` UInt16,
        `dropoff_nyct2010_gid` UInt8,
        `dropoff_ctlabel` Float32,
        `dropoff_borocode` UInt8,
        `dropoff_ct2010` String,
        `dropoff_boroct2010` FixedString(7),
        `dropoff_cdeligibil` String,
        `dropoff_ntacode` FixedString(4),
        `dropoff_ntaname` String,
        `dropoff_puma` UInt16
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(pickup_date)
    ORDER BY pickup_datetime
    ```


## 2. Insert the Dataset

Now that you have a table created, let's add the NYC taxi data. It is in CSV files in S3, and you can simply load the data from there. 

1. The following command inserts ~2,000,000 rows into your `trips` table from two different files in S3: `trips_1.tsv.gz` and `trips_2.tsv.gz`:
    ```sql
    INSERT INTO trips 
        SELECT * FROM s3(
            'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{1..2}.gz', 
            'TabSeparatedWithNames'
        ) 
    ```

2. Wait for the `INSERT` to execute - it might take a minute or two for the 150MB of data to be downloaded.

    :::note
    The `s3` function cleverly knows how to decompress the data, and the `TabSeparatedWithNames` format tells ClickHouse that the data is tab-separated and also to skip the header row of each file.
    :::

3. When the data is finished being inserted, verify it worked:
    ```sql
    SELECT count() FROM trips
    ```

    You should see about 2M rows (1,999,657 rows, to be precise).

    :::note
    Notice how quickly and how few rows ClickHouse had to process to determine the count. You can get back the count in 0.001 seconds with only 6 rows processed. (6 just happens to be the number of **parts** that the `trips` table currently has, and parts know how many rows they have.)
    :::

4. If you run a query that needs to hit every row, you will notice considerably more rows need to be processed, but the execution time is still blazing fast:
    ```sql
    SELECT DISTINCT(pickup_ntaname) FROM trips
    ```

    This query has to process 2M rows and return 190 values, but notice it does this in about 0.05 seconds. The `pickup_ntaname` column represents the name of the neighborhood in New York City where the taxi ride originated.

## 3. Analyze the Data

Let's see how quickly ClickHouse can process 2M rows of data...

1. We will start with some simple and fast calculations, like computing the average tip amount (which is right on $1)
    ```sql
    SELECT avg(tip_amount) FROM trips
    ```

    The response is almost immediate:
    ```response
    ┌────avg(tip_amount)─┐
    │ 1.6847585806972212 │
    └────────────────────┘

    1 rows in set. Elapsed: 0.113 sec. Processed 2.00 million rows, 8.00 MB (17.67 million rows/s., 70.69 MB/s.)
    ```

2. This query computes the average cost based on the number of passengers:
    ```sql
    SELECT 
        passenger_count, 
        ceil(avg(total_amount),2) AS average_total_amount
    FROM trips 
    GROUP BY passenger_count
    ```

    The `passenger_count` ranges from 0 to 9:
    ```response
    ┌─passenger_count─┬─average_total_amount─┐
    │               0 │                22.69 │
    │               1 │                15.97 │
    │               2 │                17.15 │
    │               3 │                16.76 │
    │               4 │                17.33 │
    │               5 │                16.35 │
    │               6 │                16.04 │
    │               7 │                 59.8 │
    │               8 │                36.41 │
    │               9 │                 9.81 │
    └─────────────────┴──────────────────────┘

    10 rows in set. Elapsed: 0.015 sec. Processed 2.00 million rows, 10.00 MB (129.00 million rows/s., 645.01 MB/s.)
    ```

3. Here is a query that calculates the daily number of pickups per neighborhood:
    ```sql
    SELECT
        pickup_date,
        pickup_ntaname,
        SUM(1) AS number_of_trips
    FROM trips
    GROUP BY pickup_date, pickup_ntaname
    ORDER BY pickup_date ASC
    ```

    The result looks like:
    ```response
    ┌─pickup_date─┬─pickup_ntaname───────────────────────────────────────────┬─number_of_trips─┐
    │  2015-07-01 │ Brooklyn Heights-Cobble Hill                             │              13 │
    │  2015-07-01 │ Old Astoria                                              │               5 │
    │  2015-07-01 │ Flushing                                                 │               1 │
    │  2015-07-01 │ Yorkville                                                │             378 │
    │  2015-07-01 │ Gramercy                                                 │             344 │
    │  2015-07-01 │ Fordham South                                            │               2 │
    │  2015-07-01 │ SoHo-TriBeCa-Civic Center-Little Italy                   │             621 │
    │  2015-07-01 │ Park Slope-Gowanus                                       │              29 │
    │  2015-07-01 │ Bushwick South                                           │               5 │
    ```


4. This query computes the length of the trip and groups the results by that value:
    ```sql
    SELECT 
        avg(tip_amount) AS avg_tip, 
        avg(fare_amount) AS avg_fare, 
        avg(passenger_count) AS avg_passenger,
        count() AS count,
        truncate(date_diff('second', pickup_datetime, dropoff_datetime)/3600) as trip_minutes
    FROM trips
    WHERE trip_minutes > 0
    GROUP BY trip_minutes
    ORDER BY trip_minutes DESC
    ```

    The result looks like:
    ```response
    ┌────────────avg_tip─┬───────────avg_fare─┬──────avg_passenger─┬─count─┬─trip_minutes─┐
    │ 0.9800000190734863 │                 10 │                1.5 │     2 │          458 │
    │   1.18236789075801 │ 14.493377928590297 │  2.060200668896321 │  1495 │           23 │
    │ 2.1159574744549206 │  23.22872340425532 │ 2.4680851063829787 │    47 │           22 │
    │ 1.1218181631781838 │ 13.681818181818182 │ 1.9090909090909092 │    11 │           21 │
    │ 0.3218181837688793 │ 18.045454545454547 │ 2.3636363636363638 │    11 │           20 │
    │ 2.1490000009536745 │              17.55 │                1.5 │    10 │           19 │
    │  4.537058907396653 │                 37 │ 1.7647058823529411 │    17 │           18 │
    ```


5. This query shows the number of pickups in each neighborhood, broken down by hour of the day:
    ```sql
    SELECT
        pickup_ntaname,
        toHour(pickup_datetime) as pickup_hour,
        SUM(1) AS pickups
    FROM trips
    WHERE pickup_ntaname != ''
    GROUP BY pickup_ntaname, pickup_hour
    ORDER BY pickup_ntaname, pickup_hour
    ```

    The result looks like:
    ```response
    ┌─pickup_ntaname───────────────────────────────────────────┬─pickup_hour─┬─pickups─┐
    │ Airport                                                  │           0 │    3509 │
    │ Airport                                                  │           1 │    1184 │
    │ Airport                                                  │           2 │     401 │
    │ Airport                                                  │           3 │     152 │
    │ Airport                                                  │           4 │     213 │
    │ Airport                                                  │           5 │     955 │
    │ Airport                                                  │           6 │    2161 │
    │ Airport                                                  │           7 │    3013 │
    │ Airport                                                  │           8 │    3601 │
    │ Airport                                                  │           9 │    3792 │
    │ Airport                                                  │          10 │    4546 │
    │ Airport                                                  │          11 │    4659 │
    │ Airport                                                  │          12 │    4621 │
    │ Airport                                                  │          13 │    5348 │
    │ Airport                                                  │          14 │    5889 │
    │ Airport                                                  │          15 │    6505 │
    │ Airport                                                  │          16 │    6119 │
    │ Airport                                                  │          17 │    6341 │
    │ Airport                                                  │          18 │    6173 │
    │ Airport                                                  │          19 │    6329 │
    │ Airport                                                  │          20 │    6271 │
    │ Airport                                                  │          21 │    6649 │
    │ Airport                                                  │          22 │    6356 │
    │ Airport                                                  │          23 │    6016 │
    │ Allerton-Pelham Gardens                                  │           4 │       1 │
    │ Allerton-Pelham Gardens                                  │           6 │       1 │
    │ Allerton-Pelham Gardens                                  │           7 │       1 │
    │ Allerton-Pelham Gardens                                  │           9 │       5 │
    │ Allerton-Pelham Gardens                                  │          10 │       3 │
    │ Allerton-Pelham Gardens                                  │          15 │       1 │
    │ Allerton-Pelham Gardens                                  │          20 │       2 │
    │ Allerton-Pelham Gardens                                  │          23 │       1 │
    │ Annadale-Huguenot-Prince's Bay-Eltingville               │          23 │       1 │
    │ Arden Heights                                            │          11 │       1 │
    ```

7. Let's look at rides to LaGuardia or JFK airports, which requires all 2M rows to be processed and returns in less than 0.04 seconds:
    ```sql
    SELECT
        pickup_datetime,
        dropoff_datetime,
        total_amount,
        pickup_nyct2010_gid,
        dropoff_nyct2010_gid,
        CASE
            WHEN dropoff_nyct2010_gid = 138 THEN 'LGA'
            WHEN dropoff_nyct2010_gid = 132 THEN 'JFK'
        END AS airport_code,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(DAY FROM pickup_datetime) AS day,
        EXTRACT(HOUR FROM pickup_datetime) AS hour
    FROM trips
    WHERE dropoff_nyct2010_gid IN (132, 138)
    ORDER BY pickup_datetime  
    ```  

    The response is:
    ```response
    ┌─────pickup_datetime─┬────dropoff_datetime─┬─total_amount─┬─pickup_nyct2010_gid─┬─dropoff_nyct2010_gid─┬─airport_code─┬─year─┬─day─┬─hour─┐
    │ 2015-07-01 00:04:14 │ 2015-07-01 00:15:29 │         13.3 │                 -34 │                  132 │ JFK          │ 2015 │   1 │    0 │
    │ 2015-07-01 00:09:42 │ 2015-07-01 00:12:55 │          6.8 │                  50 │                  138 │ LGA          │ 2015 │   1 │    0 │
    │ 2015-07-01 00:23:04 │ 2015-07-01 00:24:39 │          4.8 │                -125 │                  132 │ JFK          │ 2015 │   1 │    0 │
    │ 2015-07-01 00:27:51 │ 2015-07-01 00:39:02 │        14.72 │                -101 │                  138 │ LGA          │ 2015 │   1 │    0 │
    │ 2015-07-01 00:32:03 │ 2015-07-01 00:55:39 │        39.34 │                  48 │                  138 │ LGA          │ 2015 │   1 │    0 │
    │ 2015-07-01 00:34:12 │ 2015-07-01 00:40:48 │         9.95 │                 -93 │                  132 │ JFK          │ 2015 │   1 │    0 │
    │ 2015-07-01 00:38:26 │ 2015-07-01 00:49:00 │         13.3 │                 -11 │                  138 │ LGA          │ 2015 │   1 │    0 │
    │ 2015-07-01 00:41:48 │ 2015-07-01 00:44:45 │          6.3 │                 -94 │                  132 │ JFK          │ 2015 │   1 │    0 │
    │ 2015-07-01 01:06:18 │ 2015-07-01 01:14:43 │        11.76 │                  37 │                  132 │ JFK          │ 2015 │   1 │    1 │
    ```

    :::note
    As you can see, it doesn't seem to matter what type of grouping or calculation that is being performed, ClickHouse retrieves the results almost immediately!
    :::

## 4. Create a Dictionary

If you are new to ClickHouse, it is important to understand how ***dictionaries*** work. A dictionary is a mapping of key->value pairs that is stored in memory. They often are associated with data in a file or external database (and they can periodically update with their external data source). 

1. Let's see how to create a dictionary associated with a file in S3. The file contains 265 rows, one row for each neighborhood in NYC. The neighborhoods are mapped to the names of the NYC boroughs (NYC has 5 boroughs: the Bronx, Booklyn, Manhattan, Queens and Staten Island), and this file counts Newark Airport (EWR) as a borough as well.

    The `LocationID` column in the our file maps to the `pickup_nyct2010_gid` and `dropoff_nyct2010_gid` columns in your `trips` table. Here are a few rows from the CSV file:

    | LocationID      | Borough |  Zone      | service_zone |
    | ----------- | ----------- |   ----------- | ----------- |
    | 1      | EWR       |  Newark Airport   | EWR        |
    | 2    |   Queens     |   Jamaica Bay   |      Boro Zone   |
    | 3   |   Bronx     |  Allerton/Pelham Gardens    |    Boro Zone     |
    | 4     |    Manhattan    |    Alphabet City  |     Yellow Zone    |
    | 5     |  Staten Island      |   Arden Heights   |    Boro Zone     |


2. The URL for the file is `https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv`. Run the following SQL, which creates a new dictionary named `taxi_zone_dictionary` that is based on this file in S3:
    ```sql
    CREATE DICTIONARY taxi_zone_dictionary (
        LocationID UInt16 DEFAULT 0,
        Borough String,
        Zone String,
        service_zone String
    )
    PRIMARY KEY LocationID
    SOURCE(HTTP(
        url 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv'
        format 'CSVWithNames'
    ))
    LIFETIME(0)
    LAYOUT(HASHED())
    ```

    :::note
    Setting `LIFETIME` to 0 means this dictionary will never update with its source. It is used here to not send unnecessary traffic to our S3 bucket, but in general you could specify any lifetime values you prefer. 
    
    For example:

    ```sql
    LIFETIME(MIN 1 MAX 10)
    ```
    specifies the dictionary to update after some random time between 1 and 10 seconds. (The random time is necessary in order to distribute the load on the dictionary source when updating on a large number of servers.)
    :::
    
3. Verify it worked - you should get 265 rows (one row for each neighborhood):
    ```sql
    SELECT * FROM taxi_zone_dictionary 
    ```

4. Use the `dictGet` function ([or its variations](./en/sql-reference/functions/ext-dict-functions.md)) to retrieve a value from a dictionary. You pass in the name of the dictionary, the value you want, and the key (which in our example is the `LocationID` column of `taxi_zone_dictionary`). 

    For example, the following query returns the `Borough` whose `LocationID` is 132 (which as we saw above is JFK airport):
    ```sql
    SELECT dictGet('taxi_zone_dictionary', 'Borough', 132) 
    ```

    JFK is in Queens, and notice the time to retrieve the value is essentially 0:
    ```response
    ┌─dictGet('taxi_zone_dictionary', 'Borough', 132)─┐
    │ Queens                                          │
    └─────────────────────────────────────────────────┘

    1 rows in set. Elapsed: 0.004 sec.
    ```

5. Use the `dictHas` function to see if a key is present in the dictionary. For example, the following query returns 1 (which is "true" in ClickHouse):
    ```sql
    SELECT dictHas('taxi_zone_dictionary', 132) 
    ```

6. The following query returns 0 because 4567 is not a value of `LocationID` in the dictionary:
    ```sql
    SELECT dictHas('taxi_zone_dictionary', 4567) 
    ```

7. Use the `dictGet` function to retrieve a borough's name in a query. For example:
    ```sql
    SELECT
        count(1) AS total,
        dictGetOrDefault('taxi_zone_dictionary','Borough', toUInt64(pickup_nyct2010_gid), 'Unknown') AS borough_name
    FROM trips 
    WHERE dropoff_nyct2010_gid = 132 OR dropoff_nyct2010_gid = 138
    GROUP BY borough_name
    ORDER BY total DESC
    ```

    This query sums up the number of taxi rides per borough that end at either the LaGuardia or JFK airport. The result looks like the following, and notice there are quite a few trips where the dropoff neighborhood is unknown:
    ```response
    ┌─total─┬─borough_name──┐
    │ 23683 │ Unknown       │
    │  7053 │ Manhattan     │
    │  6828 │ Brooklyn      │
    │  4458 │ Queens        │
    │  2670 │ Bronx         │
    │   554 │ Staten Island │
    │    53 │ EWR           │
    └───────┴───────────────┘

    7 rows in set. Elapsed: 0.019 sec. Processed 2.00 million rows, 4.00 MB (105.70 million rows/s., 211.40 MB/s.)
    ```


## 5. Perform a Join

Let's write some queries that join the `taxi_zone_dictionary` with your `trips` table.

1. We can start with a simple JOIN that acts similarly to the previous airport query above:
    ```sql
    SELECT
        count(1) AS total,
        Borough
    FROM trips 
    JOIN taxi_zone_dictionary ON toUInt64(trips.pickup_nyct2010_gid) = taxi_zone_dictionary.LocationID
    WHERE dropoff_nyct2010_gid = 132 OR dropoff_nyct2010_gid = 138
    GROUP BY Borough
    ORDER BY total DESC
    ```

    The response looks familiar:
    ```response
    ┌─total─┬─Borough───────┐
    │  7053 │ Manhattan     │
    │  6828 │ Brooklyn      │
    │  4458 │ Queens        │
    │  2670 │ Bronx         │
    │   554 │ Staten Island │
    │    53 │ EWR           │
    └───────┴───────────────┘

    6 rows in set. Elapsed: 0.034 sec. Processed 2.00 million rows, 4.00 MB (59.14 million rows/s., 118.29 MB/s.)
    ```

    :::note
    Notice the output of the above `JOIN` query is the same as the query before it that used `dictGetOrDefault` (except that the `Unknown` values are not included). Behind the scenes, ClickHouse is actually calling the `dictGet` function for the `taxi_zone_dictionary` dictionary, but the `JOIN` syntax is more familiar for SQL developers.
    :::

2. We do not use `SELECT *` often in ClickHouse - you should only retrieve the columns you actually need! But it is difficult to find a query that takes a long time, so this query purposely selects every column and returns every row (except there is a built-in 10,000 row maximum in the response by default), and also does a right join of every row with the dictionary:
    ```sql
    SELECT * 
    FROM trips 
    JOIN taxi_zone_dictionary 
        ON trips.dropoff_nyct2010_gid = taxi_zone_dictionary.LocationID
    WHERE tip_amount > 0
    ORDER BY tip_amount DESC
    ```

    It is the slowest query in this tutorial, yet it only takes about 0.8 seconds to process all 2M rows. Nice!

#### Congrats!

Well done, you made it through the tutorial, and hopefully you have a better understanding of how to use ClickHouse. Here are some options for what to do next:

- View the [Getting Started with ClickHouse](https://clickhouse.com/company/events/getting-started-with-clickhouse/) video, an excellent introduction to ClickHouse
- Read [how primary keys work in ClickHouse](./guides/improving-query-performance/sparse-primary-indexes.md) - this knowledge will move you a long ways forward along your journey to becoming a ClickHouse expert
- [Integrate an external data source](./integrations/) like files, Kafka, PostgreSQL, data pipelines, or lots of other data sources
- [Connect your favorite UI/BI tool](./connect-a-ui/) to ClickHouse
- Check out the [SQL Reference](./en/sql-reference/) and browse through the various functions. ClickHouse has an amazing collection of functions for transforming, processing and analyzing data



[Original article](https://clickhouse.com/docs/tutorial/) <!--hide-->
