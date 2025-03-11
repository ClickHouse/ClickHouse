---
slug: /sql-reference/table-functions/iceberg
sidebar_position: 90
sidebar_label: iceberg
title: "iceberg"
description: "Provides a read-only table-like interface to Apache Iceberg tables in Amazon S3, Azure, HDFS or locally stored."
---

# iceberg Table Function

Provides a read-only table-like interface to Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3, Azure, HDFS or locally stored.

## Syntax {#syntax}

``` sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

## Arguments {#arguments}

Description of the arguments coincides with description of arguments in table functions `s3`, `azureBlobStorage`, `HDFS` and `file` correspondingly.
`format` stands for the format of data files in the Iceberg table.

**Returned value**
A table with the specified structure for reading data in the specified Iceberg table.

**Example**

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
ClickHouse currently supports reading v1 and v2 of the Iceberg format via the `icebergS3`, `icebergAzure`, `icebergHDFS` and `icebergLocal` table functions and `IcebergS3`, `icebergAzure`, `IcebergHDFS` and `IcebergLocal` table engines.
:::

## Defining a named collection {#defining-a-named-collection}

Here is an example of configuring a named collection for storing the URL and credentials:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test<access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

**Schema Evolution**
At the moment, with the help of CH, you can read iceberg tables, the schema of which has changed over time. We currently support reading tables where columns have been added and removed, and their order has changed. You can also change a column where a value is required to one where NULL is allowed. Additionally, we support permitted type casting for simple types, namely:  
* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P. 

Currently, it is not possible to change nested structures or the types of elements within arrays and maps.

**Partition Pruning**

ClickHouse supports partition pruning during SELECT queries for Iceberg tables, which helps optimize query performance by skipping irrelevant data files. Now it works with only identity transforms and time-based transforms (hour, day, month, year). To enable partition pruning, set `use_iceberg_partition_pruning = 1`.


**Time Travel**

 ClickHouse supports time travel for Iceberg tables, which allows you to query historical data by specifying of a timestamp. To enable time travel for a select query, set `iceberg_timestamp_ms` parameter to necessary UTC timestamp in milliseconds.

 ```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
 ```

 You need to take into account that the timestamp is applied to the snapshot, which is usualy created when new data is written to the table or compacation happens. Writers are not obliged to create new snapshot after any kind of schema changes. That means that you can get a bit unexpected results if you use timestamp from the past and used schema evolution. For example, pay attention to this spark code:

 ```
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

    ts1 = now()

    ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)

    ts2 = now()

    INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

    ts3 = now()

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+


  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

Pay attention that as a result of the query
```
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;
```

you get the table with the following schema though alter column adding `price` was already applied:
```
+------------+------------+
|order_number|product_code|
+------------+------------+
```

That's because `ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)` doesn't create a new snapshot.

Also this fact has two funny implications:

The first one is that schema of queries with time travel applied in the current moment is not always equal to schema of table at the moment of query:

```
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')


  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');


  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example_2;


    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

The second one is that while doing time travel you can't get state of table before any data was written to it:

```
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

In Clickhouse the behavior is the same as in Spark.

**Aliases**

Table function `iceberg` is an alias to `icebergS3` now.

**See Also**

- [Iceberg engine](/engines/table-engines/integrations/iceberg.md)
- [Iceberg cluster table function](/sql-reference/table-functions/icebergCluster.md)
