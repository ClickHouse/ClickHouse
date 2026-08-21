---
description: 'Provides a read-only table-like interface to Apache Iceberg tables in
  Amazon S3, Azure, HDFS or locally stored.'
sidebar_label: 'iceberg'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'reference'
---

# iceberg Table Function {#iceberg-table-function}

Provides a read-only table-like interface to Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3, Azure, HDFS or locally stored.

## Syntax {#syntax}

```sql
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

### Returned value {#returned-value}

A table with the specified structure for reading data in the specified Iceberg table.

### Example {#example}

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

## Using a data catalog {#iceberg-writes-catalogs}

Iceberg tables can also be used with various data catalogs, such as the [REST Catalog](https://iceberg.apache.org/rest-catalog-spec/), [AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html) and [Unity Catalog](https://www.unitycatalog.io/).

:::important
When using a catalog, most users will want to use the `DataLakeCatalog` database engine, which connects ClickHouse to your catalog to discover your tables. You can use this database engine instead of manually creating individual tables with `IcebergS3` table engine.
:::

To use them, create a table with the `IcebergS3` engine and provide the necessary settings.

For example, using REST Catalog with MinIO storage:
```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
SETTINGS 
  storage_catalog_type="rest",
  storage_warehouse="demo",
  object_storage_endpoint="http://minio:9000/warehouse-rest",
  storage_region="us-east-1",
  storage_catalog_url="http://rest:8181/v1"
```

Or, using AWS Glue Data Catalog with S3:
```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
SETTINGS 
  storage_catalog_type = 'glue',
  storage_warehouse = 'my_database',
  object_storage_endpoint = 's3://my-data-bucket/',
  storage_region = 'us-east-1',
  storage_catalog_url = 'https://glue.us-east-1.amazonaws.com/iceberg/v1'
```

## Schema Evolution {#schema-evolution}

At the moment, with the help of CH, you can read iceberg tables, the schema of which has changed over time. We currently support reading tables where columns have been added and removed, and their order has changed. You can also change a column where a value is required to one where NULL is allowed. Additionally, we support permitted type casting for simple types, namely:  

* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P.

Currently, it is not possible to change nested structures or the types of elements within arrays and maps.

## Partition Pruning {#partition-pruning}

ClickHouse supports partition pruning during SELECT queries for Iceberg tables, which helps optimize query performance by skipping irrelevant data files. To enable partition pruning, set `use_iceberg_partition_pruning = 1`. For more information about iceberg partition pruning address https://iceberg.apache.org/spec/#partitioning

## Time Travel {#time-travel}

ClickHouse supports time travel for Iceberg tables, allowing you to query historical data with a specific timestamp or snapshot ID.

## Processing of tables with deleted rows {#deleted-rows}

Currently, only Iceberg tables with [position deletes](https://iceberg.apache.org/spec/#position-delete-files) are supported. 

The following deletion methods are **not supported**:
- [Equality deletes](https://iceberg.apache.org/spec/#equality-delete-files)
- [Deletion vectors](https://iceberg.apache.org/spec/#deletion-vectors) (introduced in v3)

### Basic usage {#basic-usage}

 ```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
 ```

 ```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
 ```

Note: You cannot specify both `iceberg_timestamp_ms` and `iceberg_snapshot_id` parameters in the same query.

### Important considerations {#important-considerations}

* **Snapshots** are typically created when:
* New data is written to the table
* Some kind of data compaction is performed

* **Schema changes typically don't create snapshots** - This leads to important behaviors when using time travel with tables that have undergone schema evolution.

### Example scenarios {#example-scenarios}

All scenarios are written in Spark because CH doesn't support writing to Iceberg tables yet.

#### Scenario 1: Schema Changes Without New Snapshots {#scenario-1}

Consider this sequence of operations:

 ```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
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

Query results at different timestamps:

* At ts1 & ts2: Only the original two columns appear
* At ts3: All three columns appear, with NULL for the price of the first row

#### Scenario 2:  Historical vs. Current Schema Differences {#scenario-2}

A time travel query at a current moment might show a different schema than the current table:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

This happens because `ALTER TABLE` doesn't create a new snapshot but for the current table Spark takes value of `schema_id` from the latest metadata file, not a snapshot.

#### Scenario 3:  Historical vs. Current Schema Differences {#scenario-3}

The second one is that while doing time travel you can't get state of table before any data was written to it:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

In Clickhouse the behavior is consistent with Spark. You can mentally replace Spark Select queries with Clickhouse Select queries and it will work the same way.

## Metadata File Resolution {#metadata-file-resolution}

When using the `iceberg` table function in ClickHouse, the system needs to locate the correct metadata.json file that describes the Iceberg table structure. Here's how this resolution process works:

### Candidate Search (in Priority Order) {#candidate-search}

1. **Direct Path Specification**:
*If you set `iceberg_metadata_file_path`, the system will use this exact path by combining it with the Iceberg table directory path.
* When this setting is provided, all other resolution settings are ignored.

2. **Table UUID Matching**:
*If `iceberg_metadata_table_uuid` is specified, the system will:
    *Look only at `.metadata.json` files in the `metadata` directory
    *Filter for files containing a `table-uuid` field matching your specified UUID (case-insensitive)

3. **Default Search**:
*If neither of the above settings are provided, all `.metadata.json` files in the `metadata` directory become candidates

### Selecting the Most Recent File {#most-recent-file}

After identifying candidate files using the above rules, the system determines which one is the most recent:

* If `iceberg_recent_metadata_file_by_last_updated_ms_field` is enabled:
* The file with the largest `last-updated-ms` value is selected

* Otherwise:
* The file with the highest version number is selected
* (Version appears as `V` in filenames formatted as `V.metadata.json` or `V-uuid.metadata.json`)

**Note**: All mentioned settings are table function settings (not global or query-level settings) and must be specified as shown below:

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**Note**: While Iceberg Catalogs typically handle metadata resolution, the `iceberg` table function in ClickHouse directly interprets files stored in S3 as Iceberg tables, which is why understanding these resolution rules is important.

## Metadata cache {#metadata-cache}

`Iceberg` table engine and table function support metadata cache storing the information of manifest files, manifest list and metadata json. The cache is stored in memory. This feature is controlled by setting `use_iceberg_metadata_files_cache`, which is enabled by default.

## Aliases {#aliases}

Table function `iceberg` is an alias to `icebergS3` now.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Writes into iceberg table {#writes-into-iceberg-table}

Starting from version 25.7, ClickHouse supports modifications of user’s Iceberg tables.

Currently, this is an experimental feature, so you first need to enable it:

```sql
SET allow_insert_into_iceberg = 1;
```

### Creating table {#create-iceberg-table}

To create your own empty Iceberg table, use the same commands as for reading, but specify the schema explicitly.
Writes supports all data formats from iceberg specification, such as Parquet, Avro, ORC.

### Example {#example-iceberg-writes-create}

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

Note: To create a version hint file, enable the `iceberg_use_version_hint` setting.
If you want to compress the metadata.json file, specify the codec name in the `iceberg_metadata_compression_method` setting.

### INSERT {#writes-inserts}

After creating a new table, you can insert data using the usual ClickHouse syntax.

### Example {#example-iceberg-writes-insert}

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

### DELETE {#iceberg-writes-delete}

Deleting extra rows in the merge-on-read format is also supported in ClickHouse.
This query will create a new snapshot with position delete files.

NOTE: If you want to read your tables in the future with other Iceberg engines (such as Spark), you need to disable the settings `output_format_parquet_use_custom_encoder` and `output_format_parquet_parallel_encoding`.
This is because Spark reads these files by parquet field-ids, while ClickHouse does not currently support writing field-ids when these flags are enabled.
We plan to fix this behavior in the future.

### Example {#example-iceberg-writes-delete}

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

### Schema evolution {#iceberg-writes-schema-evolution}

ClickHouse allows you to add, drop, modify, or rename columns with simple types (non-tuple, non-array, non-map).

### Example {#example-iceberg-writes-evolution}

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

### Compaction {#iceberg-writes-compaction}

ClickHouse supports compaction iceberg table. Currently, it can merge position delete files into data files while updating metadata. Previous snapshot IDs and timestamps remain unchanged, so the time-travel feature can still be used with the same values.

How to use it:

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

## See Also {#see-also}

* [Iceberg engine](/engines/table-engines/integrations/iceberg.md)
* [Iceberg cluster table function](/sql-reference/table-functions/icebergCluster.md)
