---
description: 'This engine provides a read-only integration with existing Apache Iceberg
  tables in Amazon S3, Azure, HDFS and locally stored tables.'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'Iceberg table engine'
doc_type: 'reference'
---

# Iceberg table engine {#iceberg-table-engine}

:::warning 
We recommend using the [Iceberg Table Function](/sql-reference/table-functions/iceberg.md) for working with Iceberg data in ClickHouse. The Iceberg Table Function currently provides sufficient functionality, offering a partial read-only interface for Iceberg tables.

The Iceberg Table Engine is available but may have limitations. ClickHouse wasn't originally designed to support tables with externally changing schemas, which can affect the functionality of the Iceberg Table Engine. As a result, some features that work with regular tables may be unavailable or may not function correctly, especially when using the old analyzer.

For optimal compatibility, we suggest using the Iceberg Table Function while we continue to improve support for the Iceberg Table Engine.
:::

This engine provides a read-only integration with existing Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3, Azure, HDFS and locally stored tables.

## Create table {#create-table}

Note that the Iceberg table must already exist in the storage, this command does not take DDL parameters to create a new table.

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

## Engine arguments {#engine-arguments}

Description of the arguments coincides with description of arguments in engines `S3`, `AzureBlobStorage`, `HDFS` and `File` correspondingly.
`format` stands for the format of data files in the Iceberg table.

Engine parameters can be specified using [Named Collections](../../../operations/named-collections.md)

### Example {#example}

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Using named collections:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

## Aliases {#aliases}

Table engine `Iceberg` is an alias to `IcebergS3` now.

## Schema evolution {#schema-evolution}
ClickHouse supports reading Iceberg tables whose schema has evolved over time. This includes tables where columns have been added, removed, or reordered, as well as columns changed from required to nullable. Additionally, the following type casts are supported:

* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P. 

Currently, it is not possible to change nested structures or the types of elements within arrays and maps.

To read a table where the schema has changed after its creation with dynamic schema inference, set allow_dynamic_metadata_for_data_lakes = true when creating the table.

## Partition pruning {#partition-pruning}

ClickHouse supports partition pruning during SELECT queries for Iceberg tables, which helps optimize query performance by skipping irrelevant data files. To enable partition pruning, set `use_iceberg_partition_pruning = 1`. For more information about iceberg partition pruning address https://iceberg.apache.org/spec/#partitioning

## Time travel {#time-travel}

ClickHouse supports time travel for Iceberg tables, allowing you to query historical data with a specific timestamp or snapshot ID.

## Processing of tables with deleted rows {#deleted-rows}

ClickHouse supports reading Iceberg tables that use the following deletion methods:

- [Position deletes](https://iceberg.apache.org/spec/#position-delete-files)
- [Equality deletes](https://iceberg.apache.org/spec/#equality-delete-files) (supported from version 25.8+)

The following deletion method is **not supported**:
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

- **Snapshots** are typically created when:
  - New data is written to the table
  - Some kind of data compaction is performed

- **Schema changes typically don't create snapshots** - This leads to important behaviors when using time travel with tables that have undergone schema evolution.

### Example scenarios {#example-scenarios}

All scenarios are written in Spark because CH doesn't support writing to Iceberg tables yet.

#### Scenario 1: Schema changes without new snapshots {#scenario-1}

Consider this sequence of operations:

 ```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

-- Query the table at each timestamp
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

- At ts1 & ts2: Only the original two columns appear
- At ts3: All three columns appear, with NULL for the price of the first row

#### Scenario 2: Historical vs. current schema differences {#scenario-2}

A time travel query at a current moment might show a different schema than the current table:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number int, 
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

#### Scenario 3: Historical vs. current schema differences {#scenario-3}

The second one is that while doing time travel you can't get state of table before any data was written to it:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

In Clickhouse the behavior is consistent with Spark. You can mentally replace Spark Select queries with Clickhouse Select queries and it will work the same way.

## Metadata file resolution {#metadata-file-resolution}
When using the `Iceberg` table engine in ClickHouse, the system needs to locate the correct metadata.json file that describes the Iceberg table structure. Here's how this resolution process works:

### Candidates search {#candidate-search}

1. **Direct Path Specification**:
* If you set `iceberg_metadata_file_path`, the system will use this exact path by combining it with the Iceberg table directory path.
* When this setting is provided, all other resolution settings are ignored.
2. **Table UUID Matching**:
* If `iceberg_metadata_table_uuid` is specified, the system will:
  * Look only at `.metadata.json` files in the `metadata` directory
  * Filter for files containing a `table-uuid` field matching your specified UUID (case-insensitive)

3. **Default Search**:
* If neither of the above settings are provided, all `.metadata.json` files in the `metadata` directory become candidates

### Selecting the most recent file {#most-recent-file}

After identifying candidate files using the above rules, the system determines which one is the most recent:

* If `iceberg_recent_metadata_file_by_last_updated_ms_field` is enabled:
  * The file with the largest `last-updated-ms` value is selected

* Otherwise:
  * The file with the highest version number is selected
  * (Version appears as `V` in filenames formatted as `V.metadata.json` or `V-uuid.metadata.json`)

**Note**: All mentioned settings are engine-level settings and must be specified during table creation as shown below:

```sql 
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**Note**: While Iceberg Catalogs typically handle metadata resolution, the `Iceberg` table engine in ClickHouse directly interprets files stored in S3 as Iceberg tables, which is why understanding these resolution rules is important.

## Data cache {#data-cache}

`Iceberg` table engine and table function support data caching same as `S3`, `AzureBlobStorage`, `HDFS` storages. See [here](../../../engines/table-engines/integrations/s3.md#data-cache).

## Metadata cache {#metadata-cache}

`Iceberg` table engine and table function support metadata cache storing the information of manifest files, manifest list and metadata json. The cache is stored in memory. This feature is controlled by setting `use_iceberg_metadata_files_cache`, which is enabled by default.

## See also {#see-also}

- [iceberg table function](/sql-reference/table-functions/iceberg.md)
