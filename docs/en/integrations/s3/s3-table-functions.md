---
sidebar_label: S3 Table Functions 
sidebar_position: 2
description: The s3 table function allows us to read and write files from and to S3 compatible storage.
---

# S3 Table Functions

The `s3` table function allows us to read and write files from and to S3 compatible storage.  Like other table functions, such as URL and Kafka, this relies on convenient syntax, which can be incorporated into existing SELECT and INSERT statements.  The outline for this syntax is:

```
s3(path, [aws_access_key_id, aws_secret_access_key,] format, structure, [compression])
```

where:

* path — Bucket URL with a path to the file. This supports following wildcards in read-only mode: *, ?, {abc,def} and {N..M} where N, M — numbers, 'abc', 'def' — strings. For more information, see [here](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3/#wildcards-in-path).
* format — The [format](https://clickhouse.com/docs/en/interfaces/formats/#formats) of the file.
* structure — Structure of the table. Format 'column1_name column1_type, column2_name column2_type, ...'.
* compression — Parameter is optional. Supported values: none, gzip/gz, brotli/br, xz/LZMA, zstd/zst. By default, it will autodetect compression by file extension.

We will exploit several features to maximize read and write performance with s3. Note how we can utilize wildcards in the path expression, thus allowing multiple files to be referenced and opening the door for parallelism.

## Preparation

To interact with our s3 based dataset, we prepare a standard merge tree table as our destination. The statement below creates this table under the default database.


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
SETTINGS index_granularity = 8192
```

Note the use of [partitioning](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key/#custom-partitioning-key) on the pickup_date field. Whilst usually a technique to assist with data management, we can later use this key to parallelize writes to s3.


Each entry in our taxi dataset contains a taxi trip. This anonymized data consists of 20m compressed in the s3 bucket [https://datasets-documentation.s3.eu-west-3.amazonaws.com/](https://datasets-documentation.s3.eu-west-3.amazonaws.com/) under the folder nyc-taxi. We offer this data in tsv format with approximately 1m rows per file.

## Reading Data from s3

We can query s3 data as a source without requiring persistence in ClickHouse.  In the following query, we sample 10 rows. Note the absence of credentials here as the bucket is publicly accessible:

```sql
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_*.gz', 'TabSeparatedWithNames') LIMIT 10;
```

Note that we are not required to list the columns since the TabSeparatedWithNames format encodes the column names in the first row. Other formats, such as plain CSV or TSV, will return auto-generated columns for this query, e.g., c1, c2, c3 etc. 

Queries additionally support the virtual columns _path and _file that provide information regards the bucket path and filename respectively e.g.

```sql
SELECT  _path, _file, trip_id FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_0.gz', 'TabSeparatedWithNames') LIMIT 5;
```

| \_path | \_file | trip\_id |
| :--- | :--- | :--- |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999902 |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999919 |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999944 |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999969 |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999990 |

Confirm the number of rows in this sample dataset. Note the use of wildcards for file expansion, so we consider all twenty files. This query will take around 10s depending on the number of cores on the ClickHouse instance:


```sql
SELECT count() as count FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_*.gz', 'TabSeparatedWithNames');
```

| count |
| :--- |
| 20000000 |


Whilst useful for sampling data and executing one-off exploratory queries, reading data directly from s3 is unlikely to perform on larger datasets. 

## Using clickHouse-local

The clickhouse-local program enables you to perform fast processing on local files without deploying and configuring the ClickHouse server. Any queries using the s3 table function can be performed with this utility. For example,

```sql
clickhouse-local --query "SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_*.gz', 'TabSeparatedWithNames') LIMIT 10"
```

## Inserting Data from s3

To exploit the full capabilities of ClickHouse, we next read and insert the data into our instance.
We combine our s3 function with a simple INSERT statement to achieve this. Note that we aren’t required to list our columns as our target table provides the required structure. This requires the columns to appear in the order specified in the table DDL statement: columns are mapped according to their position in the SELECT clause. The insertion of all 10m rows can take a few minutes depending on the ClickHouse instance. Below we insert 1m to ensure a prompt response. Adjust the LIMIT clause or column selection to import subsets as required:


```sql
INSERT INTO trips SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_*.gz', 'TabSeparatedWithNames') LIMIT 1000000;
```

### Remote Insert using ClickHouse Local

If network security policies prevent your ClickHouse cluster from making outbound connections, you can potentially insert s3 data using the ClickHouse local. In the example below, we read from an s3 bucket and insert to ClickHouse using the remote function.

```sql
clickhouse-local --query "INSERT INTO TABLE FUNCTION remote('localhost:9000', 'default.trips', 'username', 'password') (*) SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_*.gz', 'TabSeparatedWithNames') LIMIT 10"
```

To execute this over a secure SSL connection, utilize the remoteSecure function. This approach offers inferior performance to direct inserts on the cluster and is for use in restricted scenarios only. 

## Exporting Data

We assume you have a bucket to write data in the following examples. This will require appropriate permissions. We pass the credentials needed in the request. For further options, see [Managing Credentials](./s3-table-engine#managing-credentials).

In the simple example below, we use the table function as a destination instead of a source. Here we stream 10k rows from the trips table to a bucket, specifying lz4 compression and output type of CSV.

```sql
INSERT INTO FUNCTION s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/csv/trips.csv.lz4', 's3_key', 's3_secret', 'CSV') SELECT * FROM trips LIMIT 10000;
```

:::note
This query requires write access to the bucket.
:::

Note here how the format of the file is inferred from the extension. We also don’t need to specify the columns in the s3 function - this can be inferred from the SELECT.

### Splitting Large Files

It is unlikely you will want to export your data as a single file. Most tools, including ClickHouse, will achieve higher throughput performance when reading and writing to multiple files due to the possibility of parallelism. We could execute our INSERT command multiple times, targeting a subset of the data. ClickHouse offers a means of automatic splitting files using a PARTITION key.

In the example below, we create ten files using a modulus of the rand() function. Notice how the resulting partition id is referenced in the filename. This results in ten files with a numerical suffix, e.g. `trips_0.csv.lz4`, `trips_1.csv.lz4` etc...:


```sql
INSERT INTO FUNCTION s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/csv/trips_{_partition_id}.csv.lz4', 's3_key', 's3_secret', 'CSV') PARTITION BY rand() % 10  SELECT * FROM trips LIMIT 100000;
```

:::note
This query requires write access to the bucket.
:::

Alternatively, we can reference a field in the data. For this dataset, the payment_type provides a natural partitioning key with a cardinality of 5.

```sql
INSERT INTO FUNCTION s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/csv/trips_{_partition_id}.csv.lz4', 's3_key', 's3_secret', 'CSV') PARTITION BY payment_type SELECT * FROM trips LIMIT 100000;
```

:::note
This query requires write access to the bucket.
:::

## Utilizing Clusters

The above functions are all limited to execution on a single node. Read speeds will scale linearly with CPU cores until other resources (typically network) are saturated, allowing users to vertically scale. However, this approach has its limitations. While users can alleviate some resource pressure by inserting into a distributed table when performing an INSERT INTO SELECT query, this still leaves a single node reading, parsing, and processing the data. To address this challenge and allow us to scale reads horizontally, we have the [s3Cluster](https://clickhouse.com/docs/en/sql-reference/table-functions/s3Cluster/) function.

The node which receives the query, known as the initiator, creates a connection to every node in the cluster. The glob pattern determining which files need to be read is resolved to a set of files. The initiator distributes files to the nodes in the cluster, which act as workers. These workers, in turn, request files to process as they complete reads. This process ensures that we can scale reads horizontally.

The s3Cluster function takes the same format as the single node variants, except that a target [cluster](https://clickhouse.com/docs/en/getting-started/tutorial/#cluster-deployment) is required to denote the worker nodes.

```
s3Cluster(cluster_name, source, [access_key_id, secret_access_key,] format, structure)
```

where,

* cluster_name — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
* source — URL to a file or a bunch of files. Supports following wildcards in read-only mode: *, ?, {'abc','def'} and {N..M} where N, M — numbers, abc, def — strings. For more information see [Wildcards In Path](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3/#wildcards-in-path).
* access_key_id and secret_access_key — Keys that specify credentials to use with the given endpoint. Optional.
* format — The [format](https://clickhouse.com/docs/en/interfaces/formats/#formats) of the file.
* structure — Structure of the table. Format 'column1_name column1_type, column2_name column2_type, ...'.


Like any s3 functions, the credentials are optional if the bucket is insecure or you define security through the [environment](#managing-clusters), e.g., IAM roles. Unlike the s3 function, however, the structure must be specified in the request as of 22.3.1, i.e., the schema is not inferred.

This function will be used as part of an INSERT INTO SELECT in most cases. In this case, you will often be inserting a distributed table. We illustrate a simple example below where trips_all is a distributed table. Whilst this table uses the events cluster, the consistency of the nodes used for reads and writes is not a requirement:

```sql
INSERT INTO default.trips_all SELECT * FROM s3Cluster('events', 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_*.gz', 'TabSeparatedWithNames')
```

:::note
This query requires fixes to support schema inference present in 21.3.1 and later.
:::

Note that as of 22.3.1, inserts will occur against the initiator node. This means that whilst reads will occur on each node, the resulting rows will be routed to the initiator for distribution. In high throughput scenarios, this may prove a bottleneck. To address this, the s3Cluster function will work with the parameter **_[parallel_distributed_insert_select](https://clickhouse.com/docs/en/operations/settings/settings/#parallel_distributed_insert_select)_** in future versions.

See [Optimizing for Performance](./s3-optimizing-performance) for further details on ensuring the s3cluster function achieves optimal performance.

## Other Formats & Increasing Throughput

See [Optimizing for Performance](./s3-optimizing-performance).




