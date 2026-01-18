---
description: 'Provides a table-like interface to select/insert files in Amazon S3
  and Google Cloud Storage. This table function is similar to the hdfs function, but
  provides S3-specific features.'
keywords: ['s3', 'gcs', 'bucket']
sidebar_label: 's3'
sidebar_position: 180
slug: /sql-reference/table-functions/s3
title: 's3 Table Function'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# s3 Table Function

Provides a table-like interface to select/insert files in [Amazon S3](https://aws.amazon.com/s3/) and [Google Cloud Storage](https://cloud.google.com/storage/). This table function is similar to the [hdfs function](../../sql-reference/table-functions/hdfs.md), but provides S3-specific features.

If you have multiple replicas in your cluster, you can use the [s3Cluster function](../../sql-reference/table-functions/s3Cluster.md) instead to parallelize inserts.

When using the `s3 table function` with [`INSERT INTO...SELECT`](../../sql-reference/statements/insert-into#inserting-the-results-of-select), data is read and inserted in a streaming fashion. Only a few blocks of data reside in memory while the blocks are continuously read from S3 and pushed into the destination table.

## Syntax {#syntax}

```sql
s3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,structure] [,compression_method],[,headers], [,partition_strategy], [,partition_columns_in_data_file])
s3(named_collection[, option=value [,..]])
```

:::tip GCS
The S3 Table Function integrates with Google Cloud Storage by using the GCS XML API and HMAC keys.  See the [Google interoperability docs]( https://cloud.google.com/storage/docs/interoperability) for more details about the endpoint and HMAC.

For GCS, substitute your HMAC key and HMAC secret where you see `access_key_id` and `secret_access_key`.
:::

**Parameters**

`s3` table function supports the following plain parameters:

| Parameter                               | Description                                                                                                                                                                                                                                                                                                                                                                      |
|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`                                   | Bucket url with path to file. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings. For more information see [here](../../engines/table-engines/integrations/s3.md#wildcards-in-path).                                                                                                   |
| `NOSIGN`                                | If this keyword is provided in place of credentials, all the requests will not be signed.                                                                                                                                                                                                                                                                                        |
| `access_key_id` and `secret_access_key` | Keys that specify credentials to use with given endpoint. Optional.                                                                                                                                                                                                                                                                                                              |
| `session_token`                         | Session token to use with the given keys. Optional when passing keys.                                                                                                                                                                                                                                                                                                            |
| `format`                                | The [format](/sql-reference/formats) of the file.                                                                                                                                                                                                                                                                                                                                |
| `structure`                             | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                    |
| `compression_method`                    | Parameter is optional. Supported values: `none`, `gzip` or `gz`, `brotli` or `br`, `xz` or `LZMA`, `zstd` or `zst`. By default, it will autodetect compression method by file extension.                                                                                                                                                                                         |
| `headers`                               | Parameter is optional. Allows headers to be passed in the S3 request. Pass in the format `headers(key=value)` e.g. `headers('x-amz-request-payer' = 'requester')`.                                                                                                                                                                                                               |
| `partition_strategy`                    | Parameter is optional. Supported values: `WILDCARD` or `HIVE`. `WILDCARD` requires a `{_partition_id}` in the path, which is replaced with the partition key. `HIVE` does not allow wildcards, assumes the path is the table root, and generates Hive-style partitioned directories with Snowflake IDs as filenames and the file format as the extension. Defaults to `WILDCARD` |
| `partition_columns_in_data_file`        | Parameter is optional. Only used with `HIVE` partition strategy. Tells ClickHouse whether to expect partition columns to be written in the data file. Defaults `false`.                                                                                                                                                                                                          |
| `storage_class_name`                    | Parameter is optional. Supported values: `STANDARD` or `INTELLIGENT_TIERING`. Allow to specify [AWS S3 Intelligent Tiering](https://aws.amazon.com/s3/storage-classes/intelligent-tiering/). Defaults to `STANDARD`. |

:::note GCS
The GCS url is in this format as the endpoint for the Google XML API is different than the JSON API:

```text
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
```

and not ~~https://storage.cloud.google.com~~.
:::

Arguments can also be passed using [named collections](operations/named-collections.md). In this case `url`, `access_key_id`, `secret_access_key`, `format`, `structure`, `compression_method` work in the same way, and some extra parameters are supported:

| Argument                      | Description                                                                                                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `filename`                    | appended to the url if specified.                                                                                                                                                 |
| `use_environment_credentials` | enabled by default, allows passing extra parameters using environment variables `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | disabled by default.                                                                                                                                                              |
| `expiration_window_seconds`   | default value is 120.                                                                                                                                                             |

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

## Examples {#examples}

Selecting the first 5 rows from the table from S3 file `https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv`:

```sql
SELECT *
FROM s3(
   'https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv',
   'CSVWithNames'
)
LIMIT 5;
```

```response
┌───────Date─┬────Open─┬────High─┬─────Low─┬───Close─┬───Volume─┬─OpenInt─┐
│ 1984-09-07 │ 0.42388 │ 0.42902 │ 0.41874 │ 0.42388 │ 23220030 │       0 │
│ 1984-09-10 │ 0.42388 │ 0.42516 │ 0.41366 │ 0.42134 │ 18022532 │       0 │
│ 1984-09-11 │ 0.42516 │ 0.43668 │ 0.42516 │ 0.42902 │ 42498199 │       0 │
│ 1984-09-12 │ 0.42902 │ 0.43157 │ 0.41618 │ 0.41618 │ 37125801 │       0 │
│ 1984-09-13 │ 0.43927 │ 0.44052 │ 0.43927 │ 0.43927 │ 57822062 │       0 │
└────────────┴─────────┴─────────┴─────────┴─────────┴──────────┴─────────┘
```

:::note
ClickHouse uses filename extensions to determine the format of the data. For example, we could have run the previous command without the `CSVWithNames`:

```sql
SELECT *
FROM s3(
   'https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv'
)
LIMIT 5;
```

ClickHouse also can determine the compression method of the file. For example, if the file was zipped up with a `.csv.gz` extension, ClickHouse would decompress the file automatically.
:::

:::note
Parquet files with names like `*.parquet.snappy` or `*.parquet.zstd` can confuse ClickHouse and cause `TOO_LARGE_COMPRESSED_BLOCK` or `ZSTD_DECODER_FAILED` errors.
This is because ClickHouse would attempt to read the entire file as Snappy or ZSTD-encoded data when, in fact, Parquet applies compression at the row-group and column level.

Parquet metadata already specifies the per-column compression, and so the file extension is superfluous.
You can just use `compression_method = 'none'` in such cases:

```sql
SELECT *
FROM s3(
  'https://<my-bucket>.s3.<my-region>.amazonaws.com/path/to/my-data.parquet.snappy',
  compression_format = 'none'
);
```
:::

## Usage {#usage}

Suppose that we have several files with following URIs on S3:

- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/some_prefix/some_file_1.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/some_prefix/some_file_2.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/some_prefix/some_file_3.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/some_prefix/some_file_4.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/another_prefix/some_file_1.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/another_prefix/some_file_2.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/another_prefix/some_file_3.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/another_prefix/some_file_4.csv'

Count the number of rows in files ending with numbers from 1 to 3:

```sql
SELECT count(*)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      18 │
└─────────┘
```

Count the total amount of rows in all files in these two directories:

```sql
SELECT count(*)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      24 │
└─────────┘
```

:::tip
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

Count the total amount of rows in files named `file-000.csv`, `file-001.csv`, ... , `file-999.csv`:

```sql
SELECT count(*)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─count()─┐
│      12 │
└─────────┘
```

Insert data into file `test-data.csv.gz`:

```sql
INSERT INTO FUNCTION s3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

Insert data into file `test-data.csv.gz` from existing table:

```sql
INSERT INTO FUNCTION s3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

Glob ** can be used for recursive directory traversal. Consider the below example, it will fetch all files from `my-test-bucket-768` directory recursively:

```sql
SELECT * FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

The below get data from all `test-data.csv.gz` files from any folder inside `my-test-bucket` directory recursively:

```sql
SELECT * FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

Note. It is possible to specify custom URL mappers in the server configuration file. Example:
```sql
SELECT * FROM s3('s3://clickhouse-public-datasets/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```
The URL `'s3://clickhouse-public-datasets/my-test-bucket-768/**/test-data.csv.gz'` would be replaced to `'http://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/**/test-data.csv.gz'`

Custom mapper can be added into `config.xml`:
```xml
<url_scheme_mappers>
   <s3>
      <to>https://{bucket}.s3.amazonaws.com</to>
   </s3>
   <gs>
      <to>https://{bucket}.storage.googleapis.com</to>
   </gs>
   <oss>
      <to>https://{bucket}.oss.aliyuncs.com</to>
   </oss>
</url_scheme_mappers>
```

For production use cases it is recommended to use [named collections](operations/named-collections.md). Here is the example:
```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = '***',
        secret_access_key = '***';
SELECT count(*)
FROM s3(creds, url='https://s3-object-url.csv')
```

## Partitioned Write {#partitioned-write}

### Partition Strategy {#partition-strategy}

Supported for INSERT queries only.

`WILDCARD` (default): Replaces the `{_partition_id}` wildcard in the file path with the actual partition key.

`HIVE` implements hive style partitioning for reads & writes. It generates files using the following format: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

**Example of `HIVE` partition strategy**

```sql
INSERT INTO FUNCTION s3(s3_conn, filename='t_03363_function', format=Parquet, partition_strategy='hive') PARTITION BY (year, country) SELECT 2020 as year, 'Russia' as country, 1 as id;
```

```result
SELECT _path, * FROM s3(s3_conn, filename='t_03363_function/**.parquet');

   ┌─_path──────────────────────────────────────────────────────────────────────┬─id─┬─country─┬─year─┐
1. │ test/t_03363_function/year=2020/country=Russia/7351295896279887872.parquet │  1 │ Russia  │ 2020 │
   └────────────────────────────────────────────────────────────────────────────┴────┴─────────┴──────┘
```

**Examples of `WILDCARD` partition strategy**

1. Using partition ID in a key creates separate files:

```sql
INSERT INTO TABLE FUNCTION
    s3('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```
As a result, the data is written into three files: `file_x.csv`, `file_y.csv`, and `file_z.csv`.

2. Using partition ID in a bucket name creates files in different buckets:

```sql
INSERT INTO TABLE FUNCTION
    s3('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```
As a result, the data is written into three files in different buckets: `my_bucket_1/file.csv`, `my_bucket_10/file.csv`, and `my_bucket_20/file.csv`.

## Accessing public buckets {#accessing-public-buckets}

ClickHouse tries to fetch credentials from many different types of sources.
Sometimes, it can produce problems when accessing some buckets that are public causing the client to return `403` error code.
This issue can be avoided by using `NOSIGN` keyword, forcing the client to ignore all the credentials, and not sign the requests.

```sql
SELECT *
FROM s3(
   'https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv',
   NOSIGN,
   'CSVWithNames'
)
LIMIT 5;
```

## Using S3 credentials (ClickHouse Cloud) {#using-s3-credentials-clickhouse-cloud}

For non-public buckets, users can pass an `aws_access_key_id` and `aws_secret_access_key` to the function. For example:

```sql
SELECT count() FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/mta/*.tsv', '<KEY>', '<SECRET>','TSVWithNames')
```

This is appropriate for one-off accesses or in cases where credentials can easily be rotated. However, this is not recommended as a long-term solution for repeated access or where credentials are sensitive. In this case, we recommend users rely on role-based access.

Role-based access for S3 in ClickHouse Cloud is documented [here](/cloud/data-sources/secure-s3#setup).

Once configured, a `roleARN` can be passed to the s3 function via an `extra_credentials` parameter. For example:

```sql
SELECT count() FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/mta/*.tsv','CSVWithNames',extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/ClickHouseAccessRole-001'))
```

Further examples can be found [here](/cloud/data-sources/secure-s3#access-your-s3-bucket-with-the-clickhouseaccess-role)

## Working with archives {#working-with-archives}

Suppose that we have several archive files with following URIs on S3:

- 'https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-2018-01-10.csv.zip'
- 'https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-2018-01-11.csv.zip'
- 'https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-2018-01-12.csv.zip'

Extracting data from these archives is possible using ::. Globs can be used both in the url part as well as in the part after :: (responsible for the name of a file inside the archive).

```sql
SELECT *
FROM s3(
   'https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-2018-01-1{0..2}.csv.zip :: *.csv'
);
```

:::note
ClickHouse supports three archive formats:
ZIP
TAR
7Z
While ZIP and TAR archives can be accessed from any supported storage location, 7Z archives can only be read from the local filesystem where ClickHouse is installed.
:::

## Inserting Data {#inserting-data}

Note that rows can only be inserted into new files. There are no merge cycles or file split operations. Once a file is written, subsequent inserts will fail. See more details [here](/integrations/s3#inserting-data).

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`. In case of archive, shows path in a format: `"{path_to_archive}::{path_to_file_inside_archive}"`
- `_file` — Name of the file. Type: `LowCardinality(String)`. In case of archive shows name of the file inside the archive.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`. In case of archive shows uncompressed file size of the file inside the archive.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.

## use_hive_partitioning setting {#hive-style-partitioning}

This is a hint for ClickHouse to parse hive style partitioned files upon reading time. It has no effect on writing. For symmetrical reads and writes, use the `partition_strategy` argument.

When setting `use_hive_partitioning` is set to 1, ClickHouse will detect Hive-style partitioning in the path (`/name=value/`) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path, but starting with `_`.

**Example**

```sql
SELECT * FROM s3('s3://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

## Accessing requester-pays buckets {#accessing-requester-pays-buckets}

To access a requester-pays bucket, a header `x-amz-request-payer = requester` must be passed in any requests. This is achieved by passing the parameter `headers('x-amz-request-payer' = 'requester')` to the s3 function. For example:

```sql
SELECT
    count() AS num_rows,
    uniqExact(_file) AS num_files
FROM s3('https://coiled-datasets-rp.s3.us-east-1.amazonaws.com/1trc/measurements-100*.parquet', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', headers('x-amz-request-payer' = 'requester'))

┌───num_rows─┬─num_files─┐
│ 1110000000 │       111 │
└────────────┴───────────┘

1 row in set. Elapsed: 3.089 sec. Processed 1.09 billion rows, 0.00 B (353.55 million rows/s., 0.00 B/s.)
Peak memory usage: 192.27 KiB.
```

## Storage Settings {#storage-settings}

- [s3_truncate_on_insert](operations/settings/settings.md#s3_truncate_on_insert) - allows to truncate file before insert into it. Disabled by default.
- [s3_create_new_file_on_insert](operations/settings/settings.md#s3_create_new_file_on_insert) - allows to create a new file on each insert if format has suffix. Disabled by default.
- [s3_skip_empty_files](operations/settings/settings.md#s3_skip_empty_files) - allows to skip empty files while reading. Enabled by default.

## Nested Avro Schemas {#nested-avro-schemas}

When reading Avro files that contain **nested records** which diverge across files (for example, some files have an extra field inside a nested object), ClickHouse may return an error such as:

> The number of leaves in record doesn't match the number of elements in tuple...

This happens because ClickHouse expects all nested record structures to match the same schema.  
To handle this scenario, you can:

- Use `schema_inference_mode='union'` to merge different nested record schemas, or  
- Manually align your nested structures and enable  
  `use_structure_from_insertion_table_in_table_functions=1`.

:::note[Performance note]
`schema_inference_mode='union'` may take longer on very large S3 datasets because it must scan each file to infer the schema.
:::

**Example**
```sql
INSERT INTO data_stage
SELECT
    id,
    data
FROM s3('https://bucket-name/*.avro', 'Avro')
SETTINGS schema_inference_mode='union';

## Related {#related}

- [S3 engine](../../engines/table-engines/integrations/s3.md)
- [Integrating S3 with ClickHouse](/integrations/s3)
