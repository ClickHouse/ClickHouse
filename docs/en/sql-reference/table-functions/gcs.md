---
slug: /en/sql-reference/table-functions/gcs
sidebar_position: 70
sidebar_label: gcs
keywords: [gcs, bucket]
---

# gcs Table Function

Provides a table-like interface to select/insert files in [Google Cloud Storage](https://cloud.google.com/storage/).

**Syntax**

``` sql
gcs(path [,hmac_key, hmac_secret] [,format] [,structure] [,compression])
```

:::tip GCS
The GCS Table Function integrates with Google Cloud Storage by using the GCS XML API and HMAC keys. See the [Google interoperability docs]( https://cloud.google.com/storage/docs/interoperability) for more details about the endpoint and HMAC.

:::

**Arguments**

-   `path` — Bucket url with path to file. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings.

  :::note GCS
  The GCS path is in this format as the endpoint for the Google XML API is different than the JSON API:
  ```
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
  ```
  and not ~~https://storage.cloud.google.com~~.
  :::

-   `format` — The [format](../../interfaces/formats.md#formats) of the file.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.
-   `compression` — Parameter is optional. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

**Examples**

Selecting the first two rows from the table from GCS file `https://storage.googleapis.com/my-test-bucket-768/data.csv`:

``` sql
SELECT *
FROM gcs('https://storage.googleapis.com/my-test-bucket-768/data.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

The similar but from file with `gzip` compression:

``` sql
SELECT *
FROM gcs('https://storage.googleapis.com/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32', 'gzip')
LIMIT 2;
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## Usage

Suppose that we have several files with following URIs on GCS:

-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_1.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_2.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_3.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_4.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_1.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_2.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_3.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_4.csv'

Count the amount of rows in files ending with numbers from 1 to 3:

``` sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'name String, value UInt32')
```

``` text
┌─count()─┐
│      18 │
└─────────┘
```

Count the total amount of rows in all files in these two directories:

``` sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'name String, value UInt32')
```

``` text
┌─count()─┐
│      24 │
└─────────┘
```

:::warning
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

Count the total amount of rows in files named `file-000.csv`, `file-001.csv`, … , `file-999.csv`:

``` sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'name String, value UInt32');
```

``` text
┌─count()─┐
│      12 │
└─────────┘
```

Insert data into file `test-data.csv.gz`:

``` sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

Insert data into file `test-data.csv.gz` from existing table:

``` sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

Glob ** can be used for recursive directory traversal. Consider the below example, it will fetch all files from `my-test-bucket-768` directory recursively:

``` sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

The below get data from all `test-data.csv.gz` files from any folder inside `my-test-bucket` directory recursively:

``` sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

## Partitioned Write

If you specify `PARTITION BY` expression when inserting data into `GCS` table, a separate file is created for each partition value. Splitting the data into separate files helps to improve reading operations efficiency.

**Examples**

1. Using partition ID in a key creates separate files:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```
As a result, the data is written into three files: `file_x.csv`, `file_y.csv`, and `file_z.csv`.

2. Using partition ID in a bucket name creates files in different buckets:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```
As a result, the data is written into three files in different buckets: `my_bucket_1/file.csv`, `my_bucket_10/file.csv`, and `my_bucket_20/file.csv`.

**See Also**

-   [S3 table function](s3.md)
-   [S3 engine](../../engines/table-engines/integrations/s3.md)
