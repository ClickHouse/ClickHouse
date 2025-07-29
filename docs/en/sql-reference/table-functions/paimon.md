---
description: 'Provides a read-only table-like interface to Apache Paimon tables in
  Amazon S3, Azure, HDFS or locally stored.'
sidebar_label: 'paimon'
sidebar_position: 90
slug: /sql-reference/table-functions/paimon
title: 'paimon'
---

# paimon Table Function {#paimon-table-function}

Provides a read-only table-like interface to Apache [Paimon](https://paimon.apache.org/) tables in Amazon S3, Azure, HDFS or locally stored.

## Syntax {#syntax}

```sql
paimonS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method])
paimonS3(named_collection[, option=value [,..]])

paimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
paimonAzure(named_collection[, option=value [,..]])

paimonHDFS(path_to_table, [,format] [,compression_method])
paimonHDFS(named_collection[, option=value [,..]])

paimonLocal(path_to_table, [,format] [,compression_method])
paimonLocal(named_collection[, option=value [,..]])
```

## Arguments {#arguments}

Description of the arguments coincides with description of arguments in table functions `s3`, `azureBlobStorage`, `HDFS` and `file` correspondingly.
`format` stands for the format of data files in the Paimon table.

### Returned value {#returned-value}

A table with the specified structure for reading data in the specified Paimon table.

### Example {#example}

```sql
SELECT * FROM paimonS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
ClickHouse currently only supports reading v3 of the Paimon format via the `paimonS3`, `paimonAzure`, `paimonHDFS` and `paimonLocal` table functions.
:::

## Defining a named collection {#defining-a-named-collection}

Here is an example of configuring a named collection for storing the URL and credentials:

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test<access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM paimonS3(paimon_conf, filename = 'test_table')
DESCRIBE paimonS3(paimon_conf, filename = 'test_table')
```

## Schema Evolution {#schema-evolution}

At the moment, with the help of CH, you can read paimon tables, the schema of which has changed over time. We currently support reading tables where columns have been added and removed, and their order has changed. You can also change a column where a value is required to one where NULL is allowed. Additionally, we support permitted type casting for simple types, namely:  

* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P.

Currently, it is not possible to change nested structures or the types of elements within arrays and maps.

## Aliases {#aliases}

Table function `paimon` is an alias to `paimonS3` now.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## See Also {#see-also}

* [Paimon cluster table function](/sql-reference/table-functions/paimonCluster.md)
