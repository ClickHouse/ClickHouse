---
description: 'Provides a read-only table-like interface to Apache Paimon tables in
  Amazon S3, Azure, HDFS or locally stored.'
sidebar_label: 'paimon'
sidebar_position: 90
slug: /sql-reference/table-functions/paimon
title: 'paimon'
doc_type: 'reference'
---

# paimon Table Function {#paimon-table-function}

Provides a read-only table-like interface to Apache [Paimon](https://paimon.apache.org/) tables in Amazon S3, Azure, HDFS or locally stored.

## Syntax {#syntax}

```sql
paimon(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression])

paimonS3(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression])

paimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFS(path_to_table, [,format] [,compression_method])

paimonLocal(path_to_table, [,format] [,compression_method])
```

## Arguments {#arguments}

Description of the arguments coincides with description of arguments in table functions `s3`, `azureBlobStorage`, `HDFS` and `file` correspondingly.
`format` stands for the format of data files in the Paimon table.

### Returned value {#returned-value}

A table with the specified structure for reading data in the specified Paimon table.

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

## Aliases {#aliases}

Table function `paimon` is an alias to `paimonS3` now.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Data Types supported {#data-types-supported}

| Paimon Data Type | Clickhouse Data Type 
|-------|--------|
|BOOLEAN     |Int8      |
|TINYINT     |Int8      |
|SMALLINT     |Int16      |
|INTEGER     |Int32      |
|BIGINT     |Int64      |
|FLOAT     |Float32      |
|DOUBLE     |Float64      |
|STRING,VARCHAR,BYTES,VARBINARY     |String      |
|DATE     |Date      |
|TIME(p),TIME     |Time('UTC')      |
|TIMESTAMP(p) WITH LOCAL TIME ZONE     |DateTime64      |
|TIMESTAMP(p)     |DateTime64('UTC')      |
|CHAR     |FixedString(1)      |
|BINARY(n)     |FixedString(n)      |
|DECIMAL(P,S)     |Decimal(P,S)      |
|ARRAY     |Array      |
|MAP     |Map    |

## Partition supported {#partition-supported}
Data types supported in Paimon partition keys:
* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`

## See Also {#see-also}

* [Paimon cluster table function](/sql-reference/table-functions/paimonCluster.md)
