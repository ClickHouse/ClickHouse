---
description: 'Provides a read-only table-like interface to Apache Hudi tables in Amazon
  S3.'
sidebar_label: 'hudi'
sidebar_position: 85
slug: /sql-reference/table-functions/hudi
title: 'hudi'
doc_type: 'reference'
---

# hudi Table Function

Provides a read-only table-like interface to Apache [Hudi](https://hudi.apache.org/) tables in Amazon S3.

## Syntax {#syntax}

```sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])
```

## Arguments {#arguments}

| Argument                                     | Description                                                                                                                                                                                                                                                                                                                                                                           |
|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`                                        | Bucket url with the path to an existing Hudi table in S3.                                                                                                                                                                                                                                                                                                                             |
| `aws_access_key_id`, `aws_secret_access_key` | Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. These parameters are optional. If credentials are not specified, they are used from the ClickHouse configuration. For more information see [Using S3 for Data Storage](/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | The [format](/interfaces/formats) of the file.                                                                                                                                                                                                                                                                                                                                        |
| `structure`                                  | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                         |
| `compression`                                | Parameter is optional. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, compression will be autodetected by the file extension.                                                                                                                                                                                                                   |

## Returned value {#returned_value}

A table with the specified structure for reading data in the specified Hudi table in S3.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Related {#related}

- [Hudi engine](/engines/table-engines/integrations/hudi.md)
- [Hudi cluster table function](/sql-reference/table-functions/hudiCluster.md)
