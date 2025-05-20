---
slug: /en/sql-reference/table-functions/hudi
sidebar_position: 85
sidebar_label: hudi
---

# hudi Table Function

Provides a read-only table-like interface to Apache [Hudi](https://hudi.apache.org/) tables in Amazon S3.

## Syntax

``` sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])
```

## Arguments

- `url` — Bucket url with the path to an existing Hudi table in S3.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. These parameters are optional. If credentials are not specified, they are used from the ClickHouse configuration. For more information see [Using S3 for Data Storage](/docs/en/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3).
- `format` — The [format](/docs/en/interfaces/formats.md/#formats) of the file.
- `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.
- `compression` — Parameter is optional. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, compression will be autodetected by the file extension.

**Returned value**

A table with the specified structure for reading data in the specified Hudi table in S3.

**See Also**

- [Hudi engine](/docs/en/engines/table-engines/integrations/hudi.md)

