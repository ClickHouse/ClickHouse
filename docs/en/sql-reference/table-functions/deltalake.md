---
description: 'Provides a read-only table-like interface to the Delta Lake tables in
  Amazon S3.'
sidebar_label: 'deltaLake'
sidebar_position: 45
slug: /sql-reference/table-functions/deltalake
title: 'deltaLake'
---

# deltaLake Table Function

Provides a read-only table-like interface to the [Delta Lake](https://github.com/delta-io/delta) tables in Amazon S3.

## Syntax {#syntax}

```sql
deltaLake(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])
```

## Arguments {#arguments}

- `url` — Bucket url with path to existing Delta Lake table in S3.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. These parameters are optional. If credentials are not specified, they are used from the ClickHouse configuration. For more information see [Using S3 for Data Storage](engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3).
- `format` — The [format](/interfaces/formats) of the file.
- `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.
- `compression` — Parameter is optional. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, compression will be autodetected by the file extension.

**Returned value**

A table with the specified structure for reading data in the specified Delta Lake table in S3.

**Examples**

Selecting rows from the table in S3 `https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/`:

```sql
SELECT
    URL,
    UserAgent
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/')
WHERE URL IS NOT NULL
LIMIT 2
```

```response
┌─URL───────────────────────────────────────────────────────────────────┬─UserAgent─┐
│ http://auto.ria.ua/search/index.kz/jobinmoscow/detail/55089/hasimages │         1 │
│ http://auto.ria.ua/search/index.kz/jobinmoscow.ru/gosushi             │         1 │
└───────────────────────────────────────────────────────────────────────┴───────────┘
```

**See Also**

- [DeltaLake engine](engines/table-engines/integrations/deltalake.md)
- [DeltaLake cluster table function](sql-reference/table-functions/deltalakeCluster.md)
