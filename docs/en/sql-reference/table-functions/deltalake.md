---
description: 'Provides a read-only table-like interface to the Delta Lake tables in
  Amazon S3.'
sidebar_label: 'deltaLake'
sidebar_position: 45
slug: /sql-reference/table-functions/deltalake
title: 'deltaLake'
doc_type: 'reference'
---

# deltaLake table function

Provides a table-like interface to [Delta Lake](https://github.com/delta-io/delta) tables in Amazon S3, Azure Blob Storage, or a locally mounted file system, supporting both reads and writes (from v25.10)

## Syntax {#syntax}

`deltaLake` is an alias of `deltaLakeS3` which is supported for compatibility.

```sql
deltaLake(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])

deltaLakeS3(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])

deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

deltaLakeLocal(path, [,format])
```

## Arguments {#arguments}

The arguments for this table function are the same as for the `s3`, `azureBlobStorage`, `HDFS` and `file` table functions respectively.
The `format` argument stands for the format of data files in the Delta lake table.

## Returned value {#returned_value}

Returns a table with the specified structure for reading or writing data from/to the specified Delta Lake table.

## Examples {#examples}

### Reading data {#reading-data}

Consider a table in S3 storage at `https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/`.
To read data from the table in ClickHouse, run:

```sql title="Query"
SELECT
    URL,
    UserAgent
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/')
WHERE URL IS NOT NULL
LIMIT 2
```

```response title="Response"
┌─URL───────────────────────────────────────────────────────────────────┬─UserAgent─┐
│ http://auto.ria.ua/search/index.kz/jobinmoscow/detail/55089/hasimages │         1 │
│ http://auto.ria.ua/search/index.kz/jobinmoscow.ru/gosushi             │         1 │
└───────────────────────────────────────────────────────────────────────┴───────────┘
```

### Inserting data {#inserting-data}

Consider a table in S3 storage at `s3://ch-docs-s3-bucket/people_10k/`.
To insert data into the table, first enable the experimental feature:

```sql
SET allow_experimental_delta_lake_writes=1
```

Then write:

```sql title="Query"
INSERT INTO TABLE FUNCTION deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>') VALUES (10001, 'John', 'Smith', 'Male', 30)
```

```response title="Response"
Query id: 09069b47-89fa-4660-9e42-3d8b1dde9b17

Ok.

1 row in set. Elapsed: 3.426 sec.
```

You can confirm the insert worked by reading the table again:

```sql title="Query"
SELECT *
FROM deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>')
WHERE (firstname = 'John') AND (lastname = 'Smith')
```

```response title="Response"
Query id: 65032944-bed6-4d45-86b3-a71205a2b659

   ┌────id─┬─firstname─┬─lastname─┬─gender─┬─age─┐
1. │ 10001 │ John      │ Smith    │ Male   │  30 │
   └───────┴───────────┴──────────┴────────┴─────┘
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Related {#related}

- [DeltaLake engine](engines/table-engines/integrations/deltalake.md)
- [DeltaLake cluster table function](sql-reference/table-functions/deltalakeCluster.md)
