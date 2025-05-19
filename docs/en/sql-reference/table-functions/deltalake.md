---
description: 'Provides a read-only table-like interface to the Delta Lake tables in
  Amazon S3.'
sidebar_label: 'deltaLake'
sidebar_position: 45
slug: /sql-reference/table-functions/deltalake
title: 'deltaLake'
---

# deltaLake Table Function

Provides a read-only table-like interface to [Delta Lake](https://github.com/delta-io/delta) tables in Amazon S3 or Azure Blob Storage.

## Syntax {#syntax}

`deltaLake` is an alias of `deltaLakeS3`, its supported for compatibility.


```sql
deltaLake(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])

deltaLakeS3(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])

deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
```

## Arguments {#arguments}

Description of the arguments coincides with description of arguments in table functions `s3`, `azureBlobStorage`, `HDFS` and `file` correspondingly.
`format` stands for the format of data files in the Delta lake table.

**Returned value**

A table with the specified structure for reading data in the specified Delta Lake table.

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
