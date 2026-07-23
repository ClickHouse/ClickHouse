---
description: 'Provides a read-only table-like interface to Lance datasets in Amazon S3.'
sidebar_label: 'lance'
sidebar_position: 91
slug: /sql-reference/table-functions/lance
title: 'lance'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# lance Table Function {#lance-table-function}

<ExperimentalBadge />

The `lanceS3` table function provides a read-only table-like interface to existing [Lance](https://lancedb.github.io/lance/) datasets stored in Amazon S3 or S3-compatible object storage.

`lanceS3` reads existing Lance datasets. It does not create datasets, write data, build indexes, or perform vector search.

## Syntax {#syntax}

```sql
lanceS3(url [, NOSIGN | access_key_id, secret_access_key [, session_token]])

lanceS3(named_collection[, option=value [,..]])

lanceS3(path_to_dataset, SETTINGS disk = 'disk_name')
```

## Arguments {#arguments}

The arguments for `lanceS3` follow the same S3 argument rules as other S3 data lake table functions such as `icebergS3`, `deltaLakeS3`, and `paimonS3`.

- `url` — S3 or S3-compatible URL to an existing Lance dataset.
- `access_key_id` and `secret_access_key` — Credentials used to access the dataset.
- `session_token` — Optional temporary credential session token.
- `NOSIGN` — Use an unsigned request.
- `named_collection` — Name of a [named collection](/operations/named-collections.md) containing S3 connection parameters.
- `filename` — Dataset path relative to the named collection URL.
- `SETTINGS disk = 'disk_name'` — Read through an S3 object-storage disk defined in server configuration.

Named arguments such as `access_key_id`, `secret_access_key`, `session_token`, `region`, `no_sign_request`, and `use_environment_credentials` are routed through the existing S3 configuration path.

## Returned value {#returned-value}

`lanceS3` returns a table whose schema is inferred from the Lance dataset snapshot selected at query analysis time.

The dataset snapshot is pinned in the query metadata. Reads use the pinned snapshot state, so execution does not drift to a newer Lance dataset version while the query is running.

## Examples {#examples}

Read a Lance dataset using explicit credentials:

```sql
SELECT *
FROM lanceS3(
    'https://bucket.s3.amazonaws.com/path/to/dataset.lance',
    'access_key_id',
    'secret_access_key');
```

Read a Lance dataset using named arguments:

```sql
SELECT *
FROM lanceS3(
    'https://bucket.s3.amazonaws.com/path/to/dataset.lance',
    access_key_id = 'access_key_id',
    secret_access_key = 'secret_access_key',
    region = 'us-east-1');
```

Read a Lance dataset using a named collection:

```xml
<clickhouse>
    <named_collections>
        <lance_conf>
            <url>https://bucket.s3.amazonaws.com/path/</url>
            <access_key_id>access_key_id</access_key_id>
            <secret_access_key>secret_access_key</secret_access_key>
        </lance_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT *
FROM lanceS3(lance_conf, filename = 'dataset.lance');
```

Read a Lance dataset through an S3 disk:

```sql
SELECT *
FROM lanceS3('dataset.lance', SETTINGS disk = 'lance_s3_disk');
```

## Virtual columns {#virtual-columns}

`lanceS3` supports the file-like virtual columns provided by the object-storage read path, including `_path`, `_file`, `_size`, `_time`, and `_etag`.

For data lake reads, `_data_lake_snapshot_version` contains the Lance snapshot id used by the query.

## Limitations {#limitations}

- `lanceS3` is read-only.
- Creating new Lance datasets from ClickHouse is not supported.
- Writing to Lance datasets is not supported.
- Lance indexes and vector search are not supported.
- Unsupported Lance or Arrow types fail with an `Unsupported Lance column` exception.

## See also {#see-also}

- [`LanceS3` table engine](/engines/table-engines/integrations/lance)
- [`s3` table function](/sql-reference/table-functions/s3)
- [`icebergS3` table function](/sql-reference/table-functions/iceberg)
