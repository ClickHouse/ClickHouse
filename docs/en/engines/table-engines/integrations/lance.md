---
description: 'This engine provides a read-only integration with existing Lance datasets in Amazon S3.'
sidebar_label: 'Lance'
sidebar_position: 96
slug: /engines/table-engines/integrations/lance
title: 'Lance table engine'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# Lance Table Engine {#lance-table-engine}

<ExperimentalBadge />

The `LanceS3` table engine provides a read-only integration with existing [Lance](https://lancedb.github.io/lance/) datasets stored in Amazon S3 or S3-compatible object storage.

`LanceS3` mounts existing Lance datasets. It does not create datasets, write data, build indexes, or perform vector search.

## Create table {#create-table}

```sql
CREATE TABLE lance_table
ENGINE = LanceS3(url [, NOSIGN | access_key_id, secret_access_key [, session_token]]);
```

Using a named collection:

```sql
CREATE TABLE lance_table
ENGINE = LanceS3(lance_conf, filename = 'dataset.lance');
```

Using an S3 disk:

```sql
CREATE TABLE lance_table
ENGINE = LanceS3('dataset.lance')
SETTINGS disk = 'lance_s3_disk';
```

The Lance dataset must already exist. When no columns are specified, ClickHouse infers the table schema from the Lance dataset.

## Engine arguments {#engine-arguments}

The arguments for `LanceS3` follow the same S3 argument rules as other S3 data lake table engines such as `IcebergS3`, `DeltaLakeS3`, and `PaimonS3`.

- `url` — S3 or S3-compatible URL to an existing Lance dataset.
- `access_key_id` and `secret_access_key` — Credentials used to access the dataset.
- `session_token` — Optional temporary credential session token.
- `NOSIGN` — Use an unsigned request.
- `named_collection` — Name of a [named collection](/operations/named-collections.md) containing S3 connection parameters.
- `filename` — Dataset path relative to the named collection URL.
- `SETTINGS disk = 'disk_name'` — Read through an S3 object-storage disk defined in server configuration.

Named arguments and named collection fields such as `region`, `no_sign_request`, and `use_environment_credentials` are routed through the existing S3 configuration path.

## Explicit schema {#explicit-schema}

`LanceS3` supports an explicit table schema when it is compatible with the inferred Lance dataset schema:

```sql
CREATE TABLE lance_table
(
    id UInt64,
    name String,
    score Nullable(Int64)
)
ENGINE = LanceS3(lance_conf, filename = 'dataset.lance');
```

If an explicit column is missing from the dataset or has an incompatible type, ClickHouse returns `BAD_ARGUMENTS`.

## Data types {#data-types}

`LanceS3` supports primitive numeric and string types, decimals, `Date32`, timestamps, time and duration types, `Array`, named `Tuple`, and `Map`. Nullability is preserved recursively where the corresponding ClickHouse type can represent it.

Container-level `NULL` values in `Array` and `Map` are rejected because ClickHouse cannot represent them without losing information. Nullable `Struct` values require `enable_nullable_tuple_type = 1`. See the [`lanceS3` data type mapping](/sql-reference/table-functions/lance#data-types) for the complete mapping and unsupported types.

## Snapshot state {#snapshot-state}

`LanceS3` captures the current Lance dataset version through `DataLakeTableStateSnapshot` during query analysis. Reads use the saved `Lance::TableStateSnapshot`, so query execution uses a consistent dataset version even if the dataset is updated concurrently. If the pinned version is removed before the query finishes, the query returns an exception instead of reading the latest version.

The virtual column `_data_lake_snapshot_version` exposes the Lance dataset version used by the query.

## Observability {#observability}

Lance reads emit dedicated `ProfileEvents` on `system.query_log` / `system.query_thread_log`, including:

- Open / plan / next-batch counts and microseconds (`LanceDatasetOpen*`, `LancePlanScan*`, `LanceNextBatch*`)
- Query-scoped dataset cache hits (`LanceDatasetCacheHit`)
- Batches, rows, and approximate Arrow buffer bytes (`LanceBatchesRead`, `LanceRowsRead`, `LanceReadBytes`, `LanceLocalReadBytes`, `LanceS3ReadBytes`)
- Arrow to ClickHouse conversion time (`LanceArrowConvertMicroseconds`)
- Count fast path (`LanceCountRows`, `LanceCountRowsMicroseconds`)
- Predicate and limit pushdown (`LancePredicatePushdownComplete`, `LancePredicatePushdownPartial`, `LanceLimitPushdown`, `LanceProjectedColumns`)

`LanceReadBytes` / `LanceS3ReadBytes` measure Arrow array buffer footprint observed after the Lance scanner returns a batch. They are not S3 wire GET byte counters. Fragment-level scanned/skipped counters are not exposed yet.

## Limitations {#limitations}

- `LanceS3` is read-only.
- Creating new Lance datasets from ClickHouse is not supported.
- Writing to Lance datasets is not supported.
- Lance indexes and vector search are not supported.
- Unsupported Lance or Arrow types fail with an `Unsupported Lance column` exception.
- Non-S3 disks are rejected with `BAD_ARGUMENTS`.

## See also {#see-also}

- [`lanceS3` table function](/sql-reference/table-functions/lance)
- [`S3` table engine](/engines/table-engines/integrations/s3)
- [`IcebergS3` table engine](/engines/table-engines/integrations/iceberg)
