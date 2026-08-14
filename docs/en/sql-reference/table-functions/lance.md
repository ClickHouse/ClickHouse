---
description: 'Provides a read-only table-like interface to Lance datasets in Amazon S3.'
sidebar_label: 'lance'
sidebar_position: 91
slug: /sql-reference/table-functions/lance
title: 'lance'
doc_type: 'reference'
---

# lance Table Function {#lance-table-function}

The `lanceS3` table function provides a read-only table-like interface to existing [Lance](https://lancedb.github.io/lance/) datasets stored in Amazon S3 or S3-compatible object storage.

`lanceS3` reads existing Lance datasets. It does not create datasets, write data, build indexes, or perform vector search.

## Syntax {#syntax}

```sql
lanceS3(url [, NOSIGN | access_key_id, secret_access_key [, session_token]])

lanceS3(named_collection[, option=value [,..]])

lanceS3(path_to_dataset, SETTINGS disk = 'disk_name')

lanceS3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key [, session_token]])

lanceS3Cluster(cluster_name, named_collection[, option=value [,..]])
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
- `cluster_name` — (cluster form only) Name of a cluster used to distribute fragment packs across nodes.

## Cluster reads {#cluster-reads}

`lanceS3Cluster` is an experimental extension that splits a pinned Lance dataset into fragment packs on the initiator and assigns packs to workers (same pattern as `icebergS3Cluster` / `s3Cluster`).

- Tasks carry the pinned dataset version and fragment ids; workers do not fall back to the latest version.
- Credentials are not sent in tasks; each node uses its local configuration or named collection.
- `LIMIT` pushdown and count fast paths may force a single pack, so cluster parallelism may not help those queries.
- Prefer single-node `lanceS3` for small datasets.

Fragment packing is controlled by the same settings as single-node multi-stream reads (`lance_enable_fragment_parallelism`, `lance_fragment_pack_mode`, `lance_max_fragment_packs`, and related options). See [Lance table engine](/engines/table-engines/integrations/lance#read-parallelism).

Named arguments such as `access_key_id`, `secret_access_key`, `session_token`, `region`, `no_sign_request`, and `use_environment_credentials` are routed through the existing S3 configuration path.

## Returned value {#returned-value}

`lanceS3` returns a table whose schema is inferred from the Lance dataset version selected at query analysis time.

The dataset version is pinned in the query metadata. Reads use the pinned version, so execution does not drift to a newer Lance dataset version while the query is running. If the pinned version is removed before the query finishes, the query returns an exception instead of reading the latest version.

## Data types {#data-types}

`lanceS3` recursively converts the following Arrow types stored by Lance:

| Arrow type | ClickHouse type |
|---|---|
| `Boolean` | `Bool` |
| Signed and unsigned integers | Corresponding `Int8`–`Int64` or `UInt8`–`UInt64` type |
| `Float16`, `Float32`, `Float64` | `Float32`, `Float32`, `Float64` |
| `Utf8`, `LargeUtf8`, `Binary`, `LargeBinary` | `String` |
| `FixedSizeBinary(N)` | `FixedString(N)` |
| `Decimal128`, `Decimal256` | Corresponding `Decimal` type |
| `Date32` | `Date32` |
| `Timestamp` | `DateTime64` with scale `0`, `3`, `6`, or `9` |
| `Time32`, `Time64` | `Time64` with the corresponding scale |
| `Duration` | Corresponding `Interval` type |
| `List`, `LargeList`, `FixedSizeList` | `Array` |
| `Struct` | Named `Tuple` |
| `Map` | `Map` |

Nullability is preserved recursively for scalar fields, array elements, map values, and struct fields. A nullable Arrow `Struct` is inferred as `Nullable(Tuple(...))` when `enable_nullable_tuple_type = 1`.

ClickHouse cannot represent a container-level `NULL` for `Array` or `Map`. A nullable Lance list or map schema can be read when all container values are present, but a row containing a container-level `NULL` returns an exception instead of converting it to an empty container. A nullable `Struct` value likewise returns an exception unless `enable_nullable_tuple_type = 1`.

`Date64`, dictionary, union, run-end encoded, Arrow extension, and other unlisted types are rejected with an `Unsupported Lance column` exception. `Date64` is rejected because it has no lossless date-only ClickHouse mapping.

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

For data lake reads, `_data_lake_snapshot_version` contains the Lance dataset version used by the query.

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
