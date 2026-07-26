---
description: 'Stores one deterministic latest row per composite key in memory and serves indexed lookups.'
sidebar_label: 'OverwriteCache'
sidebar_position: 115
slug: /engines/table-engines/special/overwrite-cache
title: 'OverwriteCache table engine'
doc_type: 'reference'
---

The `OverwriteCache` engine stores exactly one winning row for each composite key in RAM. It is intended for bounded latest-state caches that are rebuilt from an upstream source.

Data is not persisted. Restarting the server, detaching and attaching the table, or recreating the table produces an empty cache.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE latest_state
(
    website_type UInt8,
    user_id UInt64,
    tag LowCardinality(String),
    version DateTime64(3),
    source_sequence UInt64,
    value String
)
ENGINE = OverwriteCache(version)
KEYS (website_type, user_id, tag)
SETTINGS
    max_memory_bytes = 1073741824,
    equal_version_tiebreak_columns = 'source_sequence',
    secondary_index_columns = 'tag',
    secondary_index_segment_column = 'website_type',
    max_secondary_index_rows = 1000000;
```

`OverwriteCache` requires one engine argument identifying the version column and a nonempty storage-level `KEYS (...)` clause. The version column cannot also be a key column.

## Winner selection {#winner-selection}

For each composite key, an inserted row is handled as follows:

1. A greater version replaces the current row.
2. A lower version is ignored.
3. Equal versions compare `equal_version_tiebreak_columns` in declaration order.
4. Equal winner metadata and an identical payload is an idempotent no-op.
5. Equal winner metadata and a different payload causes the complete inserted block to be rejected.

Conflict and memory-limit validation occurs before publishing mutations from an inserted block. `OverwriteCache` does not evict rows when its memory limit is reached.

## Concurrent publication {#concurrent-publication}

`OverwriteCache` addresses primary keys and secondary postings through fixed hash shards. Existing rows use a fixed set of striped row locks rather than one lock per row. This bounds lock memory independently of the number of stored keys.

An inserted block is prepared as a pending publication. Readers continue using the previously committed rows while the writer installs pending rows across the affected shards. One atomic generation change then makes the complete block visible. A query captures one generation, so it does not mix rows from before and after the same publication.

The atomicity boundary is one input block received by the table-engine sink. A SQL `INSERT` can be divided into multiple input blocks by its pipeline; earlier blocks remain committed if a later block is rejected. Buffering a complete statement would make publication size unbounded and is intentionally not part of this in-memory engine's contract.

Writers are serialized, but a large replacement publication does not take an engine-wide exclusive reader lock. New primary keys can briefly contend with readers of the same primary or posting shard. Large secondary-index results can still require substantial traversal and result-materialization work; sharding does not make an unbounded secondary result inexpensive.

## Settings {#settings}

- `max_memory_bytes` — Required hard admission limit for engine-accounted memory. It must be greater than zero.
- `equal_version_tiebreak_columns` — Optional comma-separated column names used to select a deterministic winner when versions are equal. These columns cannot be key or version columns.
- `secondary_index_columns` — Optional comma-separated key columns used for indexed equality and `IN` reads.
- `secondary_index_segment_column` — Optional key column prepended to the configured secondary index.
- `max_secondary_index_rows` — Required when a secondary index is configured. A lookup is rejected before returning more candidate rows than this value.

Column names in each list must be unique. Every secondary-index column must be present in `KEYS (...)`.

## Reading rows {#reading-rows}

Normal `SELECT` queries must contain either:

- equality or `IN` predicates that fully specify all columns in `KEYS (...)`; or
- equality or `IN` predicates on all `secondary_index_columns`, optionally narrowed by `secondary_index_segment_column`.

Other predicates can be applied after an indexed lookup, but they cannot be the only access path. Queries that require an unrestricted or partial-key scan are rejected.

```sql
SELECT value
FROM latest_state
WHERE website_type = 1 AND user_id = 42 AND tag = 'risk';

SELECT user_id, value
FROM latest_state
WHERE website_type = 1 AND tag = 'risk';
```

## Scalar lookup functions {#scalar-lookup-functions}

`overwriteCacheGet` and `overwriteCacheGetOrNull` perform full composite-key lookups. The table name and returned column name must be constant strings. Key arguments follow the order in `KEYS (...)`.

```sql
SELECT overwriteCacheGetOrNull(
    'default.latest_state',
    'value',
    toUInt8(1),
    toUInt64(42),
    'risk'
);
```

`overwriteCacheGet` returns the column default for a missing key. `overwriteCacheGetOrNull` returns `NULL`.
