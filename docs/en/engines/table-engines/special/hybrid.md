---
description: 'Hybrid unions multiple data sources behind per-segment predicates so queries behave like a single table while data is migrated or tiered.'
slug: /engines/table-engines/special/hybrid
title: 'Hybrid Table Engine'
sidebar_label: 'Hybrid'
sidebar_position: 11
---

# Hybrid table engine

`Hybrid` builds on top of the [Distributed](./distributed.md) table engine. It lets you expose several data sources as one logical table and assign every source its own predicate.
The engine rewrites incoming queries so that each segment receives the original query plus its predicate. This keeps all of the Distributed optimisations (remote aggregation, `skip_unused_shards`,
global JOIN pushdown, and so on) while you duplicate or migrate data across clusters, storage types, or formats.

It keeps the same execution pipeline as `engine=Distributed` but can read from multiple underlying sources simultaneously—similar to `engine=Merge`—while still pushing logic down to each source.

Typical use cases include:

- Zero-downtime migrations where "old" and "new" replicas temporarily overlap.
- Tiered storage, for example fresh data on a local cluster and historical data in S3.
- Gradual roll-outs where only a subset of rows should be served from a new backend.

By giving mutually exclusive predicates to the segments (for example, `date < watermark` and `date >= watermark`), you ensure that each row is read from exactly one source.

## Enable the engine

The Hybrid engine is experimental. Enable it per session (or in the user profile) before creating tables:

```sql
SET allow_experimental_hybrid_table = 1;
```

### Automatic Type Alignment

Hybrid segments can evolve independently, so the same logical column may use different physical types. With the experimental `hybrid_table_auto_cast_columns = 1` **(enabled by default and requires `allow_experimental_analyzer = 1`)**, the engine inserts the necessary `CAST` operations into each rewritten query so every shard receives the schema defined by the Hybrid table. You can opt out by setting the flag to `0` if it causes issues.

Segment schemas are cached when you create or attach a Hybrid table. If you alter a segment later (for example change a column type), refresh the Hybrid table (detach/attach or recreate it) so the cached headers stay in sync with the new schema; otherwise the auto-cast feature may miss the change and queries can still fail with header/type errors.

## Engine definition

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    column1 type1,
    column2 type2,
    ...
)
ENGINE = Hybrid(table_function_1, predicate_1 [, table_function_2, predicate_2 ...])
```

You must pass at least two arguments – the first table function and its predicate. Additional sources are appended as `table_function, predicate` pairs. The first table function is also used for `INSERT` statements.

### Arguments and behaviour

- `table_function_n` must be a valid table function (for example `remote`, `remoteSecure`, `cluster`, `clusterAllReplicas`, `s3Cluster`) or a fully qualified table name (`database.table`). The first argument must be a table function—such as `remote` or `cluster`—because it instantiates the underlying `Distributed` storage.
- `predicate_n` must be an expression that can be evaluated on the table columns. The engine adds it to the segment's query with an additional `AND`, so expressions like `event_date >= '2025-09-01'` or `id BETWEEN 10 AND 15` are typical.
- The query planner picks the same processing stage for every segment as it does for the base `Distributed` plan, so remote aggregation, ORDER BY pushdown, `skip_unused_shards`, and the legacy/analyzer execution modes behave the same way.
- `INSERT` statements are forwarded to the first table function only. If you need multi-destination writes, use explicit `INSERT` statements into the respective sources.
- Align schemas across the segments. ClickHouse builds a common header and rejects creation if any segment misses a column defined in the Hybrid schema. If the physical types differ you may need to add casts on one side or in the query, just as you would when reading from heterogeneous replicas.

## Example: local cluster plus S3 historical tier

The following commands illustrate a two-segment layout. Hot data stays on a local ClickHouse cluster, while historical rows come from public S3 Parquet files.

```sql
-- Local MergeTree table that keeps current data
CREATE OR REPLACE TABLE btc_blocks_local
(
    `hash` FixedString(64),
    `version` Int64,
    `mediantime` DateTime64(9),
    `nonce` Int64,
    `bits` FixedString(8),
    `difficulty` Float64,
    `chainwork` FixedString(64),
    `size` Int64,
    `weight` Int64,
    `coinbase_param` String,
    `number` Int64,
    `transaction_count` Int64,
    `merkle_root` FixedString(64),
    `stripped_size` Int64,
    `timestamp` DateTime64(9),
    `date` Date
)
ENGINE = MergeTree
ORDER BY (timestamp)
PARTITION BY toYYYYMM(date);

-- Hybrid table that unions the local shard with historical data in S3
CREATE OR REPLACE TABLE btc_blocks ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'btc_blocks_local'), date >= '2025-09-01',
    s3('s3://aws-public-blockchain/v1.0/btc/blocks/**.parquet', NOSIGN), date < '2025-09-01'
) AS btc_blocks_local;

-- Writes target the first (remote) segment
INSERT INTO btc_blocks
SELECT *
FROM s3('s3://aws-public-blockchain/v1.0/btc/blocks/**.parquet', NOSIGN)
WHERE date BETWEEN '2025-09-01' AND '2025-09-30';

-- Reads seamlessly combine both predicates
SELECT * FROM btc_blocks WHERE date = '2025-08-01'; -- data from s3
SELECT * FROM btc_blocks WHERE date = '2025-09-05'; -- data from MergeTree (TODO: still analyzes s3)
SELECT * FROM btc_blocks WHERE date IN ('2025-08-31','2025-09-01') -- data from both sources, single copy always


-- Run analytic queries as usual
SELECT
    date,
    count(),
    uniqExact(CAST(hash, 'Nullable(String)')) AS hashes,
    sum(CAST(number, 'Nullable(Int64)')) AS blocks_seen
FROM btc_blocks
WHERE date BETWEEN '2025-08-01' AND '2025-09-30'
GROUP BY date
ORDER BY date;
```

Because the predicates are applied inside every segment, queries such as `ORDER BY`, `GROUP BY`, `LIMIT`, `JOIN`, and `EXPLAIN` behave as if you were reading from a single `Distributed` table. When sources expose different physical types (for example `FixedString(64)` versus `String` in Parquet), add explicit casts during ingestion or in the query, as shown above.
