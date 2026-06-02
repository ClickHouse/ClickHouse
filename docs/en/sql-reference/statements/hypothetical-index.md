---
description: 'Documentation for hypothetical (what-if) indexes'
sidebar_label: 'HYPOTHETICAL INDEX'
sidebar_position: 47
slug: /sql-reference/statements/hypothetical-index
title: 'Hypothetical Indexes'
doc_type: 'reference'
---

# Hypothetical Indexes {#hypothetical-indexes}

Hypothetical indexes are virtual, session-scoped skip indexes that you can attach to a `MergeTree` family table without actually building or storing them. They exist only inside the current session and are used by [`EXPLAIN WHATIF`](/sql-reference/statements/explain#explain-whatif) to estimate how a real skip index would affect a query — typically the skip ratio (fraction of marks that could be skipped) and a rough cost in marks and bytes.

Use hypothetical indexes to evaluate candidate indexes before paying the cost of materializing them on disk.

## CREATE HYPOTHETICAL INDEX {#create-hypothetical-index}

```sql
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name
    ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
```

The syntax mirrors `ALTER TABLE ... ADD INDEX`, but no data is read or written — the index description is stored only in the current session.

- `name` — index name; must be unique within `(database, table)` for this session.
- `expression` — the column or expression to index.
- `TYPE type` — `minmax`, `set(N)`, `bloom_filter(p)`, `ngrambf_v1(...)`, `tokenbf_v1(...)`. `text` and `vector_similarity` are not supported and rejected at `CREATE` time, because their real `ALTER TABLE ... ADD INDEX` validation depends on table-level settings the session-only store can't replicate.
- `GRANULARITY value` — number of data granules per index granule. Defaults to 1.

The target table must be a `MergeTree` family table in an `Atomic` database (it must have a UUID). Tables without a UUID — for example in a legacy `Ordinary` database, or old-syntax `MergeTree` — are rejected, because the session store keys hypothetical indexes by table UUID.

**Example**

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
```

## Evaluating a hypothetical index with EXPLAIN WHATIF {#evaluating-a-hypothetical-index-with-explain-whatif}

Defining a hypothetical index by itself does nothing — to see how it would affect a query, run [`EXPLAIN WHATIF`](/sql-reference/statements/explain#explain-whatif) against a representative `SELECT`. The estimator reports each candidate index's applicability, the marks it would read, the resulting skip ratio, and how the estimate was produced (`empirical`, `statistical`, or `applicability_only`).

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

Result:

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
  elapsed_us:       631
```

To skip the in-memory empirical scan and estimate from [column statistics](/engines/table-engines/mergetree-family/mergetree#column-statistics) instead, define them on the relevant columns first (they are off by default), wait for the materialize mutation to finish, then disable the empirical path:

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  skip_ratio:   99.9%

Estimation:
  source:           statistical
```

See the [`EXPLAIN WHATIF`](/sql-reference/statements/explain#explain-whatif) reference for the full output schema and settings.

## DROP HYPOTHETICAL INDEX {#drop-hypothetical-index}

```sql
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
```

Removes a hypothetical index from the current session.

## DROP ALL HYPOTHETICAL INDEXES {#drop-all-hypothetical-indexes}

```sql
DROP ALL HYPOTHETICAL INDEXES
```

Clears every hypothetical index defined in the current session, regardless of table.

## Scope and lifetime {#scope-and-lifetime}

- Hypothetical indexes live only in the **current session** — they are invisible to other sessions and discarded when the session ends.
- They never read or write data; real queries against the table are unaffected.
- Inspect the current session's hypothetical indexes via [`system.hypothetical_indexes`](/operations/system-tables/hypothetical_indexes).

## Limitations {#limitations}

`text` and `vector_similarity` candidates are rejected at `CREATE HYPOTHETICAL INDEX` time, because their real validation depends on table-level settings the session-only store cannot replicate.

`EXPLAIN WHATIF` reports `status: not_applicable` for queries with `FINAL` (skip-index pruning interacts with `PrimaryKeyExpand`), and errors with `NOT_IMPLEMENTED` when the query is served from a projection (a parent-table index is not materialized on projection parts).

The empirical `skip_ratio` is an **upper bound**: it counts each surviving granule independently and does not model seek-gap coalescing (`merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek`), nor the combination of a candidate with an existing skip index under a disjunctive (`OR`) predicate. A real materialized index may therefore read slightly more, or prune in cases the estimate does not.

## Required privileges {#required-privileges}

`SELECT` on the target table.

## See also {#see-also}

- [`EXPLAIN WHATIF`](/sql-reference/statements/explain#explain-whatif)
- [`system.hypothetical_indexes`](/operations/system-tables/hypothetical_indexes)
- [Data skipping indexes](/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)
