---
description: 'Table-wide lookup indexes for `MergeTree` tables.'
keywords: ['lookup index', 'table_set', 'table_join', 'MergeTree', 'index']
sidebar_label: 'Lookup Indexes'
sidebar_position: 100
slug: /engines/table-engines/mergetree-family/lookupindexes
title: 'Lookup Indexes'
doc_type: 'guide'
---

# Lookup indexes {#lookup-indexes}

`MergeTree` tables support table-wide lookup indexes declared with `LOOKUP INDEX`.
They are designed for fast key-value style lookups on the right-hand side of a query.

Unlike data skipping indexes, lookup indexes do not prune parts or granules.
Instead, ClickHouse builds an in-memory lookup structure from the indexed table and reuses it across queries.
The feature is experimental and disabled by default. Enable it with the setting `allow_experimental_lookup_index = 1`.

This means:

- the first query may pay the cost of building the lookup structure;
- repeated queries can be significantly faster;
- `GRANULARITY` is not used.

Lookup indexes are supported for `MergeTree` tables, including `ReplicatedMergeTree`.

## Types {#types}

Two lookup index types are available:

- `table_set` builds a table-wide `Set` and is used for membership checks such as `IN table`;
- `table_join` builds a table-wide key-value lookup and is used for direct-compatible joins.

## Creating a lookup index {#creating}

You can define a lookup index in `CREATE TABLE`:

```sql
CREATE TABLE dim
(
    id UInt64,
    subid UInt64,
    value String,
    LOOKUP INDEX idx_set (id, subid) TYPE table_set
)
ENGINE = MergeTree
ORDER BY (id, subid);
```

You can also add one with `ALTER TABLE`:

```sql
ALTER TABLE dim ADD LOOKUP INDEX idx_set (id, subid) TYPE table_set;
ALTER TABLE dim DROP LOOKUP INDEX idx_set;
```

## Using `table_set` {#table-set}

`table_set` is intended for membership checks.

Example:

```sql
CREATE TABLE fact
(
    id UInt64,
    subid UInt64
)
ENGINE = MergeTree
ORDER BY (id, subid);

SELECT id, subid
FROM fact
WHERE (id, subid) IN dim;
```

`table_set` can also be used for trivial key-only subqueries such as:

```sql
SELECT id, subid
FROM fact
WHERE (id, subid) IN (SELECT id, subid FROM dim);
```

## Using `table_join` {#table-join}

`table_join` is intended for direct key-value joins.

Example:

```sql
CREATE TABLE dim
(
    id UInt64,
    subid UInt64,
    value String,
    LOOKUP INDEX idx_join (id, subid) TYPE table_join
)
ENGINE = MergeTree
ORDER BY (id, subid);

SELECT f.id, f.subid, d.value
FROM fact AS f
LEFT ANY JOIN dim AS d USING (id, subid)
SETTINGS join_algorithm = 'direct';
```

`table_join` is only used for direct-compatible equality joins.
It is most useful when the right-hand side is reused across many queries, because the lookup structure is cached after the first use.

## Limitations {#limitations}

- Lookup index keys must be plain columns, not arbitrary expressions.
- `table_join` requires non-nullable key columns.
- `table_join` is only used for direct-compatible equality joins.
- Lookup indexes are not data skipping indexes. They do not use `GRANULARITY`, do not materialize per-granule index files, and are not shown in `system.data_skipping_indices`.

## Replication {#replication}

Lookup index metadata is replicated together with `ReplicatedMergeTree` table metadata.
`ALTER TABLE ... ADD LOOKUP INDEX` and `ALTER TABLE ... DROP LOOKUP INDEX` are replicated in the same way as other metadata-only `ALTER` queries.
