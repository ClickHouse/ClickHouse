---
description: 'Documentation for Manipulating Constraints'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: 'Manipulating Constraints'
doc_type: 'reference'
---

# Manipulating Constraints

Constraints could be added or deleted using following syntax:

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name CHECK expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

See more on [constraints](../../../sql-reference/statements/create/table.md#constraints).

Queries will add or remove metadata about constraints from table, so they are processed immediately.

:::tip
Constraint check **will not be executed** on existing data if it was added.
:::

All changes on replicated tables are broadcast to ZooKeeper and will be applied on other replicas as well.

## UNIQUE Constraints

UNIQUE constraints enforce that no two rows share the same value(s) for the specified column(s). They are supported on MergeTree family tables.

### Syntax

```sql
-- At CREATE TABLE time:
CREATE TABLE [db].name
(
    ...
    CONSTRAINT constraint_name UNIQUE (column1 [, column2, ...])
)
ENGINE = MergeTree
ORDER BY ...;

-- Via ALTER:
ALTER TABLE [db].name ADD CONSTRAINT constraint_name UNIQUE (column1 [, column2, ...]);
ALTER TABLE [db].name DROP CONSTRAINT [IF EXISTS] constraint_name;
```

### How It Works

- An in-memory hash set (using 128-bit SipHash) tracks all existing key combinations.
- On each `INSERT`, new rows are checked against this set. If a duplicate is found, the statement is rejected with a `DUPLICATE_KEY` error.
- The hash set is **not persistent** — it is reconstructed from table data on server restart.
- INSERTs are serialized per constraint using a mutex to prevent race conditions.

### INSERT IGNORE

To silently skip duplicate rows instead of raising an error, use `INSERT IGNORE`:

```sql
INSERT IGNORE INTO table_name VALUES (...);
```

When `INSERT IGNORE` is used:
- Rows with duplicate keys are silently filtered out.
- The first occurrence wins (within a single batch, the first row with a given key is kept).
- No error is raised.

### Limitations

- Currently supported for non-replicated MergeTree tables only. ReplicatedMergeTree support is planned.
- The in-memory hash set is rebuilt on restart, which may take time for very large tables.
- There is a small probability of hash collision (128-bit SipHash), though this is negligible in practice.

