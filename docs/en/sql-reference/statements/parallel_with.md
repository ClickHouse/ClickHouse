---
description: 'Documentation for PARALLEL WITH Clause'
sidebar_label: 'PARALLEL WITH'
sidebar_position: 53
slug: /sql-reference/statements/parallel_with
title: 'PARALLEL WITH Clause'
---

# PARALLEL WITH Clause

Allows to execute multiple statements in parallel.

## Syntax {#syntax}

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

Executes statements `statement1`, `statement2`, `statement3`, ... in parallel with each other. The output of those statements is discarded.

Executing statements in parallel may be faster than just a sequence of the same statements in many cases. For example, `statement1 PARALLEL WITH statement2 PARALLEL WITH statement3` is likely to be faster than `statement1; statement2; statement3`.

## Examples {#examples}

Creates two tables in parallel:

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

Drops two tables in parallel:

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

## Settings {#settings}

Setting [max_threads](../../operations/settings/settings.md#max_threads) controls how many threads are spawned.

## Comparison with UNION {#comparison-with-union}

The `PARALLEL WITH` clause is a bit similar to [UNION](select/union.md), which also executes its operands in parallel. However there are some differences:
- `PARALLEL WITH` doesn't return any results from executing its operands, it can only rethrow an exception from them if any;
- `PARALLEL WITH` doesn't require its operands to have the same set of result columns;
- `PARALLEL WITH` can execute any statements (not just `SELECT`).
