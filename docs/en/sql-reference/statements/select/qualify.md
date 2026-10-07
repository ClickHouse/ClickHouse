---
description: 'Documentation for QUALIFY Clause'
sidebar_label: 'QUALIFY'
slug: /sql-reference/statements/select/qualify
title: 'QUALIFY Clause'
doc_type: 'reference'
---

Allows filtering window functions results. It is similar to the [WHERE](../../../sql-reference/statements/select/where.md) clause, but the difference is that `WHERE` is performed before window functions evaluation, while `QUALIFY` is performed after it.

It is possible to reference window functions results from `SELECT` clause in `QUALIFY` clause by their alias. Alternatively, `QUALIFY` clause can filter on results of additional window functions that are not returned in query results.

## Limitations {#limitations}

`QUALIFY` can't be used if there are no window functions to evaluate. Use `WHERE` instead.

## Examples {#examples}

Example:

```sql
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```

```text
┌─number─┬─partition_count─┐
│      0 │               4 │
│      3 │               4 │
│      6 │               4 │
│      9 │               4 │
└────────┴─────────────────┘
```
