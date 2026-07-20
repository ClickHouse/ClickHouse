---
description: 'Documentation for DISTINCT Clause'
sidebar_label: 'DISTINCT'
slug: /sql-reference/statements/select/distinct
title: 'DISTINCT Clause'
doc_type: 'reference'
---

If `SELECT DISTINCT` is specified, only unique rows will remain in a query result. Thus, only a single row will remain out of all the sets of fully matching rows in the result.

You can specify the list of columns that must have unique values: `SELECT DISTINCT ON (column1, column2,...)`. If the columns are not specified, all of them are taken into consideration.

Consider the table:

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Using `DISTINCT` without specifying columns:

```sql
SELECT DISTINCT * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Using `DISTINCT` with specified columns:

```sql
SELECT DISTINCT ON (a,b) * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

## DISTINCT and ORDER BY {#distinct-and-order-by}

ClickHouse supports using the `DISTINCT` and `ORDER BY` clauses for different columns in one query. The `DISTINCT` clause is executed before the `ORDER BY` clause.

Consider the table:

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

Selecting data:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```
Selecting data with the different sorting direction:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

Row `2, 4` was cut before sorting.

Take this implementation specificity into account when programming queries.

## Null Processing {#null-processing}

`DISTINCT` works with [NULL](/sql-reference/syntax#null) as if `NULL` were a specific value, and `NULL==NULL`. In other words, in the `DISTINCT` results, different combinations with `NULL` occur only once. It differs from `NULL` processing in most other contexts.

## DISTINCT in External Memory {#distinct-in-external-memory}

`DISTINCT` can spill temporary data to the disk to restrict its memory usage, and by default it is allowed to do so once the memory usage of the query exceeds half of the available memory: the threshold is controlled by the [max_bytes_ratio_before_external_distinct](/operations/settings/settings#max_bytes_ratio_before_external_distinct) setting (`0.5` by default) as a ratio of the available memory. Additionally, the [max_bytes_before_external_distinct](/operations/settings/settings#max_bytes_before_external_distinct) setting can specify the threshold as an absolute amount of bytes (unset by default); if both settings are set, the smaller resulting threshold is used. To disable spilling completely, set both settings to `0`.

When the threshold is exceeded, the distinct rows collected so far are sorted and written into a temporary file, and the rest of the data is processed the same way. After all data is read, the sorted files are merged and the remaining distinct rows are output. Rows stop streaming to the client as soon as the first spill happens: the remaining distinct rows are returned only after the merge. If a `LIMIT` is reached before the memory threshold, no spilling happens and the query still finishes early.

`DISTINCT` in external memory requires all the `DISTINCT` columns to be comparable; for the few types that support only equality checks (e.g. `AggregateFunction`), `DISTINCT` is processed in memory. Values that are different in the binary representation but compare equal — such as `0.` and `-0.`, or `NaN` values with different payloads — are normally distinct values for `DISTINCT`, but once the data is spilled they may be deduplicated as a single value, same as for `DISTINCT` over sorted data (the `optimize_distinct_in_order` optimization).

## Alternatives {#alternatives}

It is possible to obtain the same result by applying [GROUP BY](/sql-reference/statements/select/group-by) across the same set of values as specified as `SELECT` clause, without using any aggregate functions. But there are few differences from `GROUP BY` approach:

- `DISTINCT` can be applied together with `GROUP BY`.
- When [ORDER BY](../../../sql-reference/statements/select/order-by.md) is omitted and [LIMIT](../../../sql-reference/statements/select/limit.md) is defined, the query stops running immediately after the required number of different rows has been read.
- Data blocks are output as they are processed, without waiting for the entire query to finish running.
