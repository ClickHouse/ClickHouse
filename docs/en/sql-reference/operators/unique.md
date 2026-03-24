---
description: 'Documentation for the `UNIQUE` operator'
slug: /sql-reference/operators/unique
title: 'UNIQUE'
doc_type: 'reference'
---

# UNIQUE

The `UNIQUE` predicate tests whether a subquery result contains no duplicate rows. It returns `1` if all rows are distinct, and `0` if any duplicates exist. An empty subquery is considered vacuously unique and returns `1`.

:::note
This is an experimental feature. To use it, enable the setting `allow_experimental_unique_predicate`.
:::

**Syntax**

```sql
UNIQUE(subquery)
```

**Arguments**

- `subquery` — A subquery whose rows are checked for uniqueness.

**Returned value**

- `1` if all rows in the subquery are distinct.
- `0` if any duplicate rows exist.

Type: [UInt8](/sql-reference/data-types/int-uint.md).

**NULL handling**

Per the SQL standard, rows containing `NULL` in any column are never considered duplicates. Two rows are duplicates only when every corresponding column value is non-null and equal. This means a subquery returning only `NULL` rows is considered unique.

**Implementation details**

Internally, `UNIQUE(subquery)` is rewritten to `SELECT __hasNoDuplicates(*) FROM (subquery)`. The `__hasNoDuplicates` aggregate function maintains a hash set and can terminate early — as soon as the first duplicate is found, it stops reading further data from the subquery.

Correlated subqueries are not currently supported with `UNIQUE`.

**Example**

All distinct rows:

```sql
SET allow_experimental_unique_predicate = 1;

SELECT UNIQUE(SELECT number FROM numbers(5));
```

```text
┌─result─┐
│      1 │
└────────┘
```

Duplicate rows present:

```sql
SELECT UNIQUE(SELECT number % 3 FROM numbers(6));
```

```text
┌─result─┐
│      0 │
└────────┘
```

`UNIQUE` can also be used in a [WHERE](../../sql-reference/statements/select/where.md) clause:

```sql
SELECT 'has unique rows' WHERE UNIQUE(SELECT number FROM numbers(3));
```

```text
┌─'has unique rows'─┐
│ has unique rows    │
└────────────────────┘
```
