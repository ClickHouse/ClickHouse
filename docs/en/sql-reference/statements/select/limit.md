---
description: 'Documentation for LIMIT Clause'
sidebar_label: 'LIMIT'
slug: /sql-reference/statements/select/limit
title: 'LIMIT Clause'
doc_type: 'reference'
---

# LIMIT clause

The `LIMIT` clause controls how many rows are returned from your query results.

## Basic syntax {#basic-syntax}

**Select first rows:**

```sql
LIMIT m
```

Returns the first `m` rows from the result, or all records when there are fewer than `m`.

**Alternative TOP syntax (MS SQL Server compatible):**

```sql
-- SELECT TOP number|percent column_name(s) FROM table_name
SELECT TOP 10 * FROM numbers(100);
SELECT TOP 0.1 * FROM numbers(100);
```

This is equivalent to `LIMIT m` and can be used for compatibility with Microsoft SQL Server queries.

**Select with offset:**

```sql
LIMIT m OFFSET n
-- or equivalently:
LIMIT n, m
```

Skips the first `n` rows, then returns the next `m` rows.

In both forms, `n` and `m` must be non-negative integers.

## Negative limits {#negative-limits}

Select rows from the *end* of the result set using negative values:

| Syntax | Result |
|--------|--------|
| `LIMIT -m` | Last `m` rows |
| `LIMIT -m OFFSET -n` | Last `m` rows after skipping the last `n` rows |
| `LIMIT m OFFSET -n` | First `m` rows after skipping the last `n` rows |
| `LIMIT -m OFFSET n` | Last `m` rows after skipping the first `n` rows |

The `LIMIT -n, -m` syntax is equivalent to `LIMIT -m OFFSET -n`.

## Fractional limits {#fractional-limits}

Use decimal values between 0 and 1 to select a percentage of rows:

| Syntax | Result |
|--------|--------|
| `LIMIT 0.1` | First 10% of rows |
| `LIMIT 1 OFFSET 0.5` | The median row |
| `LIMIT 0.25 OFFSET 0.5` | Third quartile (25% of rows after skipping the first 50%) |

:::note
- Fractions must be [Float64](../../data-types/float.md) values greater than 0 and less than 1.
- Fractional row counts are rounded to the next whole number.
:::

## Combining limit types {#combining-limit-types}

You can mix standard integers with fractional or negative offsets:

```sql
LIMIT 10 OFFSET 0.5    -- 10 rows starting from the halfway point
LIMIT 10 OFFSET -20    -- 10 rows after skipping the last 20
```

## LIMIT ... WITH TIES {#limit--with-ties-modifier}

The `WITH TIES` modifier includes additional rows that have the same `ORDER BY` values as the last row in your limit.

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

With `WITH TIES`, all rows matching the last value are included:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5 WITH TIES
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

Row 6 is included because it has the same value (`2`) as row 5.

:::note
`WITH TIES` is not supported with negative limits.
:::

This modifier can be combined with the [`ORDER BY ... WITH FILL`](/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier) modifier.

## LIMIT ... AFTER ... UNTIL (range by conditions) {#limit-after-until}

You can limit the result to a *range* of rows between two boundary conditions:

```sql
LIMIT [n] AFTER start_expr [UNTIL end_expr]
LIMIT [n] UNTIL end_expr
```

- `AFTER start_expr`: Start output from the first row where `start_expr` is true (that row is included).
- `UNTIL end_expr`: Stop before the first row where `end_expr` is true (that row is excluded).
- `n`: Maximum number of rows to return (optional; when present, caps the range).

Stream order (the order rows are read) defines “first” match; use `ORDER BY` to control it.

If the first `UNTIL` match appears before the first `AFTER` match, the result is empty.

**Examples:**

```sql
-- First 3 rows starting from the first row where number >= 3
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 3;

-- Rows from first row where number >= 2 until (exclusive) first row where number >= 6
SELECT number FROM numbers(10) ORDER BY number LIMIT 10 AFTER number >= 2 UNTIL number >= 6;
```

:::note
- `WITH TIES`, fractional/negative `LIMIT`/`OFFSET`, and `OFFSET` are not supported together with `AFTER`/`UNTIL`.
- Preliminary `LIMIT` pushdown is disabled when `AFTER`/`UNTIL` is used.
:::

## Considerations {#considerations}

**Non-deterministic results:** Without an [`ORDER BY`](../../../sql-reference/statements/select/order-by.md) clause, the rows returned may be arbitrary and vary between query executions.

**Server-side limit:** The number of rows returned can also be affected by the [limit](../../../operations/settings/settings.md#limit) setting.

## See also {#see-also}

- [LIMIT BY](/sql-reference/statements/select/limit-by) — Limits rows per group of values, useful for getting top N results within each category.
