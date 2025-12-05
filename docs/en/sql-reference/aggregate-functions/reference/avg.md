---
description: 'Calculates the arithmetic mean.'
sidebar_position: 112
slug: /sql-reference/aggregate-functions/reference/avg
title: 'avg'
doc_type: 'reference'
---

# avg

Calculates the arithmetic mean.

**Syntax**

```sql
avg(x)
```

**Arguments**

- `x` — input values, must be [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), [Decimal](../../../sql-reference/data-types/decimal.md), [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md), [DateTime](../../../sql-reference/data-types/datetime.md), or [DateTime64](../../../sql-reference/data-types/datetime64.md).

**Returned value**

- For numeric types: the arithmetic mean, always as [Float64](../../../sql-reference/data-types/float.md).
- For Date/DateTime types: the arithmetic mean as [Nullable](../../../sql-reference/data-types/nullable.md) of the same type as the input (preserves type and timezone/scale).
- `NaN` if the input parameter `x` is empty for numeric types.
- `NULL` if the input parameter `x` is empty for Date/DateTime types.

**Example**

Query:

```sql
SELECT avg(x) FROM VALUES('x Int8', 0, 1, 2, 3, 4, 5);
```

Result:

```text
┌─avg(x)─┐
│    2.5 │
└────────┘
```

**Example with Date/DateTime**

Query:

```sql
SELECT avg(d) FROM VALUES('d Date', '2024-01-01', '2024-01-03');
```

Result:

```text
┌─avg(d)─────┐
│ 2024-01-02 │
└────────────┘
```

**Example with empty result**

Create a temp table:

Query:

```sql
CREATE TABLE test (t UInt8) ENGINE = Memory;
```

Get the arithmetic mean:

Query:

```sql
SELECT avg(t) FROM test;
```

Result:

```text
┌─avg(t)─┐
│    nan │
└────────┘
```

**Example with empty Date result**

Query:

```sql
SELECT avg(d) FROM (SELECT toDate('2024-01-01') AS d WHERE 0);
```

Result:

```text
┌─avg(d)─┐
│   ᴺᵁᴸᴸ │
└────────┘
```
