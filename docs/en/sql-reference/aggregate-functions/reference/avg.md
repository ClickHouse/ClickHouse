---
description: 'Calculates the arithmetic mean.'
sidebar_position: 112
slug: /sql-reference/aggregate-functions/reference/avg
title: 'avg'
---

# avg

Calculates the arithmetic mean.

**Syntax**

```sql
avg(x)
```

**Arguments**

- `x` — input values, must be [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), or [Decimal](../../../sql-reference/data-types/decimal.md).

**Returned value**

- The arithmetic mean, always as [Float64](../../../sql-reference/data-types/float.md).
- `NaN` if the input parameter `x` is empty.

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

**Example**

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
┌─avg(x)─┐
│    nan │
└────────┘
```
