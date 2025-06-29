---
description: 'Selects the last encountered value, similar to `anyLast`, but could
  accept NULL.'
sidebar_position: 160
slug: /sql-reference/aggregate-functions/reference/last_value
title: 'last_value'
---

# last_value

Selects the last encountered value, similar to `anyLast`, but could accept NULL.
Mostly it should be used with [Window Functions](../../window-functions/index.md).
Without Window Functions the result will be random if the source stream is not ordered.

## examples {#examples}

```sql
CREATE TABLE test_data
(
    a Int64,
    b Nullable(Int64)
)
ENGINE = Memory;

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null)
```

### example1 {#example1}
The NULL value is ignored at default.
```sql
SELECT last_value(b) FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

### example2 {#example2}
The NULL value is ignored.
```sql
SELECT last_value(b) ignore nulls FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

### example3 {#example3}
The NULL value is accepted.
```sql
SELECT last_value(b) respect nulls FROM test_data
```

```text
┌─last_value_respect_nulls(b)─┐
│                        ᴺᵁᴸᴸ │
└─────────────────────────────┘
```

### example4 {#example4}
Stabilized result using the sub-query with `ORDER BY`.
```sql
SELECT
    last_value_respect_nulls(b),
    last_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─last_value_respect_nulls(b)─┬─last_value(b)─┐
│                        ᴺᵁᴸᴸ │             5 │
└─────────────────────────────┴───────────────┘
```


