---
slug: /en/sql-reference/aggregate-functions/reference/first_value
sidebar_position: 7
---

# first_value

Selects the first encountered value, similar to `any`, but could accept NULL.
Mostly it should be used with [Window Functions](../../window-functions/index.md).
Without Window Functions the result will be random if the source stream is not ordered.

## examples

```sql
CREATE TABLE test_data
(
    a Int64,
    b Nullable(Int64)
)
ENGINE = Memory;

INSERT INTO test_data (a, b) Values (1,null), (2,3), (4, 5), (6,null);
```

### example1
The NULL value is ignored at default.
```sql
select first_value(b) from test_data;
```

```text
┌─first_value_ignore_nulls(b)─┐
│                           3 │
└─────────────────────────────┘
```

### example2
The NULL value is ignored.
```sql
select first_value(b) ignore nulls from test_data
```

```text
┌─first_value_ignore_nulls(b)─┐
│                           3 │
└─────────────────────────────┘
```

### example3
The NULL value is accepted.
```sql
select first_value(b) respect nulls from test_data
```

```text
┌─first_value_respect_nulls(b)─┐
│                         ᴺᵁᴸᴸ │
└──────────────────────────────┘
```

### example4
Stabilized result using the sub-query with `ORDER BY`.
```sql
SELECT
    first_value_respect_nulls(b),
    first_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─first_value_respect_nulls(b)─┬─first_value(b)─┐
│                         ᴺᵁᴸᴸ │              3 │
└──────────────────────────────┴────────────────┘
```

