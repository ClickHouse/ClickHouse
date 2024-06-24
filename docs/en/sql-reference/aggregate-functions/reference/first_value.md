---
slug: /en/sql-reference/aggregate-functions/reference/first_value
sidebar_position: 137
---

# first_value

It is an alias for [`any`](../../../sql-reference/aggregate-functions/reference/any.md) but it was introduced for compatibility with [Window Functions](../../window-functions/index.md), where sometimes it's necessary to process `NULL` values (by default all ClickHouse aggregate functions ignore NULL values).

It supports declaring a modifier to respect nulls (`RESPECT NULLS`), both under [Window Functions](../../window-functions/index.md) and in normal aggregations.

As with `any`, without Window Functions the result will be random if the source stream is not ordered and the return type
matches the input type (Null is only returned if the input is Nullable or -OrNull combinator is added).

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
By default, the NULL value is ignored.
```sql
select first_value(b) from test_data;
```

```text
┌─any(b)─┐
│      3 │
└────────┘
```

### example2
The NULL value is ignored.
```sql
select first_value(b) ignore nulls from test_data
```

```text
┌─any(b) IGNORE NULLS ─┐
│                    3 │
└──────────────────────┘
```

### example3
The NULL value is accepted.
```sql
select first_value(b) respect nulls from test_data
```

```text
┌─any(b) RESPECT NULLS ─┐
│                  ᴺᵁᴸᴸ │
└───────────────────────┘
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
┌─any_respect_nulls(b)─┬─any(b)─┐
│                 ᴺᵁᴸᴸ │      3 │
└──────────────────────┴────────┘
```

