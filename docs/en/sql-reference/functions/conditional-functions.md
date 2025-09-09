---
description: 'Documentation for Conditional Functions'
sidebar_label: 'Conditional'
sidebar_position: 40
slug: /sql-reference/functions/conditional-functions
title: 'Conditional Functions'
---

# Conditional Functions

## Overview {#overview}

### Using Conditional Results Directly {#using-conditional-results-directly}

Conditionals always result to `0`, `1` or `NULL`. So you can use conditional results directly like this:

```sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

### NULL Values in Conditionals {#null-values-in-conditionals}

When `NULL` values are involved in conditionals, the result will also be `NULL`.

```sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

So you should construct your queries carefully if the types are `Nullable`.

The following example demonstrates this by failing to add equals condition to `multiIf`.

```sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

### CASE statement {#case-statement}

The CASE expression in ClickHouse provides conditional logic similar to the SQL CASE operator. It evaluates conditions and returns values based on the first matching condition.

ClickHouse supports two forms of CASE:

1. `CASE WHEN ... THEN ... ELSE ... END`
   <br/>
   This form allows full flexibility and is internally implemented using the [multiIf](/sql-reference/functions/conditional-functions#multiif) function. Each condition is evaluated independently, and expressions can include non-constant values.

```sql
SELECT
    number,
    CASE
        WHEN number % 2 = 0 THEN number + 1
        WHEN number % 2 = 1 THEN number * 10
        ELSE number
    END AS result
FROM system.numbers
WHERE number < 5;

-- is translated to
SELECT
    number,
    multiIf((number % 2) = 0, number + 1, (number % 2) = 1, number * 10, number) AS result
FROM system.numbers
WHERE number < 5

┌─number─┬─result─┐
│      0 │      1 │
│      1 │     10 │
│      2 │      3 │
│      3 │     30 │
│      4 │      5 │
└────────┴────────┘

5 rows in set. Elapsed: 0.002 sec.
```

2. `CASE <expr> WHEN <val1> THEN ... WHEN <val2> THEN ... ELSE ... END`
   <br/>
   This more compact form is optimized for constant value matching and internally uses `caseWithExpression()`.


For example, the following is valid:

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN 100
        WHEN 1 THEN 200
        ELSE 0
    END AS result
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, 100, 1, 200, 0) AS result
FROM system.numbers
WHERE number < 3

┌─number─┬─result─┐
│      0 │    100 │
│      1 │    200 │
│      2 │      0 │
└────────┴────────┘

3 rows in set. Elapsed: 0.002 sec.
```

This form also does not require return expressions to be constants.

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN number + 1
        WHEN 1 THEN number * 10
        ELSE number
    END
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, number + 1, 1, number * 10, number)
FROM system.numbers
WHERE number < 3

┌─number─┬─caseWithExpr⋯0), number)─┐
│      0 │                        1 │
│      1 │                       10 │
│      2 │                        2 │
└────────┴──────────────────────────┘

3 rows in set. Elapsed: 0.001 sec.
```

#### Caveats  {#caveats}

ClickHouse determines the result type of a CASE expression (or its internal equivalent, such as `multiIf`) before evaluating any conditions. This is important when the return expressions differ in type, such as different timezones or numeric types.

- The result type is selected based on the largest compatible type among all branches.
- Once this type is selected, all other branches are implicitly cast to it - even if their logic would never be executed at runtime.
- For types like DateTime64, where the timezone is part of the type signature, this can lead to surprising behavior: the first encountered timezone may be used for all branches, even when other branches specify different timezones.

For example, below all rows return the timestamp in the timezone of the first matched branch i.e. `Asia/Kolkata`

```sql
SELECT
    number,
    CASE
        WHEN number = 0 THEN fromUnixTimestamp64Milli(0, 'Asia/Kolkata')
        WHEN number = 1 THEN fromUnixTimestamp64Milli(0, 'America/Los_Angeles')
        ELSE fromUnixTimestamp64Milli(0, 'UTC')
    END AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, fromUnixTimestamp64Milli(0, 'Asia/Kolkata'), number = 1, fromUnixTimestamp64Milli(0, 'America/Los_Angeles'), fromUnixTimestamp64Milli(0, 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬──────────────────────tz─┐
│      0 │ 1970-01-01 05:30:00.000 │
│      1 │ 1970-01-01 05:30:00.000 │
│      2 │ 1970-01-01 05:30:00.000 │
└────────┴─────────────────────────┘

3 rows in set. Elapsed: 0.011 sec.
```

Here, ClickHouse sees multiple `DateTime64(3, <timezone>)` return types. It infers the common type as `DateTime64(3, 'Asia/Kolkata'` as the first one it sees, implicitly casting other branches to this type.

This can be addressed by converting to a string to preserve intended timezone formatting:

```sql
SELECT
    number,
    multiIf(
        number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'),
        number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'),
        formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')
    ) AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'), number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'), formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬─tz──────────────────┐
│      0 │ 1970-01-01 05:30:00 │
│      1 │ 1969-12-31 16:00:00 │
│      2 │ 1970-01-01 00:00:00 │
└────────┴─────────────────────┘

3 rows in set. Elapsed: 0.002 sec.
```

## if {#if}

Performs conditional branching.

If the condition `cond` evaluates to a non-zero value, the function returns the result of the expression `then`. If `cond` evaluates to zero or `NULL`, then the result of the `else` expression is returned.

Setting [short_circuit_function_evaluation](/operations/settings/settings#short_circuit_function_evaluation) controls whether short-circuit evaluation is used. If enabled, the `then` expression is evaluated only on rows where `cond` is `true` and the `else` expression where `cond` is `false`. For example, with short-circuit evaluation, no division-by-zero exception is thrown when executing the query `SELECT if(number = 0, 0, intDiv(42, number)) FROM numbers(10)`.

`then` and `else` must be of a similar type.

**Syntax**

```sql
if(cond, then, else)
```
Alias: `cond ? then : else` (ternary operator)

**Arguments**

- `cond` – The evaluated condition. UInt8, Nullable(UInt8) or NULL.
- `then` – The expression returned if `condition` is true.
- `else` – The expression returned if `condition` is `false` or NULL.

**Returned values**

The result of either the `then` and `else` expressions, depending on condition `cond`.

**Example**

```sql
SELECT if(1, plus(2, 2), plus(2, 6));
```

Result:

```text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

## multiIf {#multiif}

Allows to write the [CASE](../../sql-reference/operators/index.md#conditional-expression) operator more compactly in the query.

**Syntax**

```sql
multiIf(cond_1, then_1, cond_2, then_2, ..., else)
```

Setting [short_circuit_function_evaluation](/operations/settings/settings#short_circuit_function_evaluation) controls whether short-circuit evaluation is used. If enabled, the `then_i` expression is evaluated only on rows where `((NOT cond_1) AND (NOT cond_2) AND ... AND (NOT cond_{i-1}) AND cond_i)` is `true`, `cond_i` will be evaluated only on rows where `((NOT cond_1) AND (NOT cond_2) AND ... AND (NOT cond_{i-1}))` is `true`. For example, with short-circuit evaluation, no division-by-zero exception is thrown when executing the query `SELECT multiIf(number = 2, intDiv(1, number), number = 5) FROM numbers(10)`.

**Arguments**

The function accepts `2N+1` parameters:
- `cond_N` — The N-th evaluated condition which controls if `then_N` is returned.
- `then_N` — The result of the function when `cond_N` is true.
- `else` — The result of the function if none of conditions is true.

**Returned values**

The result of either any of the `then_N` or `else` expressions, depending on the conditions `cond_N`.

**Example**

Assuming this table:

```text
┌─left─┬─right─┐
│ ᴺᵁᴸᴸ │     4 │
│    1 │     3 │
│    2 │     2 │
│    3 │     1 │
│    4 │  ᴺᵁᴸᴸ │
└──────┴───────┘
```

```sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'left is greater', left = right, 'Both equal', 'Null value') AS result
FROM LEFT_RIGHT

┌─left─┬─right─┬─result──────────┐
│ ᴺᵁᴸᴸ │     4 │ Null value      │
│    1 │     3 │ left is smaller │
│    2 │     2 │ Both equal      │
│    3 │     1 │ left is greater │
│    4 │  ᴺᵁᴸᴸ │ Null value      │
└──────┴───────┴─────────────────┘
```

## greatest {#greatest}

Returns the greatest across a list of values.  All of the list members must be of comparable types.

Examples:

```sql
SELECT greatest(1, 2, toUInt8(3), 3.) result,  toTypeName(result) type;
```
```response
┌─result─┬─type────┐
│      3 │ Float64 │
└────────┴─────────┘
```

:::note
The type returned is a Float64 as the UInt8 must be promoted to 64 bit for the comparison.
:::

```sql
SELECT greatest(['hello'], ['there'], ['world'])
```
```response
┌─greatest(['hello'], ['there'], ['world'])─┐
│ ['world']                                 │
└───────────────────────────────────────────┘
```

```sql
SELECT greatest(toDateTime32(now() + toIntervalDay(1)), toDateTime64(now(), 3))
```
```response
┌─greatest(toDateTime32(plus(now(), toIntervalDay(1))), toDateTime64(now(), 3))─┐
│                                                       2023-05-12 01:16:59.000 │
└──---──────────────────────────────────────────────────────────────────────────┘
```

:::note
The type returned is a DateTime64 as the DateTime32 must be promoted to 64 bit for the comparison.
:::

## least {#least}

Returns the least across a list of values.  All of the list members must be of comparable types.

Examples:

```sql
SELECT least(1, 2, toUInt8(3), 3.) result,  toTypeName(result) type;
```
```response
┌─result─┬─type────┐
│      1 │ Float64 │
└────────┴─────────┘
```

:::note
The type returned is a Float64 as the UInt8 must be promoted to 64 bit for the comparison.
:::

```sql
SELECT least(['hello'], ['there'], ['world'])
```
```response
┌─least(['hello'], ['there'], ['world'])─┐
│ ['hello']                              │
└────────────────────────────────────────┘
```

```sql
SELECT least(toDateTime32(now() + toIntervalDay(1)), toDateTime64(now(), 3))
```
```response
┌─least(toDateTime32(plus(now(), toIntervalDay(1))), toDateTime64(now(), 3))─┐
│                                                    2023-05-12 01:16:59.000 │
└────────────────────────────────────────────────────────────────────────────┘
```

:::note
The type returned is a DateTime64 as the DateTime32 must be promoted to 64 bit for the comparison.
:::

## clamp {#clamp}

Constrain the return value between A and B.

**Syntax**

```sql
clamp(value, min, max)
```

**Arguments**

- `value` – Input value.
- `min` – Limit the lower bound.
- `max` – Limit the upper bound.

**Returned values**

If the value is less than the minimum value, return the minimum value; if it is greater than the maximum value, return the maximum value; otherwise, return the current value.

Examples:

```sql
SELECT clamp(1, 2, 3) result,  toTypeName(result) type;
```
```response
┌─result─┬─type────┐
│      2 │ Float64 │
└────────┴─────────┘
```

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
