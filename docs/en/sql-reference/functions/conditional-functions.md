---
slug: /en/sql-reference/functions/conditional-functions
sidebar_position: 40
sidebar_label: Conditional
---

# Conditional Functions

## if

Performs conditional branching.

If the condition `cond` evaluates to a non-zero value, the function returns the result of the expression `then`. If `cond` evaluates to zero or `NULL`, then the result of the `else` expression is returned.

Setting [short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) controls whether short-circuit evaluation is used. If enabled, the `then` expression is evaluated only on rows where `cond` is `true` and the `else` expression where `cond` is `false`. For example, with short-circuit evaluation, no division-by-zero exception is thrown when executing the query `SELECT if(number = 0, 0, intDiv(42, number)) FROM numbers(10)`.

`then` and `else` must be of a similar type.

**Syntax**

``` sql
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

``` sql
SELECT if(1, plus(2, 2), plus(2, 6));
```

Result:

``` text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

## multiIf

Allows to write the [CASE](../../sql-reference/operators/index.md#conditional-expression) operator more compactly in the query.

**Syntax**

``` sql
multiIf(cond_1, then_1, cond_2, then_2, ..., else)
```

Setting [short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) controls whether short-circuit evaluation is used. If enabled, the `then_i` expression is evaluated only on rows where `((NOT cond_1) AND (NOT cond_2) AND ... AND (NOT cond_{i-1}) AND cond_i)` is `true`, `cond_i` will be evaluated only on rows where `((NOT cond_1) AND (NOT cond_2) AND ... AND (NOT cond_{i-1}))` is `true`. For example, with short-circuit evaluation, no division-by-zero exception is thrown when executing the query `SELECT multiIf(number = 2, intDiv(1, number), number = 5) FROM numbers(10)`.

**Arguments**

The function accepts `2N+1` parameters:
- `cond_N` — The N-th evaluated condition which controls if `then_N` is returned.
- `then_N` — The result of the function when `cond_N` is true.
- `else` — The result of the function if none of conditions is true.

**Returned values**

The result of either any of the `then_N` or `else` expressions, depending on the conditions `cond_N`.

**Example**

Assuming this table:

``` text
┌─left─┬─right─┐
│ ᴺᵁᴸᴸ │     4 │
│    1 │     3 │
│    2 │     2 │
│    3 │     1 │
│    4 │  ᴺᵁᴸᴸ │
└──────┴───────┘
```

``` sql
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

## Using Conditional Results Directly

Conditionals always result to `0`, `1` or `NULL`. So you can use conditional results directly like this:

``` sql
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

## NULL Values in Conditionals

When `NULL` values are involved in conditionals, the result will also be `NULL`.

``` sql
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

``` sql
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

## greatest

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
The type returned is a DateTime64 as the DataTime32 must be promoted to 64 bit for the comparison.
:::

## least

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
The type returned is a DateTime64 as the DataTime32 must be promoted to 64 bit for the comparison.
:::

## clamp

Constrain the return value between A and B.

**Syntax**

``` sql
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
