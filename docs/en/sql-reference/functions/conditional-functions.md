---
toc_priority: 43
toc_title: 'Conditional '
---

# Conditional Functions {#conditional-functions}

## if {#if}

Controls conditional branching. Unlike most systems, ClickHouse always evaluate both expressions `then` and `else`.

**Syntax**

``` sql
SELECT if(cond, then, else)
```

If the condition `cond` evaluates to a non-zero value, returns the result of the expression `then`, and the result of the expression `else`, if present, is skipped. If the `cond` is zero or `NULL`, then the result of the `then` expression is skipped and the result of the `else` expression, if present, is returned.

**Parameters**

-   `cond` – The condition for evaluation that can be zero or not. The type is UInt8, Nullable(UInt8) or NULL.
-   `then` - The expression to return if condition is met.
-   `else` - The expression to return if condition is not met.

**Returned values**

The function executes `then` and `else` expressions and returns its result, depending on whether the condition `cond` ended up being zero or not.

**Example**

Query:

``` sql
SELECT if(1, plus(2, 2), plus(2, 6))
```

Result:

``` text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

Query:

``` sql
SELECT if(0, plus(2, 2), plus(2, 6))
```

Result:

``` text
┌─plus(2, 6)─┐
│          8 │
└────────────┘
```

-   `then` and `else` must have the lowest common type.

**Example:**

Take this `LEFT_RIGHT` table:

``` sql
SELECT *
FROM LEFT_RIGHT

┌─left─┬─right─┐
│ ᴺᵁᴸᴸ │     4 │
│    1 │     3 │
│    2 │     2 │
│    3 │     1 │
│    4 │  ᴺᵁᴸᴸ │
└──────┴───────┘
```

The following query compares `left` and `right` values:

``` sql
SELECT
    left,
    right,
    if(left < right, 'left is smaller than right', 'right is greater or equal than left') AS is_smaller
FROM LEFT_RIGHT
WHERE isNotNull(left) AND isNotNull(right)

┌─left─┬─right─┬─is_smaller──────────────────────────┐
│    1 │     3 │ left is smaller than right          │
│    2 │     2 │ right is greater or equal than left │
│    3 │     1 │ right is greater or equal than left │
└──────┴───────┴─────────────────────────────────────┘
```

Note: `NULL` values are not used in this example, check [NULL values in conditionals](#null-values-in-conditionals) section.

## Ternary Operator {#ternary-operator}

It works same as `if` function.

Syntax: `cond ? then : else`

Returns `then` if the `cond` evaluates to be true (greater than zero), otherwise returns `else`.

-   `cond` must be of type of `UInt8`, and `then` and `else` must have the lowest common type.

-   `then` and `else` can be `NULL`

**See also**

-   [ifNotFinite](other-functions.md#ifnotfinite).

## multiIf {#multiif}

Allows you to write the [CASE](../operators/index.md#operator_case) operator more compactly in the query.

Syntax: `multiIf(cond_1, then_1, cond_2, then_2, ..., else)`

**Parameters:**

-   `cond_N` — The condition for the function to return `then_N`.
-   `then_N` — The result of the function when executed.
-   `else` — The result of the function if none of the conditions is met.

The function accepts `2N+1` parameters.

**Returned values**

The function returns one of the values `then_N` or `else`, depending on the conditions `cond_N`.

**Example**

Again using `LEFT_RIGHT` table.

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

## Using Conditional Results Directly {#using-conditional-results-directly}

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

## NULL Values in Conditionals {#null-values-in-conditionals}

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

[Original article](https://clickhouse.tech/docs/en/query_language/functions/conditional_functions/) <!--hide-->
