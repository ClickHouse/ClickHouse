---
description: 'Documentation for Regular Functions'
sidebar_label: 'Overview'
sidebar_position: 1
slug: /sql-reference/functions/overview
title: 'Regular Functions'
doc_type: 'reference'
---

There are at least\* two types of functions - regular functions (they are just called "functions") and aggregate functions. These are completely different concepts. Regular functions work as if they are applied to each row separately (for each row, the result of the function does not depend on the other rows). Aggregate functions accumulate a set of values from various rows (i.e. they depend on the entire set of rows).

In this section we discuss regular functions. For aggregate functions, see the section "Aggregate functions".

:::note 
There is a third type of function that the ['arrayJoin' function](../functions/array-join.md) belongs to. And [table functions](../table-functions/index.md) can also be mentioned separately.
:::

## Strong Typing {#strong-typing}

In contrast to standard SQL, ClickHouse has strong typing. In other words, it does not make implicit conversions between types. Each function works for a specific set of types. This means that sometimes you need to use type conversion functions.

## Common Subexpression Elimination {#common-subexpression-elimination}

All expressions in a query that have the same AST (the same record or same result of syntactic parsing) are considered to have identical values. Such expressions are concatenated and executed once. Identical subqueries are also eliminated this way.

## Types of Results {#types-of-results}

All functions return a single value as the result (not several values, and not zero values). The type of result is usually defined only by the types of arguments, not by the values. Exceptions are the tupleElement function (the a.N operator), and the toFixedString function.

## Constants {#constants}

For simplicity, certain functions can only work with constants for some arguments. For example, the right argument of the LIKE operator must be a constant.
Almost all functions return a constant for constant arguments. The exception is functions that generate random numbers.
The 'now' function returns different values for queries that were run at different times, but the result is considered a constant, since constancy is only important within a single query.
A constant expression is also considered a constant (for example, the right half of the LIKE operator can be constructed from multiple constants).

Functions can be implemented in different ways for constant and non-constant arguments (different code is executed). But the results for a constant and for a true column containing only the same value should match each other.

## NULL Processing {#null-processing}

Functions have the following behaviors:

- If at least one of the arguments of the function is `NULL`, the function result is also `NULL`.
- Special behavior that is specified individually in the description of each function. In the ClickHouse source code, these functions have `UseDefaultImplementationForNulls=false`.

## Constancy {#constancy}

Functions can't change the values of their arguments – any changes are returned as the result. Thus, the result of calculating separate functions does not depend on the order in which the functions are written in the query.

## Higher-order functions {#higher-order-functions}

### `->` operator and lambda(params, expr) functions {#arrow-operator-and-lambda}

Higher-order functions can only accept lambda functions as their functional argument. To pass a lambda function to a higher-order function use `->` operator. The left side of the arrow has a formal parameter, which is any ID, or multiple formal parameters – any IDs in a tuple. The right side of the arrow has an expression that can use these formal parameters, as well as any table columns.

Examples:

```python
x -> 2 * x
str -> str != Referer
```

A lambda function that accepts multiple arguments can also be passed to a higher-order function. In this case, the higher-order function is passed several arrays of identical length that these arguments will correspond to.

For some functions the first argument (the lambda function) can be omitted. In this case, identical mapping is assumed.

### Bare function names as lambdas {#bare-function-names-as-lambdas}

Instead of writing a full lambda expression, you can pass a function name directly to a higher-order function. The function name is automatically converted to an equivalent lambda expression.

For example, the following pairs are equivalent:

```sql
SELECT arrayMap(negate, [1, 2, 3]);            -- [-1, -2, -3]
SELECT arrayMap(x -> negate(x), [1, 2, 3]);    -- [-1, -2, -3]

SELECT arrayMap(plus, [1, 2, 3], [10, 20, 30]);            -- [11, 22, 33]
SELECT arrayMap((x, y) -> plus(x, y), [1, 2, 3], [10, 20, 30]); -- [11, 22, 33]

SELECT arrayFilter(isNotNull, [1, NULL, 3, NULL, 5]);            -- [1, 3, 5]
SELECT arrayFilter(x -> isNotNull(x), [1, NULL, 3, NULL, 5]);    -- [1, 3, 5]

SELECT arrayFold(plus, [1, 2, 3, 4, 5], toUInt64(0));                      -- 15
SELECT arrayFold((acc, x) -> plus(acc, x), [1, 2, 3, 4, 5], toUInt64(0));  -- 15
```

This works with built-in functions, SQL UDFs, executable UDFs, and WebAssembly UDFs. Column and alias names take priority over function names when there is ambiguity.

The lambda arity is taken from the inner function. For example, `arrayMap(plus, ...)` uses arity 2 because `plus` takes two arguments, so it also works with tuple inputs such as `arrayMap(plus, [(1, 10), (2, 20)])` where the tuple elements are unpacked into the lambda arguments.

For variadic inner functions (such as `concat`, which accepts any number of arguments), the lambda arity falls back to the number of array arguments. This is correct for higher-order functions like `arrayMap`, `arrayFilter`, and `arrayFold`. For higher-order functions that accept fixed non-array parameters in addition to arrays — for example, `arrayPartialSort(f, limit, arr)` — bare variadic function names may produce the wrong arity, in which case an explicit lambda is required.

Variadic inner functions also do not auto-unpack tuple inputs. For example, `arrayMap(concat, [('a', 'b'), ('c', 'd')])` rewrites to a unary lambda and is not equivalent to `arrayMap((x, y) -> concat(x, y), [('a', 'b'), ('c', 'd')])`. Use an explicit lambda when you want to destructure tuple elements into a variadic call.

## User Defined Functions (UDFs) {#user-defined-functions-udfs}

ClickHouse supports user-defined functions. See [UDFs](../functions/udf.md).
