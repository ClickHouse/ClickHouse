---
slug: /en/sql-reference/functions/logical-functions
sidebar_position: 110
sidebar_label: Logical
---

# Logical Functions

Below functions perform logical operations on arguments of arbitrary numeric types. They return either 0 or 1 as [UInt8](../data-types/int-uint.md) or in some cases `NULL`.

Zero as an argument is considered `false`, non-zero values are considered `true`.

## and

Calculates the logical conjunction of two or more values. 

Setting [short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) controls whether short-circuit evaluation is used. If enabled, `val_i` is evaluated only if `(val_1 AND val_2 AND ... AND val_{i-1})` is `true`. For example, with short-circuit evaluation, no division-by-zero exception is thrown when executing the query `SELECT and(number = 2, intDiv(1, number)) FROM numbers(5)`.

**Syntax**

``` sql
and(val1, val2...)
```

Alias: The [AND operator](../../sql-reference/operators/index.md#logical-and-operator).

**Arguments**

- `val1, val2, ...` — List of at least two values. [Int](../data-types/int-uint.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md) or [Nullable](../data-types/nullable.md).

**Returned value**

- `0`, if at least one argument evaluates to `false`,
- `NULL`, if no argument evaluates to `false` and at least one argument is `NULL`,
- `1`, otherwise.

Type: [UInt8](../../sql-reference/data-types/int-uint.md) or [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Example**

``` sql
SELECT and(0, 1, -2);
```

Result:

``` text
┌─and(0, 1, -2)─┐
│             0 │
└───────────────┘
```

With `NULL`:

``` sql
SELECT and(NULL, 1, 10, -2);
```

Result:

``` text
┌─and(NULL, 1, 10, -2)─┐
│                 ᴺᵁᴸᴸ │
└──────────────────────┘
```

## or

Calculates the logical disjunction of two or more values.

Setting [short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) controls whether short-circuit evaluation is used. If enabled, `val_i` is evaluated only if `((NOT val_1) AND (NOT val_2) AND ... AND (NOT val_{i-1}))` is `true`. For example, with short-circuit evaluation, no division-by-zero exception is thrown when executing the query `SELECT or(number = 0, intDiv(1, number) != 0) FROM numbers(5)`.

**Syntax**

``` sql
or(val1, val2...)
```

Alias: The [OR operator](../../sql-reference/operators/index.md#logical-or-operator).

**Arguments**

- `val1, val2, ...` — List of at least two values. [Int](../data-types/int-uint.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md) or [Nullable](../data-types/nullable.md).

**Returned value**

- `1`, if at least one argument evaluates to `true`,
- `0`, if all arguments evaluate to `false`,
- `NULL`, if all arguments evaluate to `false` and at least one argument is `NULL`.

Type: [UInt8](../../sql-reference/data-types/int-uint.md) or [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Example**

``` sql
SELECT or(1, 0, 0, 2, NULL);
```

Result:

``` text
┌─or(1, 0, 0, 2, NULL)─┐
│                    1 │
└──────────────────────┘
```

With `NULL`:

``` sql
SELECT or(0, NULL);
```

Result:

``` text
┌─or(0, NULL)─┐
│        ᴺᵁᴸᴸ │
└─────────────┘
```

## not

Calculates the logical negation of a value.

**Syntax**

``` sql
not(val);
```

Alias: The [Negation operator](../../sql-reference/operators/index.md#logical-negation-operator).

**Arguments**

- `val` — The value. [Int](../data-types/int-uint.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md) or [Nullable](../data-types/nullable.md).

**Returned value**

- `1`, if `val` evaluates to `false`,
- `0`, if `val` evaluates to `true`,
- `NULL`, if `val` is `NULL`.

Type: [UInt8](../../sql-reference/data-types/int-uint.md) or [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Example**

``` sql
SELECT NOT(1);
```

Result:

``` test
┌─not(1)─┐
│      0 │
└────────┘
```

## xor

Calculates the logical exclusive disjunction of two or more values. For more than two input values, the function first xor-s the first two values, then xor-s the result with the third value etc.

**Syntax**

``` sql
xor(val1, val2...)
```

**Arguments**

- `val1, val2, ...` — List of at least two values. [Int](../data-types/int-uint.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md) or [Nullable](../data-types/nullable.md).

**Returned value**

- `1`, for two values: if one of the values evaluates to `false` and other does not,
- `0`, for two values: if both values evaluate to `false` or to both `true`,
- `NULL`, if at least one of the inputs is `NULL`

Type: [UInt8](../../sql-reference/data-types/int-uint.md) or [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Example**

``` sql
SELECT xor(0, 1, 1);
```

Result:

``` text
┌─xor(0, 1, 1)─┐
│            0 │
└──────────────┘
```
