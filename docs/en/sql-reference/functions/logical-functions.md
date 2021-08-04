---
toc_priority: 37
toc_title: Logical
---

# Logical Functions {#logical-functions}

Performs logical operations on arguments of any numeric types, but returns a [UInt8](../../sql-reference/data-types/int-uint.md) number equal to 0, 1 or `NULL` in some cases.

Zero as an argument is considered `false`, while any non-zero value is considered `true`.

## and {#logical-and-function}

Calculates the result of the logical conjunction between two or more values. Corresponds to [Logical AND Operator](../../sql-reference/operators/index.md#logical-and-operator).

**Syntax**

``` sql
and(val1, val2...)
```

**Arguments**

-   `val1, val2, ...` — List of at least two values. [Int](../../sql-reference/data-types/int-uint.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Nullable](../../sql-reference/data-types/nullable.md). 

**Returned value**

-   `0`, if there is at least one zero value argument.
-   `NULL`, if there are no zero values arguments and there is at least one `NULL` argument.
-   `1`, otherwise.

Type: [UInt8](../../sql-reference/data-types/int-uint.md) or [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

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

## or {#logical-or-function}

Calculates the result of the logical disjunction between two or more values. Corresponds to [Logical OR Operator](../../sql-reference/operators/index.md#logical-or-operator).

**Syntax**

``` sql
and(val1, val2...)
```

**Arguments**

-   `val1, val2, ...` — List of at least two values. [Int](../../sql-reference/data-types/int-uint.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Nullable](../../sql-reference/data-types/nullable.md). 

**Returned value**

-   `1`, if there is at least one non-zero value.
-   `0`, if there are only zero values.
-   `NULL`, if there are only zero values and `NULL`.

Type: [UInt8](../../sql-reference/data-types/int-uint.md) or [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

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

## not {#logical-not-function}

Calculates the result of the logical negation of the value. Corresponds to [Logical Negation Operator](../../sql-reference/operators/index.md#logical-negation-operator).

**Syntax**

``` sql
not(val);
```

**Arguments**

-   `val` — The value. [Int](../../sql-reference/data-types/int-uint.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Nullable](../../sql-reference/data-types/nullable.md). 

**Returned value**

-   `1`, if the `val` is `0`.
-   `0`, if the `val` is a non-zero value.
-   `NULL`, if the `val` is a `NULL` value.

Type: [UInt8](../../sql-reference/data-types/int-uint.md) or [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT NOT(1);
```

Result:

``` test
┌─not(1)─┐
│      0 │
└────────┘
```

## xor {#logical-xor-function}

Calculates the result of the logical exclusive disjunction between two or more values. For more than two values the function works as if it calculates `XOR` of the first two values and then uses the result with the next value to calculate `XOR` and so on.

**Syntax**

``` sql
xor(val1, val2...)
```

**Arguments**

-   `val1, val2, ...` — List of at least two values. [Int](../../sql-reference/data-types/int-uint.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Nullable](../../sql-reference/data-types/nullable.md). 

**Returned value**

-   `1`, for two values: if one of the values is zero and other is not. 
-   `0`, for two values: if both values are zero or non-zero at the same time.
-   `NULL`, if there is at least one `NULL` value.

Type: [UInt8](../../sql-reference/data-types/int-uint.md) or [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT xor(0, 1, 1);
```

Result:

``` text
┌─xor(0, 1, 1)─┐
│            0 │
└──────────────┘
```
