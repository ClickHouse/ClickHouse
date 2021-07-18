---
toc_priority: 63
toc_title: Nullable
---

# Functions for Working with Nullable Values {#functions-for-working-with-nullable-aggregates}

## isNull {#isnull}

Checks whether the argument is [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNull(x)
```

Alias: `ISNULL`.

**Arguments**

-   `x` — A value with a non-compound data type.

**Returned value**

-   `1` if `x` is `NULL`.
-   `0` if `x` is not `NULL`.

**Example**

Input table

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Query

``` sql
SELECT x FROM t_null WHERE isNull(y);
```

``` text
┌─x─┐
│ 1 │
└───┘
```

## isNotNull {#isnotnull}

Checks whether the argument is [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNotNull(x)
```

**Arguments:**

-   `x` — A value with a non-compound data type.

**Returned value**

-   `0` if `x` is `NULL`.
-   `1` if `x` is not `NULL`.

**Example**

Input table

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Query

``` sql
SELECT x FROM t_null WHERE isNotNull(y);
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## coalesce {#coalesce}

Checks from left to right whether `NULL` arguments were passed and returns the first non-`NULL` argument.

``` sql
coalesce(x,...)
```

**Arguments:**

-   Any number of parameters of a non-compound type. All parameters must be compatible by data type.

**Returned values**

-   The first non-`NULL` argument.
-   `NULL`, if all arguments are `NULL`.

**Example**

Consider a list of contacts that may specify multiple ways to contact a customer.

``` text
┌─name─────┬─mail─┬─phone─────┬──icq─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │  123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

The `mail` and `phone` fields are of type String, but the `icq` field is `UInt32`, so it needs to be converted to `String`.

Get the first available contact method for the customer from the contact list:

``` sql
SELECT coalesce(mail, phone, CAST(icq,'Nullable(String)')) FROM aBook;
```

``` text
┌─name─────┬─coalesce(mail, phone, CAST(icq, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                            │
│ client 2 │ ᴺᵁᴸᴸ                                                 │
└──────────┴──────────────────────────────────────────────────────┘
```

## ifNull {#ifnull}

Returns an alternative value if the main argument is `NULL`.

``` sql
ifNull(x,alt)
```

**Arguments:**

-   `x` — The value to check for `NULL`.
-   `alt` — The value that the function returns if `x` is `NULL`.

**Returned values**

-   The value `x`, if `x` is not `NULL`.
-   The value `alt`, if `x` is `NULL`.

**Example**

``` sql
SELECT ifNull('a', 'b');
```

``` text
┌─ifNull('a', 'b')─┐
│ a                │
└──────────────────┘
```

``` sql
SELECT ifNull(NULL, 'b');
```

``` text
┌─ifNull(NULL, 'b')─┐
│ b                 │
└───────────────────┘
```

## nullIf {#nullif}

Returns `NULL` if the arguments are equal.

``` sql
nullIf(x, y)
```

**Arguments:**

`x`, `y` — Values for comparison. They must be compatible types, or ClickHouse will generate an exception.

**Returned values**

-   `NULL`, if the arguments are equal.
-   The `x` value, if the arguments are not equal.

**Example**

``` sql
SELECT nullIf(1, 1);
```

``` text
┌─nullIf(1, 1)─┐
│         ᴺᵁᴸᴸ │
└──────────────┘
```

``` sql
SELECT nullIf(1, 2);
```

``` text
┌─nullIf(1, 2)─┐
│            1 │
└──────────────┘
```

## assumeNotNull {#assumenotnull}

Results in a value of type [Nullable](../../sql-reference/data-types/nullable.md) for a non- `Nullable`, if the value is not `NULL`.

``` sql
assumeNotNull(x)
```

**Arguments:**

-   `x` — The original value.

**Returned values**

-   The original value from the non-`Nullable` type, if it is not `NULL`.
-   Implementation specific result if the original value was `NULL`.

**Example**

Consider the `t_null` table.

``` sql
SHOW CREATE TABLE t_null;
```

``` text
┌─statement─────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_null ( x Int8,  y Nullable(Int8)) ENGINE = TinyLog │
└───────────────────────────────────────────────────────────────────────────┘
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Apply the `assumeNotNull` function to the `y` column.

``` sql
SELECT assumeNotNull(y) FROM t_null;
```

``` text
┌─assumeNotNull(y)─┐
│                0 │
│                3 │
└──────────────────┘
```

``` sql
SELECT toTypeName(assumeNotNull(y)) FROM t_null;
```

``` text
┌─toTypeName(assumeNotNull(y))─┐
│ Int8                         │
│ Int8                         │
└──────────────────────────────┘
```

## toNullable {#tonullable}

Converts the argument type to `Nullable`.

``` sql
toNullable(x)
```

**Arguments:**

-   `x` — The value of any non-compound type.

**Returned value**

-   The input value with a `Nullable` type.

**Example**

``` sql
SELECT toTypeName(10);
```

``` text
┌─toTypeName(10)─┐
│ UInt8          │
└────────────────┘
```

``` sql
SELECT toTypeName(toNullable(10));
```

``` text
┌─toTypeName(toNullable(10))─┐
│ Nullable(UInt8)            │
└────────────────────────────┘
```

