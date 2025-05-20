---
slug: /en/sql-reference/functions/functions-for-nulls
sidebar_position: 135
sidebar_label: Nullable
---

# Functions for Working with Nullable Values

## isNull

Returns whether the argument is [NULL](../../sql-reference/syntax.md#null).

See also operator [`IS NULL`](../operators/index.md#is_null).

**Syntax**

``` sql
isNull(x)
```

Alias: `ISNULL`.

**Arguments**

- `x` — A value of non-compound data type.

**Returned value**

- `1` if `x` is `NULL`.
- `0` if `x` is not `NULL`.

**Example**

Table:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Query:

``` sql
SELECT x FROM t_null WHERE isNull(y);
```

Result:

``` text
┌─x─┐
│ 1 │
└───┘
```

## isNullable

Returns `1` if a column is [Nullable](../data-types/nullable.md) (i.e allows `NULL` values), `0` otherwise.

**Syntax**

``` sql
isNullable(x)
```

**Arguments**

- `x` — column.

**Returned value**

- `1` if `x` allows `NULL` values. [UInt8](../data-types/int-uint.md).
- `0` if `x` does not allow `NULL` values. [UInt8](../data-types/int-uint.md).

**Example**

Query:

``` sql
CREATE TABLE tab (ordinary_col UInt32, nullable_col Nullable(UInt32)) ENGINE = Log;
INSERT INTO tab (ordinary_col, nullable_col) VALUES (1,1), (2, 2), (3,3);
SELECT isNullable(ordinary_col), isNullable(nullable_col) FROM tab;    
```

Result:

``` text
   ┌───isNullable(ordinary_col)──┬───isNullable(nullable_col)──┐
1. │                           0 │                           1 │
2. │                           0 │                           1 │
3. │                           0 │                           1 │
   └─────────────────────────────┴─────────────────────────────┘
```

## isNotNull

Returns whether the argument is not [NULL](../../sql-reference/syntax.md#null-literal).

See also operator [`IS NOT NULL`](../operators/index.md#is_not_null).

``` sql
isNotNull(x)
```

**Arguments:**

- `x` — A value of non-compound data type.

**Returned value**

- `1` if `x` is not `NULL`.
- `0` if `x` is `NULL`.

**Example**

Table:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Query:

``` sql
SELECT x FROM t_null WHERE isNotNull(y);
```

Result:

``` text
┌─x─┐
│ 2 │
└───┘
```

## isNotDistinctFrom

Performs null-safe comparison. Used to compare JOIN keys which contain NULL values in the JOIN ON section.
This function will consider two `NULL` values as identical and will return `true`, which is distinct from the usual
equals behavior where comparing two `NULL` values would return `NULL`.

:::note
This function is an internal function used by the implementation of JOIN ON. Please do not use it manually in queries.
:::

**Syntax**

``` sql
isNotDistinctFrom(x, y)
```

**Arguments**

- `x` — first JOIN key.
- `y` — second JOIN key.

**Returned value**

- `true` when `x` and `y` are both `NULL`.
- `false` otherwise.

**Example**

For a complete example see: [NULL values in JOIN keys](../../sql-reference/statements/select/join#null-values-in-join-keys).

## isZeroOrNull

Returns whether the argument is 0 (zero) or [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isZeroOrNull(x)
```

**Arguments:**

- `x` — A value of non-compound data type.

**Returned value**

- `1` if `x` is 0 (zero) or `NULL`.
- `0` else.

**Example**

Table:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    0 │
│ 3 │    3 │
└───┴──────┘
```

Query:

``` sql
SELECT x FROM t_null WHERE isZeroOrNull(y);
```

Result:

``` text
┌─x─┐
│ 1 │
│ 2 │
└───┘
```

## coalesce

Returns the leftmost non-`NULL` argument.

``` sql
coalesce(x,...)
```

**Arguments:**

- Any number of parameters of non-compound type. All parameters must be of mutually compatible data types.

**Returned values**

- The first non-`NULL` argument
- `NULL`, if all arguments are `NULL`.

**Example**

Consider a list of contacts that may specify multiple ways to contact a customer.

``` text
┌─name─────┬─mail─┬─phone─────┬──telegram─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │       123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │      ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴───────────┘
```

The `mail` and `phone` fields are of type String, but the `telegram` field is `UInt32`, so it needs to be converted to `String`.

Get the first available contact method for the customer from the contact list:

``` sql
SELECT name, coalesce(mail, phone, CAST(telegram,'Nullable(String)')) FROM aBook;
```

``` text
┌─name─────┬─coalesce(mail, phone, CAST(telegram, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                                 │
│ client 2 │ ᴺᵁᴸᴸ                                                      │
└──────────┴───────────────────────────────────────────────────────────┘
```

## ifNull

Returns an alternative value if the argument is `NULL`.

``` sql
ifNull(x, alt)
```

**Arguments:**

- `x` — The value to check for `NULL`.
- `alt` — The value that the function returns if `x` is `NULL`.

**Returned values**

- `x` if `x` is not `NULL`.
- `alt` if `x` is `NULL`.

**Example**

Query:

``` sql
SELECT ifNull('a', 'b');
```

Result:

``` text
┌─ifNull('a', 'b')─┐
│ a                │
└──────────────────┘
```

Query:

``` sql
SELECT ifNull(NULL, 'b');
```

Result:

``` text
┌─ifNull(NULL, 'b')─┐
│ b                 │
└───────────────────┘
```

## nullIf

Returns `NULL` if both arguments are equal.

``` sql
nullIf(x, y)
```

**Arguments:**

`x`, `y` — Values to compare. Must be of compatible types.

**Returned values**

- `NULL` if the arguments are equal.
- `x` if the arguments are not equal.

**Example**

Query:

``` sql
SELECT nullIf(1, 1);
```

Result:

``` text
┌─nullIf(1, 1)─┐
│         ᴺᵁᴸᴸ │
└──────────────┘
```

Query:

``` sql
SELECT nullIf(1, 2);
```

Result:

``` text
┌─nullIf(1, 2)─┐
│            1 │
└──────────────┘
```

## assumeNotNull

Returns the corresponding non-`Nullable` value for a value of [Nullable](../data-types/nullable.md) type. If the original value is `NULL`, an arbitrary result can be returned. See also functions `ifNull` and `coalesce`.

``` sql
assumeNotNull(x)
```

**Arguments:**

- `x` — The original value.

**Returned values**

- The input value as non-`Nullable` type, if it is not `NULL`.
- An arbitrary value, if the input value is `NULL`.

**Example**

Table:

``` text

┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Query:

``` sql
SELECT assumeNotNull(y) FROM table;
```

Result:

``` text
┌─assumeNotNull(y)─┐
│                0 │
│                3 │
└──────────────────┘
```

Query:

``` sql
SELECT toTypeName(assumeNotNull(y)) FROM t_null;
```

Result:

``` text
┌─toTypeName(assumeNotNull(y))─┐
│ Int8                         │
│ Int8                         │
└──────────────────────────────┘
```

## toNullable

Converts the argument type to `Nullable`.

``` sql
toNullable(x)
```

**Arguments:**

- `x` — A value of non-compound type.

**Returned value**

- The input value but of `Nullable` type.

**Example**

Query:

``` sql
SELECT toTypeName(10);
```

Result:

``` text
┌─toTypeName(10)─┐
│ UInt8          │
└────────────────┘
```

Query:

``` sql
SELECT toTypeName(toNullable(10));
```

Result:

``` text
┌─toTypeName(toNullable(10))─┐
│ Nullable(UInt8)            │
└────────────────────────────┘
```
