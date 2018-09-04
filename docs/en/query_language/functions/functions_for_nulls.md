# Functions for working with Nullable aggregates

## isNull

Checks whether the argument is [NULL](../syntax.md#null-literal).

```
isNull(x)
```

**Parameters:**

- `x` — A value with a non-compound data type.

**Returned value**

- `1` if `x` is `NULL`.
- `0` if `x` is not `NULL`.

**Example**

Input table

```
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Query

```
:) SELECT x FROM t_null WHERE isNull(y)

SELECT x
FROM t_null
WHERE isNull(y)

┌─x─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.010 sec.
```

## isNotNull

Checks whether the argument is [NULL](../syntax.md#null-literal).

```
isNotNull(x)
```

**Parameters:**

- `x` — A value with a non-compound data type.

**Returned value**

- `0` if `x` is `NULL`.
- `1` if `x` is not `NULL`.

**Example**

Input table

```
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Query

```
:) SELECT x FROM t_null WHERE isNotNull(y)

SELECT x
FROM t_null
WHERE isNotNull(y)

┌─x─┐
│ 2 │
└───┘

1 rows in set. Elapsed: 0.010 sec.
```

## coalesce

Checks from left to right whether `NULL` arguments were passed and returns the first non-`NULL` argument.

```
coalesce(x,...)
```

**Parameters:**

- Any number of parameters of a non-compound type. All parameters must be compatible by data type.

**Returned values**

- The first  non-`NULL` argument.
- `NULL`, if all arguments are `NULL`.

**Example**

Consider a list of contacts that may specify multiple ways to contact a customer.

```
┌─name─────┬─mail─┬─phone─────┬──icq─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │  123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

The `mail` and `phone` fields are of type String, but the `icq` field is `UInt32`, so it needs to be converted to `String`.

Get the first available contact method for the customer from the contact list:

```
:) SELECT coalesce(mail, phone, CAST(icq,'Nullable(String)')) FROM aBook

SELECT coalesce(mail, phone, CAST(icq, 'Nullable(String)'))
FROM aBook

┌─name─────┬─coalesce(mail, phone, CAST(icq, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                            │
│ client 2 │ ᴺᵁᴸᴸ                                                 │
└──────────┴──────────────────────────────────────────────────────┘

2 rows in set. Elapsed: 0.006 sec.
```

## ifNull

Returns an alternative value if the main argument is `NULL`.

```
ifNull(x,alt)
```

**Parameters:**

- `x` — The value to check for `NULL`.
- `alt` — The value that the function returns if `x` is `NULL`.

**Returned values**

- The value `x`, if `x` is not `NULL`.
- The value `alt`, if `x` is `NULL`.

**Example**

```
SELECT ifNull('a', 'b')

┌─ifNull('a', 'b')─┐
│ a                │
└──────────────────┘
```

```
SELECT ifNull(NULL, 'b')

┌─ifNull(NULL, 'b')─┐
│ b                 │
└───────────────────┘
```

## nullIf

Returns `NULL` if the arguments are equal.

```
nullIf(x, y)
```

**Parameters:**

`x`, `y` — Values for comparison. They must be compatible types, or ClickHouse will generate an exception.

**Returned values**

- `NULL`, if the arguments are equal.
- The `x` value, if the arguments are not equal.

**Example**

```
SELECT nullIf(1, 1)

┌─nullIf(1, 1)─┐
│         ᴺᵁᴸᴸ │
└──────────────┘
```

```
SELECT nullIf(1, 2)

┌─nullIf(1, 2)─┐
│            1 │
└──────────────┘
```

## assumeNotNull

Results in a value of type [Nullable](../../data_types/nullable.md#data_type-nullable) for a non- `Nullable`, if the value is not `NULL`.

```
assumeNotNull(x)
```

**Parameters:**

- `x` — The original value.

**Returned values**

- The original value from the non-`Nullable` type, if it is not `NULL`.
- The default value for the non-`Nullable` type if the original value was `NULL`.

**Example**

Consider the `t_null` table.

```
SHOW CREATE TABLE t_null

┌─statement─────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_null ( x Int8,  y Nullable(Int8)) ENGINE = TinyLog │
└───────────────────────────────────────────────────────────────────────────┘
```

```
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Apply the `resumenotnull` function to the `y` column.

```
SELECT assumeNotNull(y) FROM t_null

┌─assumeNotNull(y)─┐
│                0 │
│                3 │
└──────────────────┘
```

```
SELECT toTypeName(assumeNotNull(y)) FROM t_null

┌─toTypeName(assumeNotNull(y))─┐
│ Int8                         │
│ Int8                         │
└──────────────────────────────┘
```

## toNullable

Converts the argument type to `Nullable`.

```
toNullable(x)
```

**Parameters:**

- `x` — The value of any non-compound type.

**Returned value**

- The input value with a non-`Nullable` type.

**Example**

```
SELECT toTypeName(10)

┌─toTypeName(10)─┐
│ UInt8          │
└────────────────┘

SELECT toTypeName(toNullable(10))

┌─toTypeName(toNullable(10))─┐
│ Nullable(UInt8)            │
└────────────────────────────┘
```

