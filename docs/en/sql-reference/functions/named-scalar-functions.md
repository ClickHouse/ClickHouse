---
description: 'Functions for reading named cached scalar values'
sidebar_label: 'Named Scalar'
slug: /sql-reference/functions/named-scalar-functions
title: 'Named Scalar Functions'
doc_type: 'reference'
---

# Named Scalar Functions

Functions for reading values of named cached scalars defined with
[`CREATE NAMED SCALAR`](/sql-reference/statements/create/named-scalar).

Both functions require the `getNamedScalar` function-execute grant. The
underlying cache kind (`local` or `shared`) is transparent to the caller —
the server dispatches to the correct backend automatically.

## getNamedScalar

Returns the current cached value of the named scalar.

**Syntax**

```sql
getNamedScalar(name)
```

**Arguments**

- `name` — scalar name, must be a constant string expression.

**Returned value**

The cached value with the type recorded at the time of the last successful
refresh. If the scalar was created without a `REFRESH` clause (static scalar),
the type is fixed at creation time.

**Errors**

| Error code | Condition |
|---|---|
| `NAMED_SCALAR_NOT_FOUND` | No scalar with this name is loaded on the server. |
| `NAMED_SCALAR_HAS_NO_VALUE` | The scalar exists but has not yet produced a value (background populate is still running, or every attempt so far failed). |

**Example**

```sql
-- Create and populate
CREATE NAMED SCALAR fx_rate
    REFRESH EVERY 1 HOUR
    AS SELECT rate FROM rates WHERE pair = 'EUR/USD';

-- Read
SELECT amount * getNamedScalar('fx_rate') AS converted FROM orders;
```

**See also**

- [`getNamedScalarOrDefault`](#getnamedscalararordefault) — returns a default value instead of throwing.
- [`system.named_scalars`](/operations/system-tables/named_scalars) — runtime introspection.

## getNamedScalarOrDefault {#getnamedscalararordefault}

Returns the current cached value of the named scalar, or a caller-supplied
default when the scalar is not found or has no value yet.

**Syntax**

```sql
getNamedScalarOrDefault(name, default)
```

**Arguments**

- `name` — scalar name, must be a constant string expression.
- `default` — value to return when `getNamedScalar` would throw
  `NAMED_SCALAR_NOT_FOUND` or `NAMED_SCALAR_HAS_NO_VALUE`.
  May be any expression that is evaluable at query compile time.

**Returned value**

The cached scalar value, or `default` if the scalar is absent or has no value.
The return type is the type of the cached value; when the default is used, it
is cast to that type if possible.

:::warning Return type follows the scalar's stored value
The return type of `getNamedScalarOrDefault` is determined at query compile
time from whichever of (cached scalar value, default) is available right
then. Two executions of the same SQL can therefore see different return
types: when the scalar is unpopulated the type comes from `default`; once
the scalar populates, the type comes from the stored value.

For type-stable contexts — views, prepared statements, replicated DDL
bodies, expressions where the result is composed with another column —
make sure the default expression's type matches the scalar's stored
type, e.g.

```sql
-- prefer this:
SELECT getNamedScalarOrDefault('p99_threshold', toUInt64(1000)) -- explicit Int64-shaped default
-- over this:
SELECT getNamedScalarOrDefault('p99_threshold', 1000)            -- inferred Int32; mismatches if scalar is UInt64
```
:::

**Example**

```sql
-- Returns 0 when 'flap' is not defined or not yet populated.
SELECT getNamedScalarOrDefault('flap', 0);

-- Fallback threshold while the scalar is initializing.
SELECT count() FROM events
WHERE latency_ms > getNamedScalarOrDefault('p99_threshold', 1000);
```

**See also**

- [`getNamedScalar`](#getnamedscalar) — throws on missing or empty scalar.
- [`system.named_scalars`](/operations/system-tables/named_scalars) — runtime introspection.
