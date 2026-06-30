---
description: 'Documentation for CREATE TYPE (user-defined types)'
sidebar_label: 'TYPE'
sidebar_position: 44
slug: /sql-reference/statements/create/type
title: 'CREATE TYPE'
doc_type: 'reference'
---

Creates a user-defined type: a named, optionally parameterized alias for an existing
data type. Wherever the user-defined type is used (for example, as a column type), it
is expanded to its definition. This is similar in spirit to user-defined functions
created with [`CREATE FUNCTION`](/sql-reference/statements/create/function), but for
data types instead of expressions.

**Syntax**

```sql
CREATE TYPE [IF NOT EXISTS | OR REPLACE] name [(parameter0, ...)] AS base_type
```

- `name` — the name of the new type. It must be unique among user-defined types and
  may be quoted with backticks to use otherwise reserved characters, e.g. `` `My-Type` ``.
  Type names are case-sensitive.
- `(parameter0, ...)` — an optional list of formal parameters. When the type is used,
  each parameter is substituted by the corresponding argument.
- `base_type` — the definition the type expands to. It can be any data type expression,
  may reference the formal parameters, and may reference other user-defined types.

`IF NOT EXISTS` makes the statement a no-op if a type with the same name already exists.
`OR REPLACE` redefines an existing type. Without either, creating a type whose name is
already taken raises the `TYPE_ALREADY_EXISTS` exception.

There are a few restrictions:

- The `base_type` must resolve to a valid data type. Referencing an unknown type
  (including a recursive reference to the type being defined) raises the `UNKNOWN_TYPE`
  exception.
- All parameters used in `base_type` must be listed in the parameter list.

**Example: a simple alias**

```sql title="Query"
CREATE TYPE UserId AS UInt64;

CREATE TABLE users (id UserId, name String) ENGINE = Memory;
SELECT name, type FROM system.columns WHERE table = 'users' ORDER BY name;
```

```text title="Response"
┌─name─┬─type───┐
│ id   │ UInt64 │
│ name │ String │
└──────┴────────┘
```

The column `id` is stored as a plain `UInt64`: the user-defined type is expanded to its
definition at table-creation time.

**Example: a parameterized type**

```sql title="Query"
CREATE TYPE KeyValue(K, V) AS Tuple(K, V);

CREATE TABLE events (data KeyValue(String, Float64)) ENGINE = Memory;
SELECT type FROM system.columns WHERE table = 'events';
```

```text title="Response"
┌─type─────────────────────┐
│ Tuple(String, Float64)   │
└──────────────────────────┘
```

User-defined types can also be nested and combined with built-in types, for example
`CREATE TYPE ProductList(T) AS Array(T)` or
`CREATE TYPE UserIdArray AS Array(UserId)`.

## Inspecting user-defined types {#inspecting-user-defined-types}

The currently defined types are exposed through the
[`system.user_defined_types`](/operations/system-tables/user_defined_types) table and
through the [`SHOW TYPES`](/sql-reference/statements/show#show-types) and
[`SHOW TYPE`](/sql-reference/statements/show#show-type) statements.

## Related content {#related-content}

- [`DROP TYPE`](/sql-reference/statements/drop#drop-type)
- [`SHOW TYPES` / `SHOW TYPE`](/sql-reference/statements/show#show-types)
