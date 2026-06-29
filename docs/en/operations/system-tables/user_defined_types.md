---
description: 'System table containing information about user-defined types.'
keywords: ['system table', 'user_defined_types']
slug: /operations/system-tables/user_defined_types
title: 'system.user_defined_types'
doc_type: 'reference'
---

## Description {#description}

Contains a list of all user-defined types created with
[`CREATE TYPE`](/sql-reference/statements/create/type).

## Columns {#columns}

- `name` ([String](../../sql-reference/data-types/string.md)) — Name of the user-defined type.
- `base_type_ast_string` ([String](../../sql-reference/data-types/string.md)) — The base type the user-defined type expands to.
- `type_parameters_ast_string` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The formal parameters of a parameterized type, or `NULL` if the type has none.
- `input_expression` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The `INPUT` expression recorded with the type, if any.
- `output_expression` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The `OUTPUT` expression recorded with the type, if any.
- `default_expression` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The `DEFAULT` expression recorded with the type, if any.
- `create_query_string` ([String](../../sql-reference/data-types/string.md)) — The `CREATE TYPE` query that was used to create the type.

**Example**

```sql
CREATE TYPE KeyValue(K, V) AS Tuple(K, V);
SELECT name, base_type_ast_string, type_parameters_ast_string FROM system.user_defined_types;
```

```text
┌─name─────┬─base_type_ast_string─┬─type_parameters_ast_string─┐
│ KeyValue │ Tuple(K, V)          │ K, V                       │
└──────────┴──────────────────────┴────────────────────────────┘
```

**See also**

- [`CREATE TYPE`](/sql-reference/statements/create/type)
- [`DROP TYPE`](/sql-reference/statements/drop#drop-type)
- [`SHOW TYPES`](/sql-reference/statements/show#show-types)
