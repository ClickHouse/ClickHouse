---
description: 'Documentation for SET Statement'
sidebar_label: 'SET'
sidebar_position: 50
slug: /sql-reference/statements/set
title: 'SET Statement'
doc_type: 'reference'
---

# SET Statement

```sql
SET param = value
```

Assigns `value` to the `param` [setting](/operations/settings/overview) for the current session. You cannot change [server settings](../../operations/server-configuration-parameters/settings.md) this way.

You can also set all the values from the specified settings profile in a single query.

```sql
SET profile = 'profile-name-from-the-settings-file'
```

For boolean settings set to true, you can use a shorthand syntax by omitting the value assignment. When only the setting name is specified, it is automatically set to `1` (true).

```sql
-- These are equivalent:
SET force_index_by_date = 1
SET force_index_by_date
```

## Setting query parameters {#setting-query-parameters}

The `SET` statement can also be used to define query parameters by prefixing the parameter name with `param_`.
Query parameters allow you to write generic queries with placeholders that are replaced with actual values at execution time.

```sql
SET param_name = value
```

To use a query parameter in your query, reference it with the syntax `{name: datatype}`:

```sql
SET param_id = 42;
SET param_name = 'John';

SELECT * FROM users
WHERE id = {id: UInt32}
AND name = {name: String};
```

Query parameters are particularly useful when the same query needs to be executed multiple times with different values.

For more detailed information about query parameters, including usage with the `Identifier` type, see [Defining and Using Query Parameters](../../sql-reference/syntax.md#defining-and-using-query-parameters).

For more information, see [Settings](../../operations/settings/settings.md).
