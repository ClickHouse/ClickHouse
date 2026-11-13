---
description: 'Creates a temporary Merge table. The structure will be derived from underlying tables by using a union of their columns and by deriving common types.'
sidebar_label: 'merge'
sidebar_position: 130
slug: /sql-reference/table-functions/merge
title: 'merge'
doc_type: 'reference'
---

# merge Table Function

Creates a temporary [Merge](../../engines/table-engines/special/merge.md) table.
The table schema is derived from underlying tables by using a union of their columns and by deriving common types.
The same virtual columns are available as for the [Merge](../../engines/table-engines/special/merge.md) table engine.

## Syntax {#syntax}

```sql
merge(['db_name',] 'tables_regexp')
```
## Arguments {#arguments}

| Argument        | Description                                                                                                                                                                                                                                                                                     |
|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `db_name`       | Possible values (optional, default is `currentDatabase()`):<br/>    - database name,<br/>    - constant expression that returns a string with a database name, for example, `currentDatabase()`,<br/>    - `REGEXP(expression)`, where `expression` is a regular expression to match the DB names. |
| `tables_regexp` | A regular expression to match the table names in the specified DB or DBs.                                                                                                                                                                                                                       |

## Related {#related}

- [Merge](../../engines/table-engines/special/merge.md) table engine
