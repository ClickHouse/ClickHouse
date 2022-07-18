---
sidebar_position: 38
sidebar_label: merge
---

# merge {#merge}

Creates a temporary [Merge](../../engines/table-engines/special/merge.md) table. The table structure is taken from the first table encountered that matches the regular expression.

**Syntax**

```sql
merge('db_name', 'tables_regexp')
```
**Arguments**

- `db_name` — Possible values:
    - database name, 
    - constant expression that returns a string with a database name, for example, `currentDatabase()`,
    - `REGEXP(expression)`, where `expression` is a regular expression to match the DB names.

- `tables_regexp` — A regular expression to match the table names in the specified DB or DBs.

**See Also**

-   [Merge](../../engines/table-engines/special/merge.md) table engine

