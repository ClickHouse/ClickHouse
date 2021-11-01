---
toc_priority: 38
toc_title: merge
---

# merge {#merge}

Creates a temporary [Merge](../../engines/table-engines/special/merge.md) table. The table structure is taken from the first table encountered that matches the regular expression.

**Syntax**

```sql
merge('db_name', 'tables_regexp')
```
**Arguments**

- `db_name` — Database name or a regular expression to match the DB names. You can use a constant expression that returns a string, for example, `currentDatabase()`. 

- `tables_regexp` — A regular expression to match the table names in the specified DB or DBs.

**See Also**

-   [Merge](../../engines/table-engines/special/merge.md) table engine

