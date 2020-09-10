---
toc_priority: 53
toc_title: TRUNCATE
---

# TRUNCATE Statement {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Removes all data from a table. When the clause `IF EXISTS` is omitted, the query returns an error if the table does not exist.

The `TRUNCATE` query is not supported for [View](../../engines/table-engines/special/view.md), [File](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) and [Null](../../engines/table-engines/special/null.md) table engines.
