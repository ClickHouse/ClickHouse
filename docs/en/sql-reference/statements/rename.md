---
toc_priority: 48
toc_title: RENAME
---

# RENAME Statement {#misc_operations-rename}

## RENAME DATABASE {#misc_operations-rename_database}
Renames database, it is supported only for Atomic database engine.

```
RENAME DATABASE atomic_database1 TO atomic_database2 [ON CLUSTER cluster]
```

## RENAME TABLE {#misc_operations-rename_table}
Renames one or more tables.

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

Renaming tables is a light operation. If you indicated another database after `TO`, the table will be moved to this database. However, the directories with databases must reside in the same file system (otherwise, an error is returned). If you rename multiple tables in one query, this is a non-atomic operation, it may be partially executed, queries in other sessions may receive the error `Table ... does not exist ..`.
