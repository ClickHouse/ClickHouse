---
toc_priority: 48
toc_title: RENAME
---

# RENAME Statement {#misc_operations-rename}

Renames databases, tables, or dictionaries. Several entities can be renamed in a single query.
Note that the `RENAME` query with several entities is non-atomic operation. To swap entities names atomically, use the [EXCHANGE](./exchange.md) statement.

!!! note "Note"
    The `RENAME` query is supported by the [Atomic](../../engines/database-engines/atomic.md) database engine only.

**Syntax**

```sql
RENAME DATABASE|TABLE|DICTIONARY name TO new_name [,...] [ON CLUSTER cluster]
```

## RENAME DATABASE {#misc_operations-rename_database}

Renames databases.

**Syntax**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

## RENAME TABLE {#misc_operations-rename_table}

Renames one or more tables.

Renaming tables is a light operation. If you pass a different database after `TO`, the table will be moved to this database. However, the directories with databases must reside in the same file system. Otherwise, an error is returned. 
If you rename multiple tables in one query, the operation is not atomic. It may be partially executed, and queries in other sessions may get `Table ... does not exist ...` error.

**Syntax**

``` sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**Example**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

## RENAME DICTIONARY {#rename_dictionary}

Renames one or several dictionaries. This query can be used to move dictionaries between databases.

**Syntax**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**See Also**

-   [Dictionaries](../../sql-reference/dictionaries/index.md)
