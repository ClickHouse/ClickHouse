---
toc_priority: 37
toc_title: COLUMN
---

# Column Manipulations {#column-manipulations}

A set of queries that allow changing the table structure.

Syntax:

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

In the query, specify a list of one or more comma-separated actions.
Each action is an operation on a column.

The following actions are supported:

-   [ADD COLUMN](#alter_add-column) — Adds a new column to the table.
-   [DROP COLUMN](#alter_drop-column) — Deletes the column.
-   [RENAME COLUMN](#alter_rename-column) — Renames the column.
-   [CLEAR COLUMN](#alter_clear-column) — Resets column values.
-   [COMMENT COLUMN](#alter_comment-column) — Adds a text comment to the column.
-   [MODIFY COLUMN](#alter_modify-column) — Changes column’s type, default expression and TTL.
-   [MODIFY COLUMN REMOVE](#modify-remove) — Removes one of the column properties.
-   [RENAME COLUMN](#alter_rename-column) — Renames an existing column.

These actions are described in detail below.

## ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

Adds a new column to the table with the specified `name`, `type`, [`codec`](../../../sql-reference/statements/create/table.md#codecs) and `default_expr` (see the section [Default expressions](../../../sql-reference/statements/create/table.md#create-default-values)).

If the `IF NOT EXISTS` clause is included, the query won’t return an error if the column already exists. If you specify `AFTER name_after` (the name of another column), the column is added after the specified one in the list of table columns. If you want to add a column to the beginning of the table use the `FIRST` clause. Otherwise, the column is added to the end of the table. For a chain of actions, `name_after` can be the name of a column that is added in one of the previous actions.

Adding a column just changes the table structure, without performing any actions with data. The data does not appear on the disk after `ALTER`. If the data is missing for a column when reading from the table, it is filled in with default values (by performing the default expression if there is one, or using zeros or empty strings). The column appears on the disk after merging data parts (see [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)).

This approach allows us to complete the `ALTER` query instantly, without increasing the volume of old data.

Example:

``` sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

``` text
Added1  UInt32
CounterID       UInt32
StartDate       Date
UserID  UInt32
VisitID UInt32
NestedColumn.A  Array(UInt8)
NestedColumn.S  Array(String)
Added2  UInt32
ToDrop  UInt32
Added3  UInt32
```
## DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

Deletes the column with the name `name`. If the `IF EXISTS` clause is specified, the query won’t return an error if the column does not exist.

Deletes data from the file system. Since this deletes entire files, the query is completed almost instantly.

!!! warning "Warning"
    You can’t delete a column if it is referenced by [materialized view](../../../sql-reference/statements/create/view.md#materialized). Otherwise, it returns an error.

Example:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

## RENAME COLUMN {#alter_rename-column}

``` sql
RENAME COLUMN [IF EXISTS] name to new_name
```

Renames the column `name` to `new_name`. If the `IF EXISTS` clause is specified, the query won’t return an error if the column does not exist. Since renaming does not involve the underlying data, the query is completed almost instantly.

**NOTE**: Columns specified in the key expression of the table (either with `ORDER BY` or `PRIMARY KEY`) cannot be renamed. Trying to change these columns will produce `SQL Error [524]`. 

Example:

``` sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

## CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Resets all data in a column for a specified partition. Read more about setting the partition name in the section [How to specify the partition expression](#alter-how-to-specify-part-expr).

If the `IF EXISTS` clause is specified, the query won’t return an error if the column does not exist.

Example:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

## COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

Adds a comment to the column. If the `IF EXISTS` clause is specified, the query won’t return an error if the column does not exist.

Each column can have one comment. If a comment already exists for the column, a new comment overwrites the previous comment.

Comments are stored in the `comment_expression` column returned by the [DESCRIBE TABLE](../../../sql-reference/statements/misc.md#misc-describe-table) query.

Example:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

## MODIFY COLUMN {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL] [AFTER name_after | FIRST]
```

This query changes the `name` column properties:

-   Type

-   Default expression

-   TTL

For examples of columns TTL modifying, see [Column TTL](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-column-ttl).

If the `IF EXISTS` clause is specified, the query won’t return an error if the column does not exist.

The query also can change the order of the columns using `FIRST | AFTER` clause, see [ADD COLUMN](#alter_add-column) description.

When changing the type, values are converted as if the [toType](../../../sql-reference/functions/type-conversion-functions.md) functions were applied to them. If only the default expression is changed, the query does not do anything complex, and is completed almost instantly.

Example:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

The `ALTER` query is atomic. For MergeTree tables it is also lock-free.

The `ALTER` query for changing columns is replicated. The instructions are saved in ZooKeeper, then each replica applies them. All `ALTER` queries are run in the same order. The query waits for the appropriate actions to be completed on the other replicas. However, a query to change columns in a replicated table can be interrupted, and all actions will be performed asynchronously.

## MODIFY COLUMN REMOVE {#modify-remove}

Removes one of the column properties: `DEFAULT`, `ALIAS`, `MATERIALIZED`, `CODEC`, `COMMENT`, `TTL`.

Syntax:

```sql
ALTER TABLE table_name MODIFY column_name REMOVE property;
```

**Example**

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**See Also**

- [REMOVE TTL](ttl.md).

## RENAME COLUMN {#alter_rename-column}

Renames an existing column.

Syntax:

```sql
ALTER TABLE table_name RENAME COLUMN column_name TO new_column_name
```

**Example**

```sql
ALTER TABLE table_with_ttl RENAME COLUMN column_ttl TO column_ttl_new;
```

## Limitations {#alter-query-limitations}

The `ALTER` query lets you create and delete separate elements (columns) in nested data structures, but not whole nested data structures. To add a nested data structure, you can add columns with a name like `name.nested_name` and the type `Array(T)`. A nested data structure is equivalent to multiple array columns with a name that has the same prefix before the dot.

There is no support for deleting columns in the primary key or the sampling key (columns that are used in the `ENGINE` expression). Changing the type for columns that are included in the primary key is only possible if this change does not cause the data to be modified (for example, you are allowed to add values to an Enum or to change a type from `DateTime` to `UInt32`).

If the `ALTER` query is not sufficient to make the table changes you need, you can create a new table, copy the data to it using the [INSERT SELECT](../../../sql-reference/statements/insert-into.md#insert_query_insert-select) query, then switch the tables using the [RENAME](../../../sql-reference/statements/misc.md#misc_operations-rename) query and delete the old table. You can use the [clickhouse-copier](../../../operations/utilities/clickhouse-copier.md) as an alternative to the `INSERT SELECT` query.

The `ALTER` query blocks all reads and writes for the table. In other words, if a long `SELECT` is running at the time of the `ALTER` query, the `ALTER` query will wait for it to complete. At the same time, all new queries to the same table will wait while this `ALTER` is running.

For tables that do not store data themselves (such as `Merge` and `Distributed`), `ALTER` just changes the table structure, and does not change the structure of subordinate tables. For example, when running ALTER for a `Distributed` table, you will also need to run `ALTER` for the tables on all remote servers.
