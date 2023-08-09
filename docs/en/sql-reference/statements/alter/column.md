---
sidebar_position: 37
sidebar_label: COLUMN
---

# Column Manipulations

A set of queries that allow changing the table structure.

Syntax:

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

In the query, specify a list of one or more comma-separated actions.
Each action is an operation on a column.

The following actions are supported:

-   [ADD COLUMN](#add-column) — Adds a new column to the table.
-   [DROP COLUMN](#drop-column) — Deletes the column.
-   [RENAME COLUMN](#rename-column) — Renames an existing column.
-   [CLEAR COLUMN](#clear-column) — Resets column values.
-   [COMMENT COLUMN](#comment-column) — Adds a text comment to the column.
-   [MODIFY COLUMN](#modify-column) — Changes column’s type, default expression and TTL.
-   [MODIFY COLUMN REMOVE](#modify-column-remove) — Removes one of the column properties.
-   [MATERIALIZE COLUMN](#materialize-column) — Materializes the column in the parts where the column is missing.

These actions are described in detail below.

## ADD COLUMN

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

Adds a new column to the table with the specified `name`, `type`, [`codec`](../create/table.md#codecs) and `default_expr` (see the section [Default expressions](../../../sql-reference/statements/create/table.md#create-default-values)).

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

## DROP COLUMN

``` sql
DROP COLUMN [IF EXISTS] name
```

Deletes the column with the name `name`. If the `IF EXISTS` clause is specified, the query won’t return an error if the column does not exist.

Deletes data from the file system. Since this deletes entire files, the query is completed almost instantly.

:::warning    
You can’t delete a column if it is referenced by [materialized view](../../../sql-reference/statements/create/view.md#materialized). Otherwise, it returns an error.
:::

Example:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

## RENAME COLUMN

``` sql
RENAME COLUMN [IF EXISTS] name to new_name
```

Renames the column `name` to `new_name`. If the `IF EXISTS` clause is specified, the query won’t return an error if the column does not exist. Since renaming does not involve the underlying data, the query is completed almost instantly.

**NOTE**: Columns specified in the key expression of the table (either with `ORDER BY` or `PRIMARY KEY`) cannot be renamed. Trying to change these columns will produce `SQL Error [524]`.

Example:

``` sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

## CLEAR COLUMN

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Resets all data in a column for a specified partition. Read more about setting the partition name in the section [How to specify the partition expression](#alter-how-to-specify-part-expr).

If the `IF EXISTS` clause is specified, the query won’t return an error if the column does not exist.

Example:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

## COMMENT COLUMN

``` sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

Adds a comment to the column. If the `IF EXISTS` clause is specified, the query won’t return an error if the column does not exist.

Each column can have one comment. If a comment already exists for the column, a new comment overwrites the previous comment.

Comments are stored in the `comment_expression` column returned by the [DESCRIBE TABLE](../../../sql-reference/statements/misc.md#misc-describe-table) query.

Example:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

## MODIFY COLUMN

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [codec] [TTL] [AFTER name_after | FIRST]
ALTER COLUMN [IF EXISTS] name TYPE [type] [default_expr] [codec] [TTL] [AFTER name_after | FIRST]
```

This query changes the `name` column properties:

-   Type

-   Default expression

-   Compression Codec

-   TTL

For examples of columns compression CODECS modifying, see [Column Compression Codecs](../create/table.md#codecs).

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

## MODIFY COLUMN REMOVE

Removes one of the column properties: `DEFAULT`, `ALIAS`, `MATERIALIZED`, `CODEC`, `COMMENT`, `TTL`.

Syntax:

```sql
ALTER TABLE table_name MODIFY column_name REMOVE property;
```

**Example**

Remove TTL:

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**See Also**

- [REMOVE TTL](ttl.md).

## MATERIALIZE COLUMN

Materializes or updates a column with an expression for a default value (`DEFAULT` or `MATERIALIZED`).
It is used if it is necessary to add or update a column with a complicated expression, because evaluating such an expression directly on `SELECT` executing turns out to be expensive. 

Syntax:

```sql
ALTER TABLE table MATERIALIZE COLUMN col;
```

**Example**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM (select x,s from tmp order by x);

┌─groupArray(x)─┬─groupArray(s)─────────┐
│ [0,1,2,3,4]   │ ['0','1','2','3','4'] │
└───────────────┴───────────────────────┘

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(round(100/x));

INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5,5;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)──────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','20','17','14','12','11'] │
└───────────────────────┴────────────────────────────────────────────────┘

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['inf','100','50','33','25','20','17','14','12','11'] │
└───────────────────────┴───────────────────────────────────────────────────────┘
```

**See Also**

- [MATERIALIZED](../../statements/create/table.md#materialized).

## Limitations

The `ALTER` query lets you create and delete separate elements (columns) in nested data structures, but not whole nested data structures. To add a nested data structure, you can add columns with a name like `name.nested_name` and the type `Array(T)`. A nested data structure is equivalent to multiple array columns with a name that has the same prefix before the dot.

There is no support for deleting columns in the primary key or the sampling key (columns that are used in the `ENGINE` expression). Changing the type for columns that are included in the primary key is only possible if this change does not cause the data to be modified (for example, you are allowed to add values to an Enum or to change a type from `DateTime` to `UInt32`).

If the `ALTER` query is not sufficient to make the table changes you need, you can create a new table, copy the data to it using the [INSERT SELECT](../../../sql-reference/statements/insert-into.md#insert_query_insert-select) query, then switch the tables using the [RENAME](../../../sql-reference/statements/misc.md#misc_operations-rename) query and delete the old table. You can use the [clickhouse-copier](../../../operations/utilities/clickhouse-copier.md) as an alternative to the `INSERT SELECT` query.

The `ALTER` query blocks all reads and writes for the table. In other words, if a long `SELECT` is running at the time of the `ALTER` query, the `ALTER` query will wait for it to complete. At the same time, all new queries to the same table will wait while this `ALTER` is running.

For tables that do not store data themselves (such as [Merge](../../../sql-reference/statements/alter/index.md) and [Distributed](../../../sql-reference/statements/alter/index.md)), `ALTER` just changes the table structure, and does not change the structure of subordinate tables. For example, when running ALTER for a `Distributed` table, you will also need to run `ALTER` for the tables on all remote servers.
