---
toc_priority: 43
toc_title: DETACH
---

# DETACH Statement {#detach}

Deletes information about the table or view from the server. The server stops knowing about their existence.

Syntax:

``` sql
DETACH TABLE|VIEW [IF EXISTS] [db.]name [PERMANENTLY] [ON CLUSTER cluster]
```

Detaching does not delete the data or metadata for the table or view. If the table or view was not detached `PERMANENTLY`, on the next server launch the server will read the metadata and recall the table/view again. If the table or view was detached `PERMANENTLY`, there will be no automatic recall. 

Whether the table was detached permanently or not, in both cases you can reattach it using the [ATTACH](../../sql-reference/statements/attach.md) query (with the exception of system tables, which do not have metadata stored for them).

`ATTACH MATERIALIZED VIEW` doesn't work with short syntax (without `SELECT`), but you can attach it using the `ATTACH TABLE` query.

Note that you can not detach permanently the table which is already detached (temporary). But you can attach it back and then detach permanently again. 

Also you can not [DROP](../../sql-reference/statements/drop.md#drop-table) the detached table, or [CREATE TABLE](../../sql-reference/statements/create/table.md) with the same name as detached permanently, or replace it with the other table with [RENAME TABLE](../../sql-reference/statements/rename.md) query.

Similarly, a “detached” table can be re-attached using the [ATTACH](../../sql-reference/statements/attach.md) query (with the exception of system tables, which do not have metadata stored for them).

## DETACH PERMANENTLY {#detach-permanently}

Deletes information about `name` table or view from the server. Permanently detached tables won't automatically reappear after the server restart.

Syntax:

``` sql
DETACH TABLE/VIEW [IF EXISTS] [db.]name PERMAMENTLY [ON CLUSTER cluster]
```

This statement does not delete the table’s data or metadata.

Permanently detached table or view can be reattached with [ATTACH](../../sql-reference/statements/attach.md) query and can be shown with [SHOW CREATE TABLE](../../sql-reference/statements/show.md#show-create-table) query.

[Original article](https://clickhouse.tech/docs/en/sql-reference/statements/detach/) <!--hide-->
