---
toc_priority: 43
toc_title: DETACH
---

# DETACH Statement {#detach}

Deletes information about the table or materialized view from the server. The server stops knowing about their existence.

Syntax:

``` sql
DETACH TABLE|VIEW [IF EXISTS] [db.]name [PERMANENTLY] [ON CLUSTER cluster]
```

Detaching does not delete the data or metadata for the table or materialized view. If the table or view was not detached `PERMANENTLY`, on the next server launch the server will read the metadata and recall the table/view again. If the table or view was detached `PERMANENTLY`, there will be no automatic recall. 

Whether the table was detached permanently or not, in both cases you can reattach it using the [ATTACH](../../sql-reference/statements/attach.md). System log tables can be also attached back (e.g. `query_log`, `text_log`, etc). Other system tables can't be reattached. On the next server launch the server will recall those tables again.

`ATTACH MATERIALIZED VIEW` doesn't work with short syntax (without `SELECT`), but you can attach it using the `ATTACH TABLE` query.

Note that you can not detach permanently the table which is already detached (temporary). But you can attach it back and then detach permanently again. 

Also you can not [DROP](../../sql-reference/statements/drop.md#drop-table) the detached table, or [CREATE TABLE](../../sql-reference/statements/create/table.md) with the same name as detached permanently, or replace it with the other table with [RENAME TABLE](../../sql-reference/statements/rename.md) query.

**Example**

Creating a table:

``` sql
CREATE TABLE test ENGINE = Log AS SELECT * FROM numbers(10);
SELECT * FROM test;

[Original article](https://clickhouse.tech/docs/en/sql-reference/statements/detach/) <!--hide-->
