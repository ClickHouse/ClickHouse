---
toc_priority: 43
toc_title: DETACH
---

# DETACH Statement {#detach}

Makes the server "forget" about the existence of a table, a materialized view, or a dictionary.

**Syntax**

``` sql
DETACH TABLE|VIEW|DICTIONARY [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY]
```

Detaching does not delete the data or metadata of a table, a materialized view or a dictionary. If an entity was not detached `PERMANENTLY`, on the next server launch the server will read the metadata and recall the table/view/dictionary again. If an entity was detached `PERMANENTLY`, there will be no automatic recall.

Whether a table or a dictionary was detached permanently or not, in both cases you can reattach them using the [ATTACH](../../sql-reference/statements/attach.md) query.
System log tables can be also attached back (e.g. `query_log`, `text_log`, etc). Other system tables can't be reattached. On the next server launch the server will recall those tables again.

`ATTACH MATERIALIZED VIEW` does not work with short syntax (without `SELECT`), but you can attach it using the `ATTACH TABLE` query.

Note that you can not detach permanently the table which is already detached (temporary). But you can attach it back and then detach permanently again.

Also you can not [DROP](../../sql-reference/statements/drop.md#drop-table) the detached table, or [CREATE TABLE](../../sql-reference/statements/create/table.md) with the same name as detached permanently, or replace it with the other table with [RENAME TABLE](../../sql-reference/statements/rename.md) query.

**Example**

Creating a table:

Query:

``` sql
CREATE TABLE test ENGINE = Log AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

Result:

``` text
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

Detaching the table:

Query:

``` sql
DETACH TABLE test;
SELECT * FROM test;
```

Result:

``` text
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

**See Also**

-   [Materialized View](../../sql-reference/statements/create/view.md#materialized)
-   [Dictionaries](../../sql-reference/dictionaries/index.md)
