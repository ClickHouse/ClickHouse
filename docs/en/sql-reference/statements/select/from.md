# FROM Clause {#select-from}

If the FROM clause is omitted, data will be read from the `system.one` table.
The `system.one` table contains exactly one row (this table fulfills the same purpose as the DUAL table found in other DBMSs).

The `FROM` clause specifies the source to read data from:

-   Table
-   Subquery
-   [Table function](../../table-functions/index.md#table-functions)

`ARRAY JOIN` and the regular `JOIN` may also be included (see below).

Instead of a table, the `SELECT` subquery may be specified in parenthesis.
In contrast to standard SQL, a synonym does not need to be specified after a subquery.

To execute a query, all the columns listed in the query are extracted from the appropriate table. Any columns not needed for the external query are thrown out of the subqueries.
If a query does not list any columns (for example, `SELECT count() FROM t`), some column is extracted from the table anyway (the smallest one is preferred), in order to calculate the number of rows.

## FINAL Modifier {#select-from-final}

Applicable when selecting data from tables from the [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)-engine family other than `GraphiteMergeTree`. When `FINAL` is specified, ClickHouse fully merges the data before returning the result and thus performs all data transformations that happen during merges for the given table engine.

Also supported for:
- [Replicated](../../../engines/table-engines/mergetree-family/replication.md) versions of `MergeTree` engines.
- [View](../../../engines/table-engines/special/view.md), [Buffer](../../../engines/table-engines/special/buffer.md), [Distributed](../../../engines/table-engines/special/distributed.md), and [MaterializedView](../../../engines/table-engines/special/materializedview.md) engines that operate over other engines, provided they were created over `MergeTree`-engine tables.

Queries that use `FINAL` are executed not as fast as similar queries that don’t, because:

-   Query is executed in a single thread and data is merged during query execution.
-   Queries with `FINAL` read primary key columns in addition to the columns specified in the query.

In most cases, avoid using `FINAL`.
