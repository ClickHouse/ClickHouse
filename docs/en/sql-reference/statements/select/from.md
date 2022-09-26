---
sidebar_label: FROM
---

# FROM Clause

The `FROM` clause specifies the source to read data from:

-   [Table](../../../engines/table-engines/index.md)
-   [Subquery](../../../sql-reference/statements/select/index.md) 
-   [Table function](../../../sql-reference/table-functions/index.md#table-functions)

[JOIN](../../../sql-reference/statements/select/join.md) and [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) clauses may also be used to extend the functionality of the `FROM` clause.

Subquery is another `SELECT` query that may be specified in parenthesis inside `FROM` clause.

`FROM` clause can contain multiple data sources, separated by commas, which is equivalent of performing [CROSS JOIN](../../../sql-reference/statements/select/join.md) on them.

## FINAL Modifier

When `FINAL` is specified, ClickHouse fully merges the data before returning the result and thus performs all data transformations that happen during merges for the given table engine.

It is applicable when selecting data from tables that use the [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)-engine family. Also supported for:

-   [Replicated](../../../engines/table-engines/mergetree-family/replication.md) versions of `MergeTree` engines.
-   [View](../../../engines/table-engines/special/view.md), [Buffer](../../../engines/table-engines/special/buffer.md), [Distributed](../../../engines/table-engines/special/distributed.md), and [MaterializedView](../../../engines/table-engines/special/materializedview.md) engines that operate over other engines, provided they were created over `MergeTree`-engine tables.

Now `SELECT` queries with `FINAL` are executed in parallel and slightly faster. But there are drawbacks (see below). The [max_final_threads](../../../operations/settings/settings.md#max-final-threads) setting limits the number of threads used.

### Drawbacks

Queries that use `FINAL` are executed slightly slower than similar queries that do not, because:

-   Data is merged during query execution.
-   Queries with `FINAL` read primary key columns in addition to the columns specified in the query.

**In most cases, avoid using `FINAL`.** The common approach is to use different queries that assume the background processes of the `MergeTree` engine have’t happened yet and deal with it by applying aggregation (for example, to discard duplicates).

## Implementation Details

If the `FROM` clause is omitted, data will be read from the `system.one` table.
The `system.one` table contains exactly one row (this table fulfills the same purpose as the DUAL table found in other DBMSs).

To execute a query, all the columns listed in the query are extracted from the appropriate table. Any columns not needed for the external query are thrown out of the subqueries.
If a query does not list any columns (for example, `SELECT count() FROM t`), some column is extracted from the table anyway (the smallest one is preferred), in order to calculate the number of rows.
