---
slug: /en/sql-reference/statements/select/from
sidebar_label: FROM
---

# FROM Clause

The `FROM` clause specifies the source to read data from:

- [Table](../../../engines/table-engines/index.md)
- [Subquery](../../../sql-reference/statements/select/index.md) 
- [Table function](../../../sql-reference/table-functions/index.md#table-functions)

[JOIN](../../../sql-reference/statements/select/join.md) and [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) clauses may also be used to extend the functionality of the `FROM` clause.

Subquery is another `SELECT` query that may be specified in parenthesis inside `FROM` clause.

The `FROM` can contain multiple data sources, separated by commas, which is equivalent of performing [CROSS JOIN](../../../sql-reference/statements/select/join.md) on them.

`FROM` can optionally appear before a `SELECT` clause. This is a ClickHouse-specific extension of standard SQL which makes `SELECT` statements easier to read. Example:

```sql
FROM table
SELECT *
```

## FINAL Modifier

When `FINAL` is specified, ClickHouse fully merges the data before returning the result. This also performs all data transformations that happen during merges for the given table engine.

It is applicable when selecting data from from tables using the following table engines:
- `ReplacingMergeTree`
- `SummingMergeTree`
- `AggregatingMergeTree`
- `CollapsingMergeTree`
- `VersionedCollapsingMergeTree`

`SELECT` queries with `FINAL` are executed in parallel. The [max_final_threads](../../../operations/settings/settings.md#max-final-threads) setting limits the number of threads used.

### Drawbacks

Queries that use `FINAL` execute slightly slower than similar queries that do not use `FINAL` because:

- Data is merged during query execution.
- Queries with `FINAL` may read primary key columns in addition to the columns specified in the query.

`FINAL` requires additional compute and memory resources because the processing that normally would occur at merge time must occur in memory at the time of the query. However, using FINAL is sometimes necessary in order to produce accurate results (as data may not yet be fully merged). It is less expensive than running `OPTIMIZE` to force a merge.

As an alternative to using `FINAL`, it is sometimes possible to use different queries that assume the background processes of the `MergeTree` engine have not yet occurred and deal with it by applying an aggregation (for example, to discard duplicates). If you need to use `FINAL` in your queries in order to get the required results, it is okay to do so but be aware of the additional processing required.

`FINAL` can be applied automatically using [FINAL](../../../operations/settings/settings.md#final) setting to all tables in a query using a session or a user profile.

### Example Usage

Using the `FINAL` keyword

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

Using `FINAL` as a query-level setting

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

Using `FINAL` as a session-level setting

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

## Implementation Details

If the `FROM` clause is omitted, data will be read from the `system.one` table.
The `system.one` table contains exactly one row (this table fulfills the same purpose as the DUAL table found in other DBMSs).

To execute a query, all the columns listed in the query are extracted from the appropriate table. Any columns not needed for the external query are thrown out of the subqueries.
If a query does not list any columns (for example, `SELECT count() FROM t`), some column is extracted from the table anyway (the smallest one is preferred), in order to calculate the number of rows.
