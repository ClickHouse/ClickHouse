---
toc_priority: 37
toc_title: VIEW
---

# CREATE VIEW {#create-view}

Creates a new view. There are two types of views: normal and materialized.

## Normal {#normal}

Syntax:

``` sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER] AS SELECT ...
```

Normal views do not store any data. They just perform a read from another table on each access. In other words, a normal view is nothing more than a saved query. When reading from a view, this saved query is used as a subquery in the [FROM](../../../sql-reference/statements/select/from.md) clause.

As an example, assume you’ve created a view:

``` sql
CREATE VIEW view AS SELECT ...
```

and written a query:

``` sql
SELECT a, b, c FROM view
```

This query is fully equivalent to using the subquery:

``` sql
SELECT a, b, c FROM (SELECT ...)
```

## Materialized {#materialized}

``` sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER] [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

Materialized views store data transformed by the corresponding [SELECT](../../../sql-reference/statements/select/index.md) query.

When creating a materialized view without `TO [db].[table]`, you must specify `ENGINE` – the table engine for storing data.

When creating a materialized view with `TO [db].[table]`, you must not use `POPULATE`.

A materialized view is implemented as follows: when inserting data to the table specified in `SELECT`, part of the inserted data is converted by this `SELECT` query, and the result is inserted in the view.

!!! important "Important"
    Materialized views in ClickHouse are implemented more like insert triggers. If there’s some aggregation in the view query, it’s applied only to the batch of freshly inserted data. Any changes to existing data of source table (like update, delete, drop partition, etc.) does not change the materialized view.

If you specify `POPULATE`, the existing table data is inserted in the view when creating it, as if making a `CREATE TABLE ... AS SELECT ...` . Otherwise, the query contains only the data inserted in the table after creating the view. We **do not recommend** using POPULATE, since data inserted in the table during the view creation will not be inserted in it.

A `SELECT` query can contain `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` is set, data is aggregated during insertion, but only within a single packet of inserted data. The data won’t be further aggregated. The exception is when using an `ENGINE` that independently performs data aggregation, such as `SummingMergeTree`.

The execution of [ALTER](../../../sql-reference/statements/alter/index.md) queries on materialized views has limitations, so they might be inconvenient. If the materialized view uses the construction `TO [db.]name`, you can `DETACH` the view, run `ALTER` for the target table, and then `ATTACH` the previously detached (`DETACH`) view.

Note that materialized view is influenced by [optimize_on_insert](../../../operations/settings/settings.md#optimize-on-insert) setting. The data is merged before the insertion into a view.

Views look the same as normal tables. For example, they are listed in the result of the `SHOW TABLES` query.

To delete a view, use [DROP VIEW](../../../sql-reference/statements/drop.md#drop-view). Although `DROP TABLE` works for VIEWs as well.

## Live View (Experimental) {#live-view}

!!! important "Important"
    This is an experimental feature that may change in backwards-incompatible ways in the future releases.
    Enable usage of live views and `WATCH` query using [allow_experimental_live_view](../../../operations/settings/settings.md#allow-experimental-live-view) setting. Input the command `set allow_experimental_live_view = 1`.


```sql
CREATE LIVE VIEW [IF NOT EXISTS] [db.]table_name [WITH [TIMEOUT [value_in_sec] [AND]] [REFRESH [value_in_sec]]] AS SELECT ...
```

Live views store result of the corresponding [SELECT](../../../sql-reference/statements/select/index.md) query and are updated any time the result of the query changes. Query result as well as partial result needed to combine with new data are stored in memory providing increased performance for repeated queries. Live views can provide push notifications when query result changes using the [WATCH](../../../sql-reference/statements/watch.md) query.

Live views are triggered by insert into the innermost table specified in the query. 

Live views work similarly to how a query in a distributed table works. But instead of combining partial results from different servers they combine partial result from current data with partial result from the new data. When a live view query includes a subquery then the cached partial result is only stored for the innermost subquery.

!!! info "Limitations"
    - [Table function](../../../sql-reference/table-functions/index.md) is not supported as the innermost table.
    - Tables that do not have inserts such as a [dictionary](../../../sql-reference/dictionaries/index.md), [system table](../../../operations/system-tables/index.md), a [normal view](#normal), or a [materialized view](#materialized) will not trigger a live view.
    - Only queries where one can combine partial result from the old data plus partial result from the new data will work. Live view will not work for queries that require the complete data set to compute the final result or aggregations where the state of the aggregation must be preserved.
    - Does not work with replicated or distributed tables where inserts are performed on different nodes.
    - Can't be triggered by multiple tables.

    See [WITH REFRESH](#live-view-with-refresh) to force periodic updates of a live view that in some cases can be used as a workaround.

### Monitoring Changes {#live-view-monitoring}

You can monitor changes in the `LIVE VIEW` query result using [WATCH](../../../sql-reference/statements/watch.md) query.

```sql
WATCH [db.]live_view
```

**Example:**

```sql
CREATE TABLE mt (x Int8) Engine = MergeTree ORDER BY x;
CREATE LIVE VIEW lv AS SELECT sum(x) FROM mt;
```
Watch a live view while doing a parallel insert into the source table.

```sql
WATCH lv;
```

```bash
┌─sum(x)─┬─_version─┐
│      1 │        1 │
└────────┴──────────┘
┌─sum(x)─┬─_version─┐
│      2 │        2 │
└────────┴──────────┘
┌─sum(x)─┬─_version─┐
│      6 │        3 │
└────────┴──────────┘
...
```

```sql
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);
INSERT INTO mt VALUES (3);
```

Or add [EVENTS](../../../sql-reference/statements/watch.md#events-clause) clause to just get change events.

```sql
WATCH [db.]live_view EVENTS;
```

**Example:**

```sql
WATCH lv EVENTS;
```

```bash
┌─version─┐
│       1 │
└─────────┘
┌─version─┐
│       2 │
└─────────┘
┌─version─┐
│       3 │
└─────────┘
...
```

You can execute [SELECT](../../../sql-reference/statements/select/index.md) query on a live view in the same way as for any regular view or a table. If the query result is cached it will return the result immediately without running the stored query on the underlying tables.

```sql
SELECT * FROM [db.]live_view WHERE ...
```

### Force Refresh {#live-view-alter-refresh}

You can force live view refresh using the `ALTER LIVE VIEW [db.]table_name REFRESH` statement.

### WITH TIMEOUT Clause {#live-view-with-timeout}

When a live view is created with a `WITH TIMEOUT` clause then the live view will be dropped automatically after the specified number of seconds elapse since the end of the last [WATCH](../../../sql-reference/statements/watch.md) query that was watching the live view. 

```sql
CREATE LIVE VIEW [db.]table_name WITH TIMEOUT [value_in_sec] AS SELECT ...
```

If the timeout value is not specified then the value specified by the [temporary_live_view_timeout](../../../operations/settings/settings.md#temporary-live-view-timeout) setting is used.

**Example:**

```sql
CREATE TABLE mt (x Int8) Engine = MergeTree ORDER BY x;
CREATE LIVE VIEW lv WITH TIMEOUT 15 AS SELECT sum(x) FROM mt;
```

### WITH REFRESH Clause {#live-view-with-refresh}

When a live view is created with a `WITH REFRESH` clause then it will be automatically refreshed after the specified number of seconds elapse since the last refresh or trigger.

```sql
CREATE LIVE VIEW [db.]table_name WITH REFRESH [value_in_sec] AS SELECT ...
```

If the refresh value is not specified then the value specified by the [periodic_live_view_refresh](../../../operations/settings/settings.md#periodic-live-view-refresh) setting is used.

**Example:**

```sql
CREATE LIVE VIEW lv WITH REFRESH 5 AS SELECT now();
WATCH lv
```

```bash
┌───────────────now()─┬─_version─┐
│ 2021-02-21 08:47:05 │        1 │
└─────────────────────┴──────────┘
┌───────────────now()─┬─_version─┐
│ 2021-02-21 08:47:10 │        2 │
└─────────────────────┴──────────┘
┌───────────────now()─┬─_version─┐
│ 2021-02-21 08:47:15 │        3 │
└─────────────────────┴──────────┘
```

You can combine `WITH TIMEOUT` and `WITH REFRESH` clauses using an `AND` clause. 

```sql
CREATE LIVE VIEW [db.]table_name WITH TIMEOUT [value_in_sec] AND REFRESH [value_in_sec] AS SELECT ...
```

**Example:**

```sql
CREATE LIVE VIEW lv WITH TIMEOUT 15 AND REFRESH 5 AS SELECT now();
```

After 15 sec the live view will be automatically dropped if there are no active `WATCH` queries.

```sql
WATCH lv
```

```
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.lv does not exist.. 
```

### Usage {#live-view-usage}

Most common uses of live view tables include:

- Providing push notifications for query result changes to avoid polling.
- Caching results of most frequent queries to provide immediate query results.
- Watching for table changes and triggering a follow-up select queries.
- Watching metrics from system tables using periodic refresh.

[Original article](https://clickhouse.tech/docs/en/sql-reference/statements/create/view/) <!--hide-->
