---
slug: /en/sql-reference/statements/create/view
sidebar_position: 37
sidebar_label: VIEW
---

# CREATE VIEW

Creates a new view. Views can be [normal](#normal-view), [materialized](#materialized-view), and [live](#live-view-deprecated) (live view is an experimental feature).

## Normal View

Syntax:

``` sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] 
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }] 
AS SELECT ...
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

## Parameterized View

Parametrized views are similar to normal views, but can be created with parameters which are not resolved immediately. These views can be used with table functions, which specify the name of the view as function name and the parameter values as its arguments.

``` sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```
The above creates a view for table which can be used as table function by substituting parameters as shown below.

``` sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

## Materialized View

``` sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER] [TO[db.]name] [ENGINE = engine] [POPULATE] 
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }] 
AS SELECT ...
```

:::tip
Here is a step by step guide on using [Materialized views](docs/en/guides/developer/cascading-materialized-views.md).
:::

Materialized views store data transformed by the corresponding [SELECT](../../../sql-reference/statements/select/index.md) query.

When creating a materialized view without `TO [db].[table]`, you must specify `ENGINE` – the table engine for storing data.

When creating a materialized view with `TO [db].[table]`, you can't also use `POPULATE`.

A materialized view is implemented as follows: when inserting data to the table specified in `SELECT`, part of the inserted data is converted by this `SELECT` query, and the result is inserted in the view.

:::note
Materialized views in ClickHouse use **column names** instead of column order during insertion into destination table. If some column names are not present in the `SELECT` query result, ClickHouse uses a default value, even if the column is not [Nullable](../../data-types/nullable.md). A safe practice would be to add aliases for every column when using Materialized views.

Materialized views in ClickHouse are implemented more like insert triggers. If there’s some aggregation in the view query, it’s applied only to the batch of freshly inserted data. Any changes to existing data of source table (like update, delete, drop partition, etc.) does not change the materialized view.

Materialized views in ClickHouse do not have deterministic behaviour in case of errors. This means that blocks that had been already written will be preserved in the destination table, but all blocks after error will not.

By default if pushing to one of views fails, then the INSERT query will fail too, and some blocks may not be written to the destination table. This can be changed using `materialized_views_ignore_errors` setting (you should set it for `INSERT` query), if you will set `materialized_views_ignore_errors=true`, then any errors while pushing to views will be ignored and all blocks will be written to the destination table.

Also note, that `materialized_views_ignore_errors` set to `true` by default for `system.*_log` tables.
:::

If you specify `POPULATE`, the existing table data is inserted into the view when creating it, as if making a `CREATE TABLE ... AS SELECT ...` . Otherwise, the query contains only the data inserted in the table after creating the view. We **do not recommend** using `POPULATE`, since data inserted in the table during the view creation will not be inserted in it.

:::note
Given that `POPULATE` works like `CREATE TABLE ... AS SELECT ...` it has limitations:
- It is not supported with Replicated database
- It is not supported in ClickHouse cloud

Instead a separate `INSERT ... SELECT` can be used.  
:::

A `SELECT` query can contain `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`. Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` is set, data is aggregated during insertion, but only within a single packet of inserted data. The data won’t be further aggregated. The exception is when using an `ENGINE` that independently performs data aggregation, such as `SummingMergeTree`.

The execution of [ALTER](/docs/en/sql-reference/statements/alter/view.md) queries on materialized views has limitations, for example, you can not update the `SELECT` query, so this might be inconvenient. If the materialized view uses the construction `TO [db.]name`, you can `DETACH` the view, run `ALTER` for the target table, and then `ATTACH` the previously detached (`DETACH`) view.

Note that materialized view is influenced by [optimize_on_insert](../../../operations/settings/settings.md#optimize-on-insert) setting. The data is merged before the insertion into a view.

Views look the same as normal tables. For example, they are listed in the result of the `SHOW TABLES` query.

To delete a view, use [DROP VIEW](../../../sql-reference/statements/drop.md#drop-view). Although `DROP TABLE` works for VIEWs as well.

## SQL security {#sql_security}

`DEFINER` and `SQL SECURITY` allow you to specify which ClickHouse user to use when executing the view's underlying query.
`SQL SECURITY` has three legal values: `DEFINER`, `INVOKER`, or `NONE`. You can specify any existing user or `CURRENT_USER` in the `DEFINER` clause.

The following table will explain which rights are required for which user in order to select from view. 
Note that regardless of the SQL security option, in every case it is still required to have `GRANT SELECT ON <view>` in order to read from it.

| SQL security option | View                                                            | Materialized View                                                                                                 |
|---------------------|-----------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `DEFINER alice`     | `alice` must have a `SELECT` grant for the view's source table. | `alice` must have a `SELECT` grant for the view's source table and an `INSERT` grant for the view's target table. |
| `INVOKER`           | User must have a `SELECT` grant for the view's source table.    | `SQL SECURITY INVOKER` can't be specified for materialized views.                                                 |
| `NONE`              | -                                                               | -                                                                                                                 |

:::note
`SQL SECURITY NONE` is a deprecated option. Any user with the rights to create views with `SQL SECURITY NONE` will be able to execute any arbitrary query.
Thus, it is required to have `GRANT ALLOW SQL SECURITY NONE TO <user>` in order to create a view with this option.
:::

If `DEFINER`/`SQL SECURITY` aren't specified, the default values are used:
- `SQL SECURITY`: `INVOKER` for normal views and `DEFINER` for materialized views ([configurable by settings](../../../operations/settings/settings.md#default_normal_view_sql_security))
- `DEFINER`: `CURRENT_USER` ([configurable by settings](../../../operations/settings/settings.md#default_view_definer))

If a view is attached without `DEFINER`/`SQL SECURITY` specified, the default value is `SQL SECURITY NONE` for the materialized view and `SQL SECURITY INVOKER` for the normal view.

To change SQL security for an existing view, use 
```sql
ALTER TABLE MODIFY SQL SECURITY { DEFINER | INVOKER | NONE } [DEFINER = { user | CURRENT_USER }]
```

### Examples sql security
```sql
CREATE test_view
DEFINER = alice SQL SECURITY DEFINER
AS SELECT ...
```

```sql
CREATE test_view
SQL SECURITY INVOKER
AS SELECT ...
```

## Live View [Deprecated]

This feature is deprecated and will be removed in the future.

For your convenience, the old documentation is located [here](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)

## Refreshable Materialized View [Experimental] {#refreshable-materialized-view}

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name
REFRESH EVERY|AFTER interval [OFFSET interval]
RANDOMIZE FOR interval
DEPENDS ON [db.]name [, [db.]name [, ...]]
[TO[db.]name] [(columns)] [ENGINE = engine] [EMPTY]
AS SELECT ...
```
where `interval` is a sequence of simple intervals:
```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

Periodically runs the corresponding query and stores its result in a table, atomically replacing the table's previous contents.

Differences from regular non-refreshable materialized views:
 * No insert trigger. I.e. when new data is inserted into the table specified in SELECT, it's *not* automatically pushed to the refreshable materialized view. The periodic refresh runs the entire query and replaces the entire table.
 * No restrictions on the SELECT query. Table functions (e.g. `url()`), views, UNION, JOIN, are all allowed.

:::note
Refreshable materialized views are a work in progress. Setting `allow_experimental_refreshable_materialized_view = 1` is required for creating one. Current limitations:
 * not compatible with Replicated database or table engines
 * It is not supported in ClickHouse Cloud
 * require [Atomic database engine](../../../engines/database-engines/atomic.md),
 * no retries for failed refresh - we just skip to the next scheduled refresh time,
 * no limit on number of concurrent refreshes.
:::

### Refresh Schedule

Example refresh schedules:
```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax errror, OFFSET is not allowed with AFTER
```

`RANDOMIZE FOR` randomly adjusts the time of each refresh, e.g.:
```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30
```

At most one refresh may be running at a time, for a given view. E.g. if a view with `REFRESH EVERY 1 MINUTE` takes 2 minutes to refresh, it'll just be refreshing every 2 minutes. If it then becomes faster and starts refreshing in 10 seconds, it'll go back to refreshing every minute. (In particular, it won't refresh every 10 seconds to catch up with a backlog of missed refreshes - there's no such backlog.)

Additionally, a refresh is started immediately after the materialized view is created, unless `EMPTY` is specified in the `CREATE` query. If `EMPTY` is specified, the first refresh happens according to schedule.

### Dependencies {#refresh-dependencies}

`DEPENDS ON` synchronizes refreshes of different tables. By way of example, suppose there's a chain of two refreshable materialized views:
```sql
CREATE MATERIALIZED VIEW source REFRESH EVERY 1 DAY AS SELECT * FROM url(...)
CREATE MATERIALIZED VIEW destination REFRESH EVERY 1 DAY AS SELECT ... FROM source
```
Without `DEPENDS ON`, both views will start a refresh at midnight, and `destination` typically will see yesterday's data in `source`. If we add dependency:
```
CREATE MATERIALIZED VIEW destination REFRESH EVERY 1 DAY DEPENDS ON source AS SELECT ... FROM source
```
then `destination`'s refresh will start only after `source`'s refresh finished for that day, so `destination` will be based on fresh data.

Alternatively, the same result can be achieved with:
```
CREATE MATERIALIZED VIEW destination REFRESH AFTER 1 HOUR DEPENDS ON source AS SELECT ... FROM source
```
where `1 HOUR` can be any duration less than `source`'s refresh period. The dependent table won't be refreshed more frequently than any of its dependencies. This is a valid way to set up a chain of refreshable views without specifying the real refresh period more than once.

A few more examples:
 * `REFRESH EVERY 1 DAY OFFSET 10 MINUTE` (`destination`) depends on `REFRESH EVERY 1 DAY` (`source`)<br/>
   If `source` refresh takes more than 10 minutes, `destination` will wait for it.
 * `REFRESH EVERY 1 DAY OFFSET 1 HOUR` depends on `REFRESH EVERY 1 DAY OFFSET 23 HOUR`<br/>
   Similar to the above, even though the corresponding refreshes happen on different calendar days.
   `destination`'s refresh on day X+1 will wait for `source`'s refresh on day X (if it takes more than 2 hours).
 * `REFRESH EVERY 2 HOUR` depends on `REFRESH EVERY 1 HOUR`<br/>
   The 2 HOUR refresh happens after the 1 HOUR refresh for every other hour, e.g. after the midnight
   refresh, then after the 2am refresh, etc.
 * `REFRESH EVERY 1 MINUTE` depends on `REFRESH EVERY 2 HOUR`<br/>
   `REFRESH AFTER 1 MINUTE` depends on `REFRESH EVERY 2 HOUR`<br/>
   `REFRESH AFTER 1 MINUTE` depends on `REFRESH AFTER 2 HOUR`<br/>
   `destination` is refreshed once after every `source` refresh, i.e. every 2 hours. The `1 MINUTE` is effectively ignored.
 * `REFRESH AFTER 1 HOUR` depends on `REFRESH AFTER 1 HOUR`<br/>
   Currently this is not recommended.

:::note
`DEPENDS ON` only works between refreshable materialized views. Listing a regular table in the `DEPENDS ON` list will prevent the view from ever refreshing (dependencies can be removed with `ALTER`, see below).
:::

### Changing Refresh Parameters {#changing-refresh-parameters}

To change refresh parameters:
```
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...]
```

:::note
This replaces refresh schedule *and* dependencies. If the table had a `DEPENDS ON`, doing a `MODIFY REFRESH` without `DEPENDS ON` will remove the dependencies.
:::

### Other operations

The status of all refreshable materialized views is available in table [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md). In particular, it contains refresh progress (if running), last and next refresh time, exception message if a refresh failed.

To manually stop, start, trigger, or cancel refreshes use [`SYSTEM STOP|START|REFRESH|CANCEL VIEW`](../system.md#refreshable-materialized-views).

## Related Content

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
- Blog: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
