---
slug: /en/sql-reference/statements/create/view
sidebar_position: 37
sidebar_label: VIEW
---

# CREATE VIEW

Creates a new view. Views can be [normal](#normal-view), [materialized](#materialized-view), [live](#live-view-experimental), and [window](#window-view-experimental) (live view and window view are experimental features).

## Normal View

Syntax:

``` sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] AS SELECT ...
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
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER] [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
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

A `SELECT` query can contain `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`. Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` is set, data is aggregated during insertion, but only within a single packet of inserted data. The data won’t be further aggregated. The exception is when using an `ENGINE` that independently performs data aggregation, such as `SummingMergeTree`.

The execution of [ALTER](/docs/en/sql-reference/statements/alter/view.md) queries on materialized views has limitations, for example, you can not update the `SELECT` query, so this might be inconvenient. If the materialized view uses the construction `TO [db.]name`, you can `DETACH` the view, run `ALTER` for the target table, and then `ATTACH` the previously detached (`DETACH`) view.

Note that materialized view is influenced by [optimize_on_insert](../../../operations/settings/settings.md#optimize-on-insert) setting. The data is merged before the insertion into a view.

Views look the same as normal tables. For example, they are listed in the result of the `SHOW TABLES` query.

To delete a view, use [DROP VIEW](../../../sql-reference/statements/drop.md#drop-view). Although `DROP TABLE` works for VIEWs as well.

## Live View [Deprecated]

This feature is deprecated and will be removed in the future.

For your convenience, the old documentation is located [here](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)

## Refreshable Materialized View {#refreshable-materialized-view}

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
 * not compatible with Replicated database or table engines,
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

## Window View [Experimental]

:::info
This is an experimental feature that may change in backwards-incompatible ways in the future releases. Enable usage of window views and `WATCH` query using [allow_experimental_window_view](../../../operations/settings/settings.md#allow-experimental-window-view) setting. Input the command `set allow_experimental_window_view = 1`.
:::

``` sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE] AS SELECT ... GROUP BY time_window_function
```

Window view can aggregate data by time window and output the results when the window is ready to fire. It stores the partial aggregation results in an inner(or specified) table to reduce latency and can push the processing result to a specified table or push notifications using the WATCH query.

Creating a window view is similar to creating `MATERIALIZED VIEW`. Window view needs an inner storage engine to store intermediate data. The inner storage can be specified by using `INNER ENGINE` clause, the window view will use `AggregatingMergeTree` as the default inner engine.

When creating a window view without `TO [db].[table]`, you must specify `ENGINE` – the table engine for storing data.

### Time Window Functions

[Time window functions](../../functions/time-window-functions.md) are used to get the lower and upper window bound of records. The window view needs to be used with a time window function.

### TIME ATTRIBUTES

Window view supports **processing time** and **event time** process.

**Processing time** allows window view to produce results based on the local machine's time and is used by default. It is the most straightforward notion of time but does not provide determinism. The processing time attribute can be defined by setting the `time_attr` of the time window function to a table column or using the function `now()`. The following query creates a window view with processing time.

``` sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

**Event time** is the time that each individual event occurred on its producing device. This time is typically embedded within the records when it is generated. Event time processing allows for consistent results even in case of out-of-order events or late events. Window view supports event time processing by using `WATERMARK` syntax.

Window view provides three watermark strategies:

* `STRICTLY_ASCENDING`: Emits a watermark of the maximum observed timestamp so far. Rows that have a timestamp smaller to the max timestamp are not late.
* `ASCENDING`: Emits a watermark of the maximum observed timestamp so far minus 1. Rows that have a timestamp equal and smaller to the max timestamp are not late.
* `BOUNDED`: WATERMARK=INTERVAL. Emits watermarks, which are the maximum observed timestamp minus the specified delay.

The following queries are examples of creating a window view with `WATERMARK`:

``` sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

By default, the window will be fired when the watermark comes, and elements that arrived behind the watermark will be dropped. Window view supports late event processing by setting `ALLOWED_LATENESS=INTERVAL`. An example of lateness handling is:

``` sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

Note that elements emitted by a late firing should be treated as updated results of a previous computation. Instead of firing at the end of windows, the window view will fire immediately when the late event arrives. Thus, it will result in multiple outputs for the same window. Users need to take these duplicated results into account or deduplicate them.

You can modify `SELECT` query that was specified in the window view by using `ALTER TABLE … MODIFY QUERY` statement. The data structure resulting in a new `SELECT` query should be the same as the original `SELECT` query when with or without `TO [db.]name` clause. Note that the data in the current window will be lost because the intermediate state cannot be reused.

### Monitoring New Windows

Window view supports the [WATCH](../../../sql-reference/statements/watch.md) query to monitoring changes, or use `TO` syntax to output the results to a table.

``` sql
WATCH [db.]window_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

`WATCH` query acts similar as in `LIVE VIEW`. A `LIMIT` can be specified to set the number of updates to receive before terminating the query. The `EVENTS` clause can be used to obtain a short form of the `WATCH` query where instead of the query result you will just get the latest query watermark.

### Settings

- `window_view_clean_interval`: The clean interval of window view in seconds to free outdated data. The system will retain the windows that have not been fully triggered according to the system time or `WATERMARK` configuration, and the other data will be deleted.
- `window_view_heartbeat_interval`: The heartbeat interval in seconds to indicate the watch query is alive.
- `wait_for_window_view_fire_signal_timeout`: Timeout for waiting for window view fire signal in event time processing.

### Example

Suppose we need to count the number of click logs per 10 seconds in a log table called `data`, and its table structure is:

``` sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

First, we create a window view with tumble window of 10 seconds interval:

``` sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

Then, we use the `WATCH` query to get the results.

``` sql
WATCH wv
```

When logs are inserted into table `data`,

``` sql
INSERT INTO data VALUES(1,now())
```

The `WATCH` query should print the results as follows:

``` text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

Alternatively, we can attach the output to another table using `TO` syntax.

``` sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

Additional examples can be found among stateful tests of ClickHouse (they are named `*window_view*` there).

### Window View Usage

The window view is useful in the following scenarios:

* **Monitoring**: Aggregate and calculate the metrics logs by time, and output the results to a target table. The dashboard can use the target table as a source table.
* **Analyzing**: Automatically aggregate and preprocess data in the time window. This can be useful when analyzing a large number of logs. The preprocessing eliminates repeated calculations in multiple queries and reduces query latency.

## Related Content

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
- Blog: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
