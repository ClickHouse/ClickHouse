---
sidebar_position: 37
sidebar_label: VIEW
---

# CREATE VIEW

Creates a new view. Views can be [normal](#normal), [materialized](#materialized), [live](#live-view), and [window](#window-view) (live view and window view are experimental features).

## Normal View

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

## Materialized View

``` sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER] [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

Materialized views store data transformed by the corresponding [SELECT](../../../sql-reference/statements/select/index.md) query.

When creating a materialized view without `TO [db].[table]`, you must specify `ENGINE` – the table engine for storing data.

When creating a materialized view with `TO [db].[table]`, you must not use `POPULATE`.

A materialized view is implemented as follows: when inserting data to the table specified in `SELECT`, part of the inserted data is converted by this `SELECT` query, and the result is inserted in the view.

:::note    
Materialized views in ClickHouse use **column names** instead of column order during insertion into destination table. If some column names are not present in the `SELECT` query result, ClickHouse uses a default value, even if the column is not [Nullable](../../data-types/nullable.md). A safe practice would be to add aliases for every column when using Materialized views.

Materialized views in ClickHouse are implemented more like insert triggers. If there’s some aggregation in the view query, it’s applied only to the batch of freshly inserted data. Any changes to existing data of source table (like update, delete, drop partition, etc.) does not change the materialized view.
:::

If you specify `POPULATE`, the existing table data is inserted into the view when creating it, as if making a `CREATE TABLE ... AS SELECT ...` . Otherwise, the query contains only the data inserted in the table after creating the view. We **do not recommend** using `POPULATE`, since data inserted in the table during the view creation will not be inserted in it.

A `SELECT` query can contain `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`. Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` is set, data is aggregated during insertion, but only within a single packet of inserted data. The data won’t be further aggregated. The exception is when using an `ENGINE` that independently performs data aggregation, such as `SummingMergeTree`.

The execution of [ALTER](../../../sql-reference/statements/alter/view.md) queries on materialized views has limitations, so they might be inconvenient. If the materialized view uses the construction `TO [db.]name`, you can `DETACH` the view, run `ALTER` for the target table, and then `ATTACH` the previously detached (`DETACH`) view.

Note that materialized view is influenced by [optimize_on_insert](../../../operations/settings/settings.md#optimize-on-insert) setting. The data is merged before the insertion into a view.

Views look the same as normal tables. For example, they are listed in the result of the `SHOW TABLES` query.

To delete a view, use [DROP VIEW](../../../sql-reference/statements/drop.md#drop-view). Although `DROP TABLE` works for VIEWs as well.

## Live View [Experimental]

:::note    
This is an experimental feature that may change in backwards-incompatible ways in the future releases. Enable usage of live views and `WATCH` query using [allow_experimental_live_view](../../../operations/settings/settings.md#allow-experimental-live-view) setting. Input the command `set allow_experimental_live_view = 1`.
:::

```sql
CREATE LIVE VIEW [IF NOT EXISTS] [db.]table_name [WITH [TIMEOUT [value_in_sec] [AND]] [REFRESH [value_in_sec]]] AS SELECT ...
```

Live views store result of the corresponding [SELECT](../../../sql-reference/statements/select/index.md) query and are updated any time the result of the query changes. Query result as well as partial result needed to combine with new data are stored in memory providing increased performance for repeated queries. Live views can provide push notifications when query result changes using the [WATCH](../../../sql-reference/statements/watch.md) query.

Live views are triggered by insert into the innermost table specified in the query.

Live views work similarly to how a query in a distributed table works. But instead of combining partial results from different servers they combine partial result from current data with partial result from the new data. When a live view query includes a subquery then the cached partial result is only stored for the innermost subquery.

:::info    
- [Table function](../../../sql-reference/table-functions/index.md) is not supported as the innermost table.
- Tables that do not have inserts such as a [dictionary](../../../sql-reference/dictionaries/index.md), [system table](../../../operations/system-tables/index.md), a [normal view](#normal), or a [materialized view](#materialized) will not trigger a live view.
- Only queries where one can combine partial result from the old data plus partial result from the new data will work. Live view will not work for queries that require the complete data set to compute the final result or aggregations where the state of the aggregation must be preserved.
- Does not work with replicated or distributed tables where inserts are performed on different nodes.
- Can't be triggered by multiple tables.

See [WITH REFRESH](#live-view-with-refresh) to force periodic updates of a live view that in some cases can be used as a workaround.
:::

### Monitoring Live View Changes

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
│      3 │        2 │
└────────┴──────────┘
┌─sum(x)─┬─_version─┐
│      6 │        3 │
└────────┴──────────┘
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
```

You can execute [SELECT](../../../sql-reference/statements/select/index.md) query on a live view in the same way as for any regular view or a table. If the query result is cached it will return the result immediately without running the stored query on the underlying tables.

```sql
SELECT * FROM [db.]live_view WHERE ...
```

### Force Live View Refresh

You can force live view refresh using the `ALTER LIVE VIEW [db.]table_name REFRESH` statement.

### WITH TIMEOUT Clause

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

### WITH REFRESH Clause

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

### Live View Usage

Most common uses of live view tables include:

- Providing push notifications for query result changes to avoid polling.
- Caching results of most frequent queries to provide immediate query results.
- Watching for table changes and triggering a follow-up select queries.
- Watching metrics from system tables using periodic refresh.

**See Also**
-   [ALTER LIVE VIEW](../alter/view.md#alter-live-view)

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
