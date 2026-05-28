---
description: 'Documentation for CREATE VIEW'
sidebar_label: 'VIEW'
sidebar_position: 37
slug: /sql-reference/statements/create/view
title: 'CREATE VIEW'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import DeprecatedBadge from '@theme/badges/DeprecatedBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# CREATE VIEW

Creates a new view. Views can be [normal](#normal-view), [materialized](#materialized-view), [refreshable materialized](#refreshable-materialized-view), and [window](/sql-reference/statements/create/view#window-view).

## Normal View {#normal-view}

Syntax:

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [(alias1 [, alias2 ...])] [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

Normal views do not store any data. They just perform a read from another table on each access. In other words, a normal view is nothing more than a saved query. When reading from a view, this saved query is used as a subquery in the [FROM](../../../sql-reference/statements/select/from.md) clause.

As an example, assume you've created a view:

```sql
CREATE VIEW view AS SELECT ...
```

and written a query:

```sql
SELECT a, b, c FROM view
```

This query is fully equivalent to using the subquery:

```sql
SELECT a, b, c FROM (SELECT ...)
```

## Parameterized View {#parameterized-view}

Parameterized views are similar to normal views, but can be created with parameters which are not resolved immediately. These views can be used with table functions, which specify the name of the view as function name and the parameter values as its arguments.

```sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```
The above creates a view for table which can be used as table function by substituting parameters as shown below.

```sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

## Materialized View {#materialized-view}

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

`OR REPLACE` and `IF NOT EXISTS` are mutually exclusive: combining them is a syntax error.

### CREATE OR REPLACE MATERIALIZED VIEW {#create-or-replace-materialized-view}

`CREATE OR REPLACE MATERIALIZED VIEW` atomically replaces an existing materialized view and its inner storage table (if any). The operation requires an `Atomic` or `Replicated` database engine.

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]name [ON CLUSTER cluster]
[TO [db.]target_table]
[ENGINE = engine]
[POPULATE]
[REFRESH ...]
AS SELECT ...
```

Key behaviors:

- **Without `TO` clause**: the old inner table is dropped and a new one is created. Existing data in the inner table is lost unless `POPULATE` is specified.
- **With `TO` clause**: only the view definition is replaced; the target table and its data are unaffected.
- Compatible with `REFRESH`, `ON CLUSTER`, and all engine options. `POPULATE` is supported on `Atomic` databases only — it is rejected on `Replicated` databases (see the `POPULATE` note below).
- Requires `CREATE VIEW` and `DROP VIEW` privileges.

:::note
`CREATE OR REPLACE MATERIALIZED VIEW` is only supported with `Atomic` or `Replicated` database engines. It is not supported with the `Ordinary` database engine.
:::

**Examples:**

```sql
-- Create a materialized view with an inner table
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, sum(y) AS total FROM src GROUP BY x;

-- Replace with a new definition (old inner table data is lost)
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, count() AS cnt FROM src GROUP BY x;

-- Replace with POPULATE to backfill from existing source data
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    POPULATE
    AS SELECT x FROM src;

-- Replace an inner-table MV with a TO-table MV (target data is preserved)
CREATE OR REPLACE MATERIALIZED VIEW mv TO target
    AS SELECT x FROM src;
```

:::tip
Here is a step-by-step guide on using [Materialized views](/guides/developer/cascading-materialized-views.md).
:::

Materialized views store data transformed by the corresponding [SELECT](../../../sql-reference/statements/select/index.md) query.

When creating a materialized view without `TO [db].[table]`, you must specify `ENGINE` – the table engine for storing data.

When creating a materialized view with `TO [db].[table]`, you can't also use `POPULATE`.

A materialized view is implemented as follows: when inserting data to the table specified in `SELECT`, part of the inserted data is converted by this `SELECT` query, and the result is inserted in the view.

:::note
Materialized views in ClickHouse use **column names** instead of column order during insertion into destination table. If some column names are not present in the `SELECT` query result, ClickHouse uses a default value, even if the column is not [Nullable](../../data-types/nullable.md). A safe practice would be to add aliases for every column when using Materialized views.

Materialized views in ClickHouse are implemented more like insert triggers. If there's some aggregation in the view query, it's applied only to the batch of freshly inserted data. Any changes to existing data of source table (like update, delete, drop partition, etc.) does not change the materialized view.

Materialized views in ClickHouse do not have deterministic behaviour in case of errors. This means that blocks that had been already written will be preserved in the destination table, but all blocks after error will not.

By default, if pushing to one of the views throws, the `INSERT` query fails. Whether the block has already reached the source table by that point is not guaranteed — it depends on insert pipeline timing, not on the view error.

Setting `materialized_views_ignore_errors=true` on the `INSERT` query only changes error reporting: the view error is logged as a warning and the `INSERT` succeeds. The block still does not appear in that view's destination table, but the source table receives it as usual. To get exactly-once delivery to the views, retry the `INSERT` with insert deduplication (`insert_deduplicate`, `deduplicate_blocks_in_dependent_materialized_views`).

`materialized_views_ignore_errors` is `true` by default for `system.*_log` tables.
:::

If you specify `POPULATE`, the existing table data is inserted into the view when creating it, as if making a `CREATE TABLE ... AS SELECT ...` . Otherwise, the query contains only the data inserted in the table after creating the view. We **do not recommend** using `POPULATE`, since data inserted in the table during the view creation will not be inserted in it.

:::note
Given that `POPULATE` works like `CREATE TABLE ... AS SELECT ...` it has limitations:
- It is not supported with Replicated database
- It is not supported in ClickHouse cloud

Instead a separate `INSERT ... SELECT` can be used.
:::

A `SELECT` query can contain `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`. Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` is set, data is aggregated during insertion, but only within a single packet of inserted data. The data won't be further aggregated. The exception is when using an `ENGINE` that independently performs data aggregation, such as `SummingMergeTree`.

If the materialized view uses the construction `TO [db.]name`, you can `DETACH` the view, run `ALTER` for the target table, and then `ATTACH` the previously detached (`DETACH`) view.

Note that materialized view is influenced by [optimize_on_insert](/operations/settings/settings#optimize_on_insert) setting. The data is merged before the insertion into a view.

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

### Examples {#examples}
```sql
CREATE VIEW test_view
DEFINER = alice SQL SECURITY DEFINER
AS SELECT ...
```

```sql
CREATE VIEW test_view
SQL SECURITY INVOKER
AS SELECT ...
```

## Live View {#live-view}

<DeprecatedBadge/>

This feature is deprecated and will be removed in the future.

For your convenience, the old documentation is located [here](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)

## Refreshable Materialized View {#refreshable-materialized-view}

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
REFRESH [EVERY|AFTER interval [OFFSET interval]]
[RANDOMIZE FOR interval]
[DEPENDS ON [db.]name [, [db.]name [, ...]]]
[SETTINGS name = value [, name = value [, ...]]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine]
[EMPTY]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```
where `interval` is a sequence of simple intervals:
```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

The `REFRESH` clause must specify at least one of `EVERY`, `AFTER`, or `DEPENDS ON`. Bare `REFRESH` (with none of these) is rejected. `REFRESH DEPENDS ON ...` without `EVERY`/`AFTER` is shorthand for `REFRESH AFTER 0 SECOND DEPENDS ON ...`; see [Refresh Dependencies](#refresh-dependencies) below.

Periodically runs the corresponding query and stores its result into a table.
* If `APPEND` is specified, each refresh inserts rows into the table without deleting existing rows. The insert is not atomic, just like a regular `INSERT INTO ... SELECT` query.
* Otherwise, each refresh atomically replaces the table's previous contents.

Differences from regular non-refreshable materialized views:
* No insert trigger. When new data is inserted into the table specified in `SELECT`, it's *not* automatically pushed to the refreshable materialized view. Instead, data insertion only takes place during the periodic or manual refresh runs.
* No restrictions on the `SELECT` query. Table functions (e.g. `url()`), views, UNION, JOIN, are all allowed.

:::note
The settings in the `REFRESH ... SETTINGS` part of the query are refresh settings (e.g. `refresh_retries`), distinct from regular settings (e.g. `max_threads`). Regular settings can be specified using `SETTINGS` at the end of the query.
:::

### Refresh Schedule {#refresh-schedule}

Example refresh schedules:
```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax error, OFFSET is not allowed with AFTER
REFRESH EVERY 1 WEEK 2 DAYS -- every 9 days, not on any particular day of the week or month;
                            -- specifically, when day number (since 1969-12-29) is divisible by 9
REFRESH EVERY 5 MONTHS -- every 5 months, different months each year (as 12 is not divisible by 5);
                       -- specifically, when month number (since 1970-01) is divisible by 5
```

`RANDOMIZE FOR` randomly adjusts the time of each refresh, e.g.:
```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30
```

At most one refresh may be running at a time, for a given view. E.g. if a view with `REFRESH EVERY 1 MINUTE` takes 2 minutes to refresh, it'll just be refreshing every 2 minutes. If it then becomes faster and starts refreshing in 10 seconds, it'll go back to refreshing every minute. (In particular, it won't refresh every 10 seconds to catch up with a backlog of missed refreshes - there's no such backlog.)

Typically the first refresh is started immediately after the materialized view is created: time since last refresh is infinity, so any schedule says it's time to refresh now. If `EMPTY` is specified, this initial refresh is skipped, and the first refresh happens at the next scheduled time; e.g. for `EVERY 1 HOUR` the first refresh will happen at the end of current hour.

### In Replicated DB {#in-replicated-db}

If the refreshable materialized view is in a [Replicated database](../../../engines/database-engines/replicated.md), the replicas coordinate with each other such that only one replica performs the refresh at each scheduled time. [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) table engine is required, so that all replicas see the data produced by the refresh.

In `APPEND` mode, coordination can be disabled using `SETTINGS all_replicas = 1`. This makes replicas do refreshes independently of each other. In this case ReplicatedMergeTree is not required.

In non-`APPEND` mode, only coordinated refreshing is supported. For uncoordinated, use `Atomic` database and `CREATE ... ON CLUSTER` query to create refreshable materialized views on all replicas.

The coordination is done through Keeper. The znode path is determined by [default_replica_path](../../../operations/server-configuration-parameters/settings.md#default_replica_path) server setting.

### Refresh Dependencies {#refresh-dependencies}

`DEPENDS ON` synchronizes refreshes of different tables:
```sql
CREATE MATERIALIZED VIEW dependent REFRESH EVERY 1 HOUR DEPENDS ON dependency [...]
```
Dependent view's refresh will start only after all dependency views' refreshes complete.

To refresh immediately after another view's refresh:
```sql
CREATE MATERIALIZED VIEW dependent REFRESH AFTER 0 SECOND DEPENDS ON dependency [...]
```
Or equivalently:
```sql
CREATE MATERIALIZED VIEW dependent REFRESH DEPENDS ON dependency [...]
```

:::note
`DEPENDS ON` only works between refreshable materialized views. In particular, if the dependency view uses `TO <table>`, make sure to use the name of the view rather than the table. If the `DEPENDS ON` list contains a regular table or non-refreshable view or has a typo, the view will never refresh and will show state `MissingDependencies` in `system.view_refreshes`. Dependencies can be changed or removed using `ALTER`, see [Changing Refresh Parameters](#changing-refresh-parameters).
:::

#### Using DEPENDS ON for consistent propagation latency {#using-depends-on-for-consistent-propagation-latency}

If both views use `REFRESH EVERY` with the same period, the dependency applies in each timeslot.

E.g. suppose views X and Y both use `REFRESH EVERY 1 HOUR`, and Y reads from X's output table. Without dependencies, Y would usually see X's data from previous hour's refresh. With `DEPENDS ON X`, Y's 11:00 refresh will start only after the X's 11:00 refresh completes.

```text
           10:00            11:00            12:00
           │                │                │
  X:        [run]┐           [run]┐           [run]┐
                 │                │                │
  Y:             └►[run]          └►[run]          └►[run]
```

Both dependency and dependent may independently skip timeslots if refreshes run for longer than the refresh period. There's no guarantee that the dependent refreshes exactly once for each dependency refresh.

```text
           10:00          11:00          12:00          13:00
           │              │              │              |
  X:        [run]┐         [run]┐         [run]┐         [run]┐
                 │              └────┐    (Y skips 12:00)     └───┐
  Y:             └►[10:00 ru------un]└►[11:00 ru---------------un]└►[13:00 run]
```

#### Using DEPENDS ON for batched stream processing {#using-depends-on-for-batched-stream-processing}

If `REFRESH EVERY` is not used, the dependent view X refreshes if all its dependencies refreshed at least once since X's last refresh. `REFRESH AFTER T` adds a delay: the dependent will start refresh T time after the dependency completes a refresh.

Circular dependencies are allowed and useful. Consider this graph of refreshable materialized views:
 1. X takes a batch of rows from some stream and puts them in a table.
 2. Then Y and Z both read from that table, do different aggregation, and append results to other tables.
 3. After the batch is fully processed, X takes the next batch, and the cycle repeats.

```text
            source
               │
               ▼
          ┌─────────┐
     ┌───►│    X    │◄───┐
     │    └──┬───┬──┘    │
  DEPENDS    │   │    DEPENDS
    ON       ▼   ▼      ON
     │      ┌─┐ ┌─┐      │
     └──────┤Y│ │Z├──────┘
            └─┘ └─┘
```

Complete example:
```sql
CREATE TABLE current_batch (t UInt64, v Int64) ENGINE ReplicatedMergeTree ORDER BY t;
CREATE TABLE batch_log (max_t UInt64, n Int64, v_sum Int64, processed_at DateTime64) ENGINE ReplicatedMergeTree ORDER BY max_t;
CREATE TABLE stats (h UInt64, n UInt64) ENGINE ReplicatedSummingMergeTree ORDER BY h;

-- (system.numbers stands in for a data source with monotonically increasing timestamps or sequence numbers)
CREATE MATERIALIZED VIEW current_batch_v REFRESH EVERY 10 SECOND DEPENDS ON batch_log_v, stats_v TO current_batch AS SELECT number as t, number * 10 as v FROM system.numbers WHERE number > (SELECT max(max_t) FROM batch_log) LIMIT 100;

CREATE MATERIALIZED VIEW batch_log_v REFRESH DEPENDS ON current_batch_v APPEND TO batch_log AS SELECT max(t) as max_t, count() as n, sum(v) as v_sum, now64() as processed_at FROM current_batch;

CREATE MATERIALIZED VIEW stats_v REFRESH DEPENDS ON current_batch_v APPEND TO stats AS SELECT cityHash64(v) % 20 as h, count() as n FROM current_batch GROUP BY h;

-- Must trigger initial refresh manually.
SYSTEM REFRESH VIEW current_batch_v;
```

Longer chains work as well.

This only works well when refresh coordination is enabled, i.e. the views are in Replicated or Shared database. Without coordination, server restart breaks the cycle, requiring a manual `SYSTEM REFRESH VIEW` after each restart rather than once after creating the views.

### Refresh Settings {#refresh-settings}

Available refresh settings:
* `refresh_retries` - How many times to retry if refresh query fails with an exception. If all retries fail, skip to the next scheduled refresh time. 0 means no retries, -1 means infinite retries. Default: 2.
* `refresh_retry_initial_backoff_ms` - Delay before the first retry, if `refresh_retries` is not zero. Each subsequent retry doubles the delay, up to `refresh_retry_max_backoff_ms`. Default: 100 ms.
* `refresh_retry_max_backoff_ms` - Limit on the exponential growth of delay between refresh attempts. Default: 60000 ms (1 minute).
* `all_replicas` - In a [Replicated database](../../../engines/database-engines/replicated.md) with `APPEND`, controls whether all replicas refresh independently or only one replica refreshes at each scheduled time. Cannot be changed after the view is created. Default: `false`.

### Changing Refresh Parameters {#changing-refresh-parameters}

Refresh parameters of an existing refreshable materialized view are changed with [`ALTER TABLE ... MODIFY REFRESH`](../alter/view.md#alter-table--modify-refresh-statement):

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

The schedule (`EVERY` or `AFTER`) is mandatory: the statement always replaces *all* refresh parameters — schedule, `RANDOMIZE FOR`, `DEPENDS ON`, and refresh settings — with what is specified. Anything omitted is reset to its default (settings) or removed (dependencies, randomization).

:::note
- To change only refresh settings (e.g. `refresh_retries`), repeat the existing schedule:

  ```sql
  ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR SETTINGS refresh_retries = 5;
  ```

- `ALTER TABLE ... MODIFY SETTING refresh_retries = ...` is not supported on materialized views; you must go through `MODIFY REFRESH`.

- Adding or removing `APPEND` is not supported.

- The `all_replicas` setting cannot be changed after creation.
:::

Examples:

```sql
-- Change the schedule, drop existing settings and dependencies.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change the schedule and tune retry behavior.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Keep the dependency while changing the period.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

### Other operations {#other-operations}

The status of all refreshable materialized views is available in table [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md). In particular, it contains refresh progress (if running), last and next refresh time, exception message if a refresh failed.

To manually stop, start, trigger, or cancel refreshes, use [`SYSTEM STOP|START|REFRESH|WAIT|CANCEL VIEW`](../system.md#managing-refreshable-materialized-views).

To wait for a refresh to complete, use [`SYSTEM WAIT VIEW`](../system.md#wait-view). In particular, useful for waiting for initial refresh after creating a view.

:::note
Fun fact: the refresh query is allowed to read from the view that's being refreshed, seeing pre-refresh version of the data. This means you can implement Conway's game of life: https://pastila.nl/?00021a4b/d6156ff819c83d490ad2dcec05676865#O0LGWTO7maUQIA4AcGUtlA==
:::

## Window View {#window-view}

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

:::info
This is an experimental feature that may change in backwards-incompatible ways in the future releases. Enable usage of window views and `WATCH` query using [allow_experimental_window_view](/operations/settings/settings#allow_experimental_window_view) setting. Input the command `set allow_experimental_window_view = 1`.
:::

```sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE]
AS SELECT ...
GROUP BY time_window_function
[COMMENT 'comment']
```

Window view can aggregate data by time window and output the results when the window is ready to fire. It stores the partial aggregation results in an inner(or specified) table to reduce latency and can push the processing result to a specified table or push notifications using the WATCH query.

Creating a window view is similar to creating `MATERIALIZED VIEW`. Window view needs an inner storage engine to store intermediate data. The inner storage can be specified by using `INNER ENGINE` clause, the window view will use `AggregatingMergeTree` as the default inner engine.

When creating a window view without `TO [db].[table]`, you must specify `ENGINE` – the table engine for storing data.

### Time Window Functions {#time-window-functions}

[Time window functions](../../functions/time-window-functions.md) are used to get the lower and upper window bound of records. The window view needs to be used with a time window function.

### TIME ATTRIBUTES {#time-attributes}

Window view supports **processing time** and **event time** process.

**Processing time** allows window view to produce results based on the local machine's time and is used by default. It is the most straightforward notion of time but does not provide determinism. The processing time attribute can be defined by setting the `time_attr` of the time window function to a table column or using the function `now()`. The following query creates a window view with processing time.

```sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

**Event time** is the time that each individual event occurred on its producing device. This time is typically embedded within the records when it is generated. Event time processing allows for consistent results even in case of out-of-order events or late events. Window view supports event time processing by using `WATERMARK` syntax.

Window view provides three watermark strategies:

* `STRICTLY_ASCENDING`: Emits a watermark of the maximum observed timestamp so far. Rows that have a timestamp smaller to the max timestamp are not late.
* `ASCENDING`: Emits a watermark of the maximum observed timestamp so far minus 1. Rows that have a timestamp equal and smaller to the max timestamp are not late.
* `BOUNDED`: WATERMARK=INTERVAL. Emits watermarks, which are the maximum observed timestamp minus the specified delay.

The following queries are examples of creating a window view with `WATERMARK`:

```sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

By default, the window will be fired when the watermark comes, and elements that arrived behind the watermark will be dropped. Window view supports late event processing by setting `ALLOWED_LATENESS=INTERVAL`. An example of lateness handling is:

```sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

Note that elements emitted by a late firing should be treated as updated results of a previous computation. Instead of firing at the end of windows, the window view will fire immediately when the late event arrives. Thus, it will result in multiple outputs for the same window. Users need to take these duplicated results into account or deduplicate them.

You can modify `SELECT` query that was specified in the window view by using `ALTER TABLE ... MODIFY QUERY` statement. The data structure resulting in a new `SELECT` query should be the same as the original `SELECT` query when with or without `TO [db.]name` clause. Note that the data in the current window will be lost because the intermediate state cannot be reused.

### Monitoring New Windows {#monitoring-new-windows}

Window view supports the [WATCH](../../../sql-reference/statements/watch.md) query to monitoring changes, or use `TO` syntax to output the results to a table.

```sql
WATCH [db.]window_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

A `LIMIT` can be specified to set the number of updates to receive before terminating the query. The `EVENTS` clause can be used to obtain a short form of the `WATCH` query where instead of the query result you will just get the latest query watermark.

### Settings {#settings-1}

- `window_view_clean_interval`: The clean interval of window view in seconds to free outdated data. The system will retain the windows that have not been fully triggered according to the system time or `WATERMARK` configuration, and the other data will be deleted.
- `window_view_heartbeat_interval`: The heartbeat interval in seconds to indicate the watch query is alive.
- `wait_for_window_view_fire_signal_timeout`: Timeout for waiting for window view fire signal in event time processing.

### Example {#example}

Suppose we need to count the number of click logs per 10 seconds in a log table called `data`, and its table structure is:

```sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

First, we create a window view with tumble window of 10 seconds interval:

```sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

Then, we use the `WATCH` query to get the results.

```sql
WATCH wv
```

When logs are inserted into table `data`,

```sql
INSERT INTO data VALUES(1,now())
```

The `WATCH` query should print the results as follows:

```text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

Alternatively, we can attach the output to another table using `TO` syntax.

```sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

Additional examples can be found among stateful tests of ClickHouse (they are named `*window_view*` there).

### Window View Usage {#window-view-usage}

The window view is useful in the following scenarios:

* **Monitoring**: Aggregate and calculate the metrics logs by time, and output the results to a target table. The dashboard can use the target table as a source table.
* **Analyzing**: Automatically aggregate and preprocess data in the time window. This can be useful when analyzing a large number of logs. The preprocessing eliminates repeated calculations in multiple queries and reduces query latency.

## Related Content {#related-content}

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
- Blog: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)

## Temporary Views {#temporary-views}

ClickHouse supports **temporary views** with the following characteristics (matching temporary tables where applicable):

* **Session-lifetime**
  A temporary view exists only for the duration of the current session. It is dropped automatically when the session ends.

* **No database**
  You **cannot** qualify a temporary view with a database name. It lives outside databases (session namespace).

* **Not replicated / no ON CLUSTER**
  Temporary objects are local to the session and **cannot** be created with `ON CLUSTER`.

* **Name resolution**
  If a temporary object (table or view) has the same name as a persistent object and a query references the name **without** a database, the **temporary** object is used.

* **Logical object (no storage)**
  A temporary view stores only its `SELECT` text (uses the `View` storage internally). It does not persist data and cannot accept `INSERT`.

* **Engine clause**
  You do **not** need to specify `ENGINE`; if provided as `ENGINE = View`, it’s ignored/treated as the same logical view.

* **Security / privileges**
  Creating a temporary view requires the privilege `CREATE TEMPORARY VIEW` which is implicitly granted by `CREATE VIEW`.

* **SHOW CREATE**
  Use `SHOW CREATE TEMPORARY VIEW view_name;` to print the DDL of a temporary view.

### Syntax {#temporary-views-syntax}

```sql
CREATE TEMPORARY VIEW [IF NOT EXISTS] view_name AS <select_query>
```

`OR REPLACE` is **not** supported for temporary views (to match temporary tables). If you need to “replace” a temporary view, drop it and create it again.

### Examples {#temporary-views-examples}

Create a temporary source table and a temporary view on top:

```sql
CREATE TEMPORARY TABLE t_src (id UInt32, val String);
INSERT INTO t_src VALUES (1, 'a'), (2, 'b');

CREATE TEMPORARY VIEW tview AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview ORDER BY id;
```

Show its DDL:

```sql
SHOW CREATE TEMPORARY VIEW tview;
```

Drop it:

```sql
DROP TEMPORARY VIEW IF EXISTS tview;  -- temporary views are dropped with TEMPORARY TABLE syntax
```

### Disallowed / limitations {#temporary-views-limitations}

* `CREATE OR REPLACE TEMPORARY VIEW ...` → **not allowed** (use `DROP` + `CREATE`).
* `CREATE TEMPORARY MATERIALIZED VIEW ...` / `WINDOW VIEW` → **not allowed**.
* `CREATE TEMPORARY VIEW db.view AS ...` → **not allowed** (no database qualifier).
* `CREATE TEMPORARY VIEW view ON CLUSTER 'name' AS ...` → **not allowed** (temporary objects are session-local).
* `POPULATE`, `REFRESH`, `TO [db.table]`, inner engines, and all MV-specific clauses → **not applicable** to temporary views.

### Notes on distributed queries {#temporary-views-distributed-notes}

A temporary **view** is just a definition; there’s no data to pass around. If your temporary view references temporary **tables** (e.g., `Memory`), their data can be shipped to remote servers during distributed query execution the same way temporary tables work.

#### Example {#temporary-views-distributed-example}

```sql
-- A session-scoped, in-memory table
CREATE TEMPORARY TABLE temp_ids (id UInt64) ENGINE = Memory;

INSERT INTO temp_ids VALUES (1), (5), (42);

-- A session-scoped view over the temp table (purely logical)
CREATE TEMPORARY VIEW v_ids AS
SELECT id FROM temp_ids;

-- Replace 'test' with your cluster name.
-- GLOBAL JOIN forces ClickHouse to *ship* the small join-side (temp_ids via v_ids)
-- to every remote server that executes the left side.
SELECT count()
FROM cluster('test', system.numbers) AS n
GLOBAL ANY INNER JOIN v_ids USING (id)
WHERE n.number < 100;

```
