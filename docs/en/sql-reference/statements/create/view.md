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

Normal views don’t store any data. They just perform a read from another table on each access. In other words, a normal view is nothing more than a saved query. When reading from a view, this saved query is used as a subquery in the [FROM](../../../sql-reference/statements/select/from.md) clause.

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
    Materialized views in ClickHouse are implemented more like insert triggers. If there’s some aggregation in the view query, it’s applied only to the batch of freshly inserted data. Any changes to existing data of source table (like update, delete, drop partition, etc.) doesn’t change the materialized view.

If you specify `POPULATE`, the existing table data is inserted in the view when creating it, as if making a `CREATE TABLE ... AS SELECT ...` . Otherwise, the query contains only the data inserted in the table after creating the view. We **don’t recommend** using POPULATE, since data inserted in the table during the view creation will not be inserted in it.

A `SELECT` query can contain `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` is set, data is aggregated during insertion, but only within a single packet of inserted data. The data won’t be further aggregated. The exception is when using an `ENGINE` that independently performs data aggregation, such as `SummingMergeTree`.

The execution of [ALTER](../../../sql-reference/statements/alter/index.md) queries on materialized views has limitations, so they might be inconvenient. If the materialized view uses the construction `TO [db.]name`, you can `DETACH` the view, run `ALTER` for the target table, and then `ATTACH` the previously detached (`DETACH`) view.

Views look the same as normal tables. For example, they are listed in the result of the `SHOW TABLES` query.

There isn’t a separate query for deleting views. To delete a view, use [DROP TABLE](../../../sql-reference/statements/drop.md).

## Live View (Experimental) {#live-view}

!!! important "Important"
    This is an experimental feature that may change in backwards-incompatible ways in the future releases.
    Enable usage of live views and `WATCH` query using `set allow_experimental_live_view = 1`.


```sql
CREATE LIVE VIEW [IF NOT EXISTS] [db.]table_name [WITH [TIMEOUT [value_in_sec] [AND]] [REFRESH [value_in_sec]]] AS SELECT ...
```

Live views store result of the corresponding [SELECT](../../../sql-reference/statements/select/index.md) query
and are updated any time the result of the query changes. Query result as well as partial result
needed to combine with new data are stored in memory providing increased performance
for repeated queries. Live views can provide push notifications
when query result changes using the [WATCH](../../../sql-reference/statements/watch.md) query.

Live views are triggered by insert into the innermost table specified in the query. 

!!! info "Note"
    [Table function](../../../sql-reference/table-functions/index.md) is not supported as the innermost table.

!!! info "Note"
    Tables that do not have inserts such as a [dictionary](../../../sql-reference/dictionaries/index.md)
    or a [system table](../../../operations/system-tables/index.md)
    will not trigger a live view. See [WITH REFRESH](#live-view-with-refresh) to enable periodic
    updates of a live view.

Live views work similarly to how a query in a distributed table works. But instead of combining partial results
from different servers they combine partial result from current data with partial result from the new data.
When a live view query includes a subquery then the cached partial result is only stored for the innermost subquery.

!!! info "Note"
   Only queries where one can combine partial result from the old data plus partial result from the new data will work.
   Live view will not work for queries that require the complete data set to compute the final result.

You can execute [SELECT](../../../sql-reference/statements/select/index.md) query on a live view
in the same way as for any regular view or a table. If the query result is cached 
it will return the result immediately without running the stored query on the underlying tables.

### Force Refresh {#live-view-alter-refresh}

You can force live view refresh using the `ALTER LIVE VIEW [db.]table_name REFRESH` statement.

### With Timeout {#live-view-with-timeout}

When a live view is create with a `WITH TIMEOUT` clause then the live view will be dropped automatically after the specified
number of seconds elapse since the end of the last [WATCH](../../../sql-reference/statements/watch.md) query. 

```sql
CREATE LIVE VIEW [db.]table_name WITH TIMEOUT value_in_sec AS SELECT ...
```

### With Refresh {#live-view-with-refresh}

When a live view is created with a `WITH REFRESH` clause then it will be automatically refreshed
after the specified number of seconds elapse since the last refresh or trigger.

```sql
CREATE LIVE VIEW [db.]table_name WITH REFRESH value_in_sec AS SELECT ...
```

You can combine `WITH TIMEOUT` and `WITH REFRESH` clauses using an `AND`. 

```sql
CREATE LIVE VIEW [db.]table_name WITH TIMEOUT value_in_sec AND REFRESH value_in_sec AS SELECT ...
```

### Settings {#live-view-settings}

You can use the following settings to control the behaviour of live views.

- `allow_experimental_live_view` - enable live views. Default `0`.
- `live_view_heartbeat_interval` - the heartbeat interval in seconds to indicate live query is alive. Default `15` seconds.
- `max_live_view_insert_blocks_before_refresh` - maximum number of inserted blocks after which
   mergeable blocks are dropped and query is re-executed. Default `64` inserts.
-  `temporary_live_view_timeout` - interval after which live view with timeout is deleted. Default `5` seconds.
-  `periodic_live_view_refresh` - interval after which periodically refreshed live view is forced to refresh. Default `60` seconds.
