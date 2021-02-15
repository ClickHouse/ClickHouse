---
toc_priority: 3
toc_title: VIEW
---

# CREATE VIEW {#create-view}

Creates a new view. There are two types of views: normal and materialized.

## Normal {#normal}

Syntax:

``` sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER] AS SELECT ...
```

Normal views don’t store any data, they just perform a read from another table on each access. In other words, a normal view is nothing more than a saved query. When reading from a view, this saved query is used as a subquery in the [FROM](../../../sql-reference/statements/select/from.md) clause.

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
