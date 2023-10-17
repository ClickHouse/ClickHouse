---
slug: /en/sql-reference/statements/alter/projection
sidebar_position: 49
sidebar_label: PROJECTION
title: "Projections"
---

Projections store data in a format that optimizes query execution, this feature is useful for:
- Running queries on a column that is not a part of the primary key
- Pre-aggregating columns, it will reduce both computation and IO

You can define one or more projections for a table, and during the query analysis the projection with the least data to scan will be selected by ClickHouse without modifying the query provided by the user.

:::note Disk usage

Projections will create internally a new hidden table, this means that more IO and space on disk will be required.
Example, If the projection has defined a different primary key, all the data from the original table will be duplicated.
:::

You can see more technical details about how projections work internally on this [page](/docs/en/guides/best-practices/sparse-primary-indexes.md/#option-3-projections).

## Example filtering without using primary keys

Creating the table:
```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```
Using `ALTER TABLE`, we could add the Projection to an existing table:
```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
SELECT
*
ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```
Inserting the data:
```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

The Projection will allow us to filter by `user_name` fast even if in the original Table `user_name` was not defined as a `PRIMARY_KEY`.
At query time ClickHouse determined that less data will be processed if the projection is used, as the data is ordered by `user_name`.
```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

To verify that a query is using the projection, we could review the `system.query_log` table. On the `projections` field we have the name of the projection used or empty if none has been used:
```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

## Example pre-aggregation query

Creating the table with the Projection:
```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```
Inserting the data:
```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```
```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```
We will execute a first query using `GROUP BY` using the field `user_agent`, this query will not use the projection defined as the pre-aggregation does not match.
```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

To use the projection we could execute queries that select part of, or all of the pre-aggregation and `GROUP BY` fields.
```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```
```
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

As mentioned before, we could review the `system.query_log` table. On the `projections` field we have the name of the projection used or empty if none has been used:
```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

# Manipulating Projections

The following operations with [projections](/docs/en/engines/table-engines/mergetree-family/mergetree.md/#projections) are available:

## ADD PROJECTION

`ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY] )` - Adds projection description to tables metadata.

## DROP PROJECTION

`ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name` - Removes projection description from tables metadata and deletes projection files from disk. Implemented as a [mutation](/docs/en/sql-reference/statements/alter/index.md#mutations).

## MATERIALIZE PROJECTION

`ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]` - The query rebuilds the projection `name` in the partition `partition_name`. Implemented as a [mutation](/docs/en/sql-reference/statements/alter/index.md#mutations).

## CLEAR PROJECTION

`ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]` - Deletes projection files from disk without removing description. Implemented as a [mutation](/docs/en/sql-reference/statements/alter/index.md#mutations).


The commands `ADD`, `DROP` and `CLEAR` are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing projections metadata via ClickHouse Keeper or ZooKeeper.

:::note
Projection manipulation is supported only for tables with [`*MergeTree`](/docs/en/engines/table-engines/mergetree-family/mergetree.md) engine (including [replicated](/docs/en/engines/table-engines/mergetree-family/replication.md) variants).
:::
