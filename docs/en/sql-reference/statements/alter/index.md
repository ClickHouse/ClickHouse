---
description: 'Documentation for ALTER'
sidebar_label: 'ALTER'
sidebar_position: 35
slug: /sql-reference/statements/alter/
title: 'ALTER'
---

# ALTER

Most `ALTER TABLE` queries modify table settings or data:

| Modifier                                                                            |
|-------------------------------------------------------------------------------------|
| [COLUMN](/sql-reference/statements/alter/column.md)                         |
| [PARTITION](/sql-reference/statements/alter/partition.md)                   |
| [DELETE](/sql-reference/statements/alter/delete.md)                         |
| [UPDATE](/sql-reference/statements/alter/update.md)                         |
| [ORDER BY](/sql-reference/statements/alter/order-by.md)                     |
| [INDEX](/sql-reference/statements/alter/skipping-index.md)                  |
| [CONSTRAINT](/sql-reference/statements/alter/constraint.md)                 |
| [TTL](/sql-reference/statements/alter/ttl.md)                               |
| [STATISTICS](/sql-reference/statements/alter/statistics.md)                 |
| [APPLY DELETED MASK](/sql-reference/statements/alter/apply-deleted-mask.md) |

:::note
Most `ALTER TABLE` queries are supported only for [\*MergeTree](/engines/table-engines/mergetree-family/index.md), [Merge](/engines/table-engines/special/merge.md) and [Distributed](/engines/table-engines/special/distributed.md) tables.
:::

These `ALTER` statements manipulate views:

| Statement                                                                           | Description                                                                          |
|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| [ALTER TABLE ... MODIFY QUERY](/sql-reference/statements/alter/view.md)     | Modifies a [Materialized view](/sql-reference/statements/create/view) structure.                                       |
| [ALTER LIVE VIEW](/sql-reference/statements/alter/view#alter-live-view-statement) | Refreshes a [Live view](/sql-reference/statements/create/view.md/#live-view).|

These `ALTER` statements modify entities related to role-based access control:

| Statement                                                                       |
|---------------------------------------------------------------------------------|
| [USER](/sql-reference/statements/alter/user.md)                         |
| [ROLE](/sql-reference/statements/alter/role.md)                         |
| [QUOTA](/sql-reference/statements/alter/quota.md)                       |
| [ROW POLICY](/sql-reference/statements/alter/row-policy.md)             |
| [SETTINGS PROFILE](/sql-reference/statements/alter/settings-profile.md) |

| Statement                                                                             | Description                                                                               |
|---------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| [ALTER TABLE ... MODIFY COMMENT](/sql-reference/statements/alter/comment.md)  | Adds, modifies, or removes comments to the table, regardless if it was set before or not. |
| [ALTER NAMED COLLECTION](/sql-reference/statements/alter/named-collection.md) | Modifies [Named Collections](/operations/named-collections.md).                   |

## Mutations {#mutations}

`ALTER` queries that are intended to manipulate table data are implemented with a mechanism called "mutations", most notably [ALTER TABLE ... DELETE](/sql-reference/statements/alter/delete.md) and [ALTER TABLE ... UPDATE](/sql-reference/statements/alter/update.md). They are asynchronous background processes similar to merges in [MergeTree](/engines/table-engines/mergetree-family/index.md) tables that to produce new "mutated" versions of parts.

For `*MergeTree` tables mutations execute by **rewriting whole data parts**. 
There is no atomicity — parts are substituted for mutated parts as soon as they are ready and a `SELECT` query that started executing during a mutation will see data from parts that have already been mutated along with data from parts that have not been mutated yet.

Mutations are totally ordered by their creation order and are applied to each part in that order. Mutations are also partially ordered with `INSERT INTO` queries: data that was inserted into the table before the mutation was submitted will be mutated and data that was inserted after that will not be mutated. Note that mutations do not block inserts in any way.

A mutation query returns immediately after the mutation entry is added (in case of replicated tables to ZooKeeper, for non-replicated tables - to the filesystem). The mutation itself executes asynchronously using the system profile settings. To track the progress of mutations you can use the [`system.mutations`](/operations/system-tables/mutations) table. A mutation that was successfully submitted will continue to execute even if ClickHouse servers are restarted. There is no way to roll back the mutation once it is submitted, but if the mutation is stuck for some reason it can be cancelled with the [`KILL MUTATION`](/sql-reference/statements/kill.md/#kill-mutation) query.

Entries for finished mutations are not deleted right away (the number of preserved entries is determined by the `finished_mutations_to_keep` storage engine parameter). Older mutation entries are deleted.

## Synchronicity of ALTER Queries {#synchronicity-of-alter-queries}

For non-replicated tables, all `ALTER` queries are performed synchronously. For replicated tables, the query just adds instructions for the appropriate actions to `ZooKeeper`, and the actions themselves are performed as soon as possible. However, the query can wait for these actions to be completed on all the replicas.

For `ALTER` queries that creates mutations (e.g.: including, but not limited to `UPDATE`, `DELETE`, `MATERIALIZE INDEX`, `MATERIALIZE PROJECTION`, `MATERIALIZE COLUMN`, `APPLY DELETED MASK`, `CLEAR STATISTIC`, `MATERIALIZE STATISTIC`) the synchronicity is defined by the [mutations_sync](/operations/settings/settings.md/#mutations_sync) setting.

For other `ALTER` queries which only modify the metadata, you can use the [alter_sync](/operations/settings/settings#alter_sync) setting to set up waiting.

You can specify how long (in seconds) to wait for inactive replicas to execute all `ALTER` queries with the [replication_wait_for_inactive_replica_timeout](/operations/settings/settings#replication_wait_for_inactive_replica_timeout) setting.

:::note
For all `ALTER` queries, if `alter_sync = 2` and some replicas are not active for more than the time, specified in the `replication_wait_for_inactive_replica_timeout` setting, then an exception `UNFINISHED` is thrown.
:::

## Related content {#related-content}

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
