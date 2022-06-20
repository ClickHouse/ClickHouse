---
sidebar_position: 35
sidebar_label: ALTER
---

# ALTER

Most `ALTER TABLE` queries modify table settings or data:

-   [COLUMN](../../../sql-reference/statements/alter/column.md)
-   [PARTITION](../../../sql-reference/statements/alter/partition.md)
-   [DELETE](../../../sql-reference/statements/alter/delete.md)
-   [UPDATE](../../../sql-reference/statements/alter/update.md)
-   [ORDER BY](../../../sql-reference/statements/alter/order-by.md)
-   [INDEX](../../../sql-reference/statements/alter/index/index.md)
-   [CONSTRAINT](../../../sql-reference/statements/alter/constraint.md)
-   [TTL](../../../sql-reference/statements/alter/ttl.md)

:::note    
Most `ALTER TABLE` queries are supported only for [\*MergeTree](../../../engines/table-engines/mergetree-family/index.md) tables, as well as [Merge](../../../engines/table-engines/special/merge.md) and [Distributed](../../../engines/table-engines/special/distributed.md).
:::

These `ALTER` statements manipulate views:

-   [ALTER TABLE ... MODIFY QUERY](../../../sql-reference/statements/alter/view.md) — Modifies a [Materialized view](../create/view.md#materialized) structure.
-   [ALTER LIVE VIEW](../../../sql-reference/statements/alter/view.md#alter-live-view) — Refreshes a [Live view](../create/view.md#live-view).

These `ALTER` statements modify entities related to role-based access control:

-   [USER](../../../sql-reference/statements/alter/user.md)
-   [ROLE](../../../sql-reference/statements/alter/role.md)
-   [QUOTA](../../../sql-reference/statements/alter/quota.md)
-   [ROW POLICY](../../../sql-reference/statements/alter/row-policy.md)
-   [SETTINGS PROFILE](../../../sql-reference/statements/alter/settings-profile.md)

[ALTER TABLE ... MODIFY COMMENT](../../../sql-reference/statements/alter/comment.md) statement adds, modifies, or removes comments to the table, regardless if it was set before or not.

## Mutations

`ALTER` queries that are intended to manipulate table data are implemented with a mechanism called “mutations”, most notably [ALTER TABLE … DELETE](../../../sql-reference/statements/alter/delete.md) and [ALTER TABLE … UPDATE](../../../sql-reference/statements/alter/update.md). They are asynchronous background processes similar to merges in [MergeTree](../../../engines/table-engines/mergetree-family/index.md) tables that to produce new “mutated” versions of parts.

For `*MergeTree` tables mutations execute by **rewriting whole data parts**. There is no atomicity - parts are substituted for mutated parts as soon as they are ready and a `SELECT` query that started executing during a mutation will see data from parts that have already been mutated along with data from parts that have not been mutated yet.

Mutations are totally ordered by their creation order and are applied to each part in that order. Mutations are also partially ordered with `INSERT INTO` queries: data that was inserted into the table before the mutation was submitted will be mutated and data that was inserted after that will not be mutated. Note that mutations do not block inserts in any way.

A mutation query returns immediately after the mutation entry is added (in case of replicated tables to ZooKeeper, for non-replicated tables - to the filesystem). The mutation itself executes asynchronously using the system profile settings. To track the progress of mutations you can use the [`system.mutations`](../../../operations/system-tables/mutations.md#system_tables-mutations) table. A mutation that was successfully submitted will continue to execute even if ClickHouse servers are restarted. There is no way to roll back the mutation once it is submitted, but if the mutation is stuck for some reason it can be cancelled with the [`KILL MUTATION`](../../../sql-reference/statements/misc.md#kill-mutation) query.

Entries for finished mutations are not deleted right away (the number of preserved entries is determined by the `finished_mutations_to_keep` storage engine parameter). Older mutation entries are deleted.

## Synchronicity of ALTER Queries

For non-replicated tables, all `ALTER` queries are performed synchronously. For replicated tables, the query just adds instructions for the appropriate actions to `ZooKeeper`, and the actions themselves are performed as soon as possible. However, the query can wait for these actions to be completed on all the replicas.

For all `ALTER` queries, you can use the [replication_alter_partitions_sync](../../../operations/settings/settings.md#replication-alter-partitions-sync) setting to set up waiting.

You can specify how long (in seconds) to wait for inactive replicas to execute all `ALTER` queries with the [replication_wait_for_inactive_replica_timeout](../../../operations/settings/settings.md#replication-wait-for-inactive-replica-timeout) setting.

:::note    
For all `ALTER` queries, if `replication_alter_partitions_sync = 2` and some replicas are not active for more than the time, specified in the `replication_wait_for_inactive_replica_timeout` setting, then an exception `UNFINISHED` is thrown.
:::

For `ALTER TABLE ... UPDATE|DELETE` queries the synchronicity is defined by the [mutations_sync](../../../operations/settings/settings.md#mutations_sync) setting.
