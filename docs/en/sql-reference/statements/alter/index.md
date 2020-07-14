---
toc_priority: 36
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

Most `ALTER` queries modify table settings or data:

-   [COLUMN](column.md)
-   [PARTITION](partition.md)
-   [DELETE](delete.md)
-   [UPDATE](update.md)
-   [ORDER BY](order-by.md)
-   [INDEX](index/index.md)
-   [CONSTRAINT](constraint.md)
-   [TTL](ttl.md)

!!! note "Note"
    Most `ALTER` queries are supported only for [\*MergeTree](../../../engines/table-engines/mergetree-family/index.md) tables, as well as [Merge](../../../engines/table-engines/special/merge.md) and [Distributed](../../../engines/table-engines/special/distributed.md).

While these `ALTER` settings modify entities related to role-based access control:

-   [USER](user.md)
-   [ROLE](role.md)
-   [QUOTA](quota.md)
-   [ROW POLICY](row-policy.md)
-   [SETTINGS PROFILE](settings-profile.md)

## Synchronicity of ALTER Queries {#synchronicity-of-alter-queries}

For non-replicated tables, all `ALTER` queries are performed synchronously. For replicated tables, the query just adds instructions for the appropriate actions to `ZooKeeper`, and the actions themselves are performed as soon as possible. However, the query can wait for these actions to be completed on all the replicas.

For `ALTER ... ATTACH|DETACH|DROP` queries, you can use the `replication_alter_partitions_sync` setting to set up waiting. Possible values: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

## Mutations

`ALTER` queries that are intended to manipulate table data are implemented with a mechanism called “mutations”, most notably [ALTER TABLE ... DELETE](delete.md) and [ALTER TABLE ... UPDATE](update.md). They are asynchronous background processes similar to merges in [MergeTree](../../../engines/table-engines/mergetree-family/index.md) tables that to produce new “mutated” versions of parts.

For `*MergeTree` tables mutations execute by **rewriting whole data parts**. There is no atomicity - parts are substituted for mutated parts as soon as they are ready and a `SELECT` query that started executing during a mutation will see data from parts that have already been mutated along with data from parts that have not been mutated yet.

Mutations are totally ordered by their creation order and are applied to each part in that order. Mutations are also partially ordered with `INSERT INTO` queries: data that was inserted into the table before the mutation was submitted will be mutated and data that was inserted after that will not be mutated. Note that mutations do not block inserts in any way.

A mutation query returns immediately after the mutation entry is added (in case of replicated tables to ZooKeeper, for non-replicated tables - to the filesystem). The mutation itself executes asynchronously using the system profile settings. To track the progress of mutations you can use the [`system.mutations`](../../../operations/system-tables/mutations.md#system_tables-mutations) table. A mutation that was successfully submitted will continue to execute even if ClickHouse servers are restarted. There is no way to roll back the mutation once it is submitted, but if the mutation is stuck for some reason it can be cancelled with the [`KILL MUTATION`](../../../sql-reference/statements/misc.md#kill-mutation) query.

Entries for finished mutations are not deleted right away (the number of preserved entries is determined by the `finished_mutations_to_keep` storage engine parameter). Older mutation entries are deleted.

[Original article](https://clickhouse.tech/docs/en/query_language/alter/) <!--hide-->
