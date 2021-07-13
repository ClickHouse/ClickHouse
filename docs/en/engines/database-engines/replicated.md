# [experimental] Replicated {#replicated}

The engine is based on the [Atomic](../../engines/database-engines/atomic.md) engine. It supports replication of metadata via DDL log being written to ZooKeeper and executed on all of the replicas for a given database. 

One Clickhouse server can have multiple replicated databases running and updating at the same time. But there can't be multiple replicas of the same replicated database.

## Creating a Database {#creating-a-database}
``` sql
    CREATE DATABASE testdb ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Engine Parameters**

-   `zoo_path` — ZooKeeper path. The same ZooKeeper path corresponds to the same database.
-   `shard_name` — Shard name. Database replicas are grouped into shards by `shard_name`.
-   `replica_name` — Replica name. Replica names must be different for all replicas of the same shard.

!!! note "Warning"
    For [ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication) tables if no arguments provided, then default arguments are used: `/clickhouse/tables/{uuid}/{shard}` and `{replica}`. These can be changed in the server settings [default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path) and [default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name). Macro `{uuid}` is unfolded to table's uuid, `{shard}` and `{replica}` are unfolded to values from server config, not from database engine arguments. But in the future, it will be possible to use `shard_name` and `replica_name` of Replicated database.

## Specifics and recommendations {#specifics-and-recommendations}

DDL queries with `Replicated` database work in a similar way to [ON CLUSTER](../../sql-reference/distributed-ddl.md) queries, but with minor differences. 

First, the DDL request tries to execute on the initiator (the host that originally received the request from the user). If the request is not fulfilled, then the user immediately receives an error, other hosts do not try to fulfill it. If the request has been successfully completed on the initiator, then all other hosts will automatically retry until they complete it. The initiator will try to wait for the query to be completed on other hosts (no longer than [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) and will return a table with the query execution statuses on each host. 

The behavior in case of errors is regulated by the [distributed_ddl_output_mode](../../operations/settings/settings.md#distributed_ddl_output_mode) setting, for `Replicated` it is better to set it to `null_status_on_timeout` — i.e. if some hosts did not have time to execute the request for [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout), then do not throw an exception, but show the `NULL` status for them in the table.

The [system.clusters](../../operations/system-tables/clusters.md) system table contains a cluster named like the replicated database, which consists of all replicas of the database. This cluster is updated automatically when creating/deleting replicas, and it can be used for [Distributed](../../engines/table-engines/special/distributed.md#distributed) tables.

When creating a new replica of the database, this replica creates tables by itself. If the replica has been unavailable for a long time and has lagged behind the replication log — it checks its local metadata with the current metadata in ZooKeeper, moves the extra tables with data to a separate non-replicated database (so as not to accidentally delete anything superfluous), creates the missing tables, updates the table names if they have been renamed. The data is replicated at the `ReplicatedMergeTree` level, i.e. if the table is not replicated, the data will not be replicated (the database is responsible only for metadata).

## Usage Example {#usage-example}

The example must show usage and use cases. The following text contains the recommended parts of this section.

Input table:

``` text
```

Query:

``` sql
```

Result:

``` text
```

Follow up with any text to clarify the example.

**See Also** 

-   [link](#)
