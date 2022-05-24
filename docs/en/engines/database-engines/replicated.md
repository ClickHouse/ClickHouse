---
sidebar_position: 30
sidebar_label: Replicated
---

# [experimental] Replicated {#replicated}

The engine is based on the [Atomic](../../engines/database-engines/atomic.md) engine. It supports replication of metadata via DDL log being written to ZooKeeper and executed on all of the replicas for a given database.

One ClickHouse server can have multiple replicated databases running and updating at the same time. But there can't be multiple replicas of the same replicated database.

## Creating a Database {#creating-a-database}
``` sql
    CREATE DATABASE testdb ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Engine Parameters**

-   `zoo_path` — ZooKeeper path. The same ZooKeeper path corresponds to the same database.
-   `shard_name` — Shard name. Database replicas are grouped into shards by `shard_name`.
-   `replica_name` — Replica name. Replica names must be different for all replicas of the same shard.

:::warning    
For [ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication) tables if no arguments provided, then default arguments are used: `/clickhouse/tables/{uuid}/{shard}` and `{replica}`. These can be changed in the server settings [default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path) and [default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name). Macro `{uuid}` is unfolded to table's uuid, `{shard}` and `{replica}` are unfolded to values from server config, not from database engine arguments. But in the future, it will be possible to use `shard_name` and `replica_name` of Replicated database.
:::

## Specifics and Recommendations {#specifics-and-recommendations}

DDL queries with `Replicated` database work in a similar way to [ON CLUSTER](../../sql-reference/distributed-ddl.md) queries, but with minor differences.

First, the DDL request tries to execute on the initiator (the host that originally received the request from the user). If the request is not fulfilled, then the user immediately receives an error, other hosts do not try to fulfill it. If the request has been successfully completed on the initiator, then all other hosts will automatically retry until they complete it. The initiator will try to wait for the query to be completed on other hosts (no longer than [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) and will return a table with the query execution statuses on each host.

The behavior in case of errors is regulated by the [distributed_ddl_output_mode](../../operations/settings/settings.md#distributed_ddl_output_mode) setting, for a `Replicated` database it is better to set it to `null_status_on_timeout` — i.e. if some hosts did not have time to execute the request for [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout), then do not throw an exception, but show the `NULL` status for them in the table.

The [system.clusters](../../operations/system-tables/clusters.md) system table contains a cluster named like the replicated database, which consists of all replicas of the database. This cluster is updated automatically when creating/deleting replicas, and it can be used for [Distributed](../../engines/table-engines/special/distributed.md#distributed) tables.

When creating a new replica of the database, this replica creates tables by itself. If the replica has been unavailable for a long time and has lagged behind the replication log — it checks its local metadata with the current metadata in ZooKeeper, moves the extra tables with data to a separate non-replicated database (so as not to accidentally delete anything superfluous), creates the missing tables, updates the table names if they have been renamed. The data is replicated at the `ReplicatedMergeTree` level, i.e. if the table is not replicated, the data will not be replicated (the database is responsible only for metadata).

[`ALTER TABLE ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) queries are allowed but not replicated. The database engine will only add/fetch/remove the partition/part to the current replica. However, if the table itself uses a Replicated table engine, then the data will be replicated after using `ATTACH`.

## Usage Example {#usage-example}

Creating a cluster with three hosts:

``` sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

Running the DDL-query:

``` sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

``` text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

Showing the system table:

``` sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

``` text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

Creating a distributed table and inserting the data:

``` sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

``` text
┌─hosts─┬─groupArray(n)─┐
│ node1 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

Adding replica on the one more host:

``` sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

The cluster configuration will look like this:

``` text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

The distributed table also will get data from the new host:

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```