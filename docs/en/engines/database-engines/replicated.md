---
description: 'The engine is based on the Atomic engine. It supports replication of
  metadata via DDL log being written to ZooKeeper and executed on all of the replicas
  for a given database.'
sidebar_label: 'Replicated'
sidebar_position: 30
slug: /engines/database-engines/replicated
title: 'Replicated'
doc_type: 'reference'
---

# Replicated

The engine is based on the [Atomic](../../engines/database-engines/atomic.md) engine. It supports replication of metadata via DDL log being written to ZooKeeper and executed on all of the replicas for a given database.

One ClickHouse server can have multiple replicated databases running and updating at the same time. But there can't be multiple replicas of the same replicated database.

## Creating a database {#creating-a-database}
```sql
CREATE DATABASE testdb [UUID '...'] ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Engine Parameters**

- `zoo_path` — ZooKeeper path. The same ZooKeeper path corresponds to the same database.
- `shard_name` — Shard name. Database replicas are grouped into shards by `shard_name`.
- `replica_name` — Replica name. Replica names must be different for all replicas of the same shard.

Parameters can be omitted, in such case missing parameters are substituted with defaults.

If `zoo_path` contains macro `{uuid}`, it is required to specify explicit UUID or add [ON CLUSTER](../../sql-reference/distributed-ddl.md) to create statement to ensure all replicas use the same UUID for this database.

For [ReplicatedMergeTree](/engines/table-engines/mergetree-family/replication) tables if no arguments provided, then default arguments are used: `/clickhouse/tables/{uuid}/{shard}` and `{replica}`. These can be changed in the server settings [default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path) and [default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name). Macro `{uuid}` is unfolded to table's uuid, `{shard}` and `{replica}` are unfolded to values from server config, not from database engine arguments. But in the future, it will be possible to use `shard_name` and `replica_name` of Replicated database.

## Specifics and recommendations {#specifics-and-recommendations}

DDL queries with `Replicated` database work in a similar way to [ON CLUSTER](../../sql-reference/distributed-ddl.md) queries, but with minor differences.

First, the DDL request tries to execute on the initiator (the host that originally received the request from the user). If the request is not fulfilled, then the user immediately receives an error, other hosts do not try to fulfill it. If the request has been successfully completed on the initiator, then all other hosts will automatically retry until they complete it. The initiator will try to wait for the query to be completed on other hosts (no longer than [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) and will return a table with the query execution statuses on each host.

The behavior in case of errors is regulated by the [distributed_ddl_output_mode](../../operations/settings/settings.md#distributed_ddl_output_mode) setting, for a `Replicated` database it is better to set it to `null_status_on_timeout` — i.e. if some hosts did not have time to execute the request for [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout), then do not throw an exception, but show the `NULL` status for them in the table.

The [system.clusters](../../operations/system-tables/clusters.md) system table contains a cluster named like the replicated database, which consists of all replicas of the database. This cluster is updated automatically when creating/deleting replicas, and it can be used for [Distributed](/engines/table-engines/special/distributed) tables.

When creating a new replica of the database, this replica creates tables by itself. If the replica has been unavailable for a long time and has lagged behind the replication log — it checks its local metadata with the current metadata in ZooKeeper, moves the extra tables with data to a separate non-replicated database (so as not to accidentally delete anything superfluous), creates the missing tables, updates the table names if they have been renamed. The data is replicated at the `ReplicatedMergeTree` level, i.e. if the table is not replicated, the data will not be replicated (the database is responsible only for metadata).

[`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) queries are allowed but not replicated. The database engine will only add/fetch/remove the partition/part to the current replica. However, if the table itself uses a Replicated table engine, then the data will be replicated after using `ATTACH`.

In case you need only configure a cluster without maintaining table replication, refer to [Cluster Discovery](../../operations/cluster-discovery.md) feature.

## Usage example {#usage-example}

Creating a cluster with three hosts:

```sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

Creating database on cluster with implicit parameters:

```sql
CREATE DATABASE r ON CLUSTER default ENGINE=Replicated;
```

Running the DDL-query:

```sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

```text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

Showing the system table:

```sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

Creating a distributed table and inserting the data:

```sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

Adding replica on the one more host:

```sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

Adding replica on the one more host if macro `{uuid}` is used in `zoo_path`:
```sql
node1 :) SELECT uuid FROM system.databases WHERE database='r';
node4 :) CREATE DATABASE r UUID '<uuid from previous query>' ENGINE=Replicated('some/path/{uuid}','other_shard','r2');
```

The cluster configuration will look like this:

```text
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

## Settings {#settings}
The following settings are supported:

| Setting                                                                      | Default                        | Description                                                                                                                                                           |
|------------------------------------------------------------------------------|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `max_broken_tables_ratio`                                                    | 1                              | Do not recover replica automatically if the ratio of staled tables to all tables is greater                                                                           |
| `max_replication_lag_to_enqueue`                                             | 50                             | Replica will throw exception on attempt to execute query if its replication lag greater                                                                               |
| `wait_entry_commited_timeout_sec`                                            | 3600                           | Replicas will try to cancel query if timeout exceed, but initiator host has not executed it yet                                                                       |
| `collection_name`                                                            |                                | A name of a collection defined in server's config where all info for cluster authentication is defined                                                                |
| `check_consistency`                                                          | true                           | Check consistency of local metadata and metadata in Keeper, do replica recovery on inconsistency                                                                      |
| `max_retries_before_automatic_recovery`                                      | 10                             | Max number of attempts to execute a queue entry before marking replica as lost recovering it from snapshot (0 means infinite)                                         |
| `allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views` | false                          | If enabled, when processing DDLs in Replicated databases, it skips creating and exchanging DDLs of the temporary tables of refreshable materialized views if possible |
| `logs_to_keep`                                                               | 1000                           | Default number of logs to keep in ZooKeeper for Replicated database.                                                                                                  |
| `default_replica_path`                                                       | `/clickhouse/databases/{uuid}` | The path to the database in ZooKeeper. Used during database creation if arguments are omitted.                                                                        |
| `default_replica_shard_name`                                                 | `{shard}`                      | The shard name of the replica in the database. Used during database creation if arguments are omitted.                                                                |
| `default_replica_name`                                                       | `{replica}`                    | The name of the replica in the database. Used during database creation if arguments are omitted.                                                                      |

Default values may be overwritten in the configuration file
```xml
<clickhouse>
    <database_replicated>
        <max_broken_tables_ratio>0.75</max_broken_tables_ratio>
        <max_replication_lag_to_enqueue>100</max_replication_lag_to_enqueue>
        <wait_entry_commited_timeout_sec>1800</wait_entry_commited_timeout_sec>
        <collection_name>postgres1</collection_name>
        <check_consistency>false</check_consistency>
        <max_retries_before_automatic_recovery>5</max_retries_before_automatic_recovery>
        <default_replica_path>/clickhouse/databases/{uuid}</default_replica_path>
        <default_replica_shard_name>{shard}</default_replica_shard_name>
        <default_replica_name>{replica}</default_replica_name>
    </database_replicated>
</clickhouse>
```