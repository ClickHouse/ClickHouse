---
slug: /en/sql-reference/statements/system
sidebar_position: 36
sidebar_label: SYSTEM
---

# SYSTEM Statements

## RELOAD EMBEDDED DICTIONARIES

Reload all [Internal dictionaries](../../sql-reference/dictionaries/internal-dicts.md).
By default, internal dictionaries are disabled.
Always returns `Ok.` regardless of the result of the internal dictionary update.

## RELOAD DICTIONARIES

Reloads all dictionaries that have been successfully loaded before.
By default, dictionaries are loaded lazily (see [dictionaries_lazy_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)), so instead of being loaded automatically at startup, they are initialized on first access through dictGet function or SELECT from tables with ENGINE = Dictionary. The `SYSTEM RELOAD DICTIONARIES` query reloads such dictionaries (LOADED).
Always returns `Ok.` regardless of the result of the dictionary update.

## RELOAD DICTIONARY

Completely reloads a dictionary `dictionary_name`, regardless of the state of the dictionary (LOADED / NOT_LOADED / FAILED).
Always returns `Ok.` regardless of the result of updating the dictionary.
The status of the dictionary can be checked by querying the `system.dictionaries` table.

``` sql
SELECT name, status FROM system.dictionaries;
```

## RELOAD MODELS

:::note
This statement and `SYSTEM RELOAD MODEL` merely unload catboost models from the clickhouse-library-bridge. The function `catboostEvaluate()`
loads a model upon first access if it is not loaded yet.
:::

Unloads all CatBoost models.

**Syntax**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

## RELOAD MODEL

Unloads a CatBoost model at `model_path`.

**Syntax**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

## RELOAD FUNCTIONS

Reloads all registered [executable user defined functions](../functions/index.md#executable-user-defined-functions) or one of them from a configuration file.

**Syntax**

```sql
RELOAD FUNCTIONS [ON CLUSTER cluster_name]
RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

## DROP DNS CACHE

Resets ClickHouse’s internal DNS cache. Sometimes (for old ClickHouse versions) it is necessary to use this command when changing the infrastructure (changing the IP address of another ClickHouse server or the server used by dictionaries).

For more convenient (automatic) cache management, see disable_internal_dns_cache, dns_cache_update_period parameters.

## DROP MARK CACHE

Resets the mark cache.

## DROP REPLICA

Dead replicas can be dropped using following syntax:

``` sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

Queries will remove the replica path in ZooKeeper. It is useful when the replica is dead and its metadata cannot be removed from ZooKeeper by `DROP TABLE` because there is no such table anymore. It will only drop the inactive/stale replica, and it cannot drop local replica, please use `DROP TABLE` for that. `DROP REPLICA` does not drop any tables and does not remove any data or metadata from disk.

The first one removes metadata of `'replica_name'` replica of `database.table` table.
The second one does the same for all replicated tables in the database.
The third one does the same for all replicated tables on the local server.
The fourth one is useful to remove metadata of dead replica when all other replicas of a table were dropped. It requires the table path to be specified explicitly. It must be the same path as was passed to the first argument of `ReplicatedMergeTree` engine on table creation.

## DROP UNCOMPRESSED CACHE

Reset the uncompressed data cache.
The uncompressed data cache is enabled/disabled with the query/user/profile-level setting [use_uncompressed_cache](../../operations/settings/settings.md#setting-use_uncompressed_cache).
Its size can be configured using the server-level setting [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size).

## DROP COMPILED EXPRESSION CACHE

Reset the compiled expression cache.
The compiled expression cache is enabled/disabled with the query/user/profile-level setting [compile_expressions](../../operations/settings/settings.md#compile-expressions).

## DROP QUERY CACHE

Resets the [query cache](../../operations/query-cache.md).

## FLUSH LOGS

Flushes buffered log messages to system tables, e.g. system.query_log. Mainly useful for debugging since most system tables have a default flush interval of 7.5 seconds.
This will also create system tables even if message queue is empty.

## RELOAD CONFIG

Reloads ClickHouse configuration. Used when configuration is stored in ZooKeeper.

## RELOAD USERS

Reloads all access storages, including: users.xml, local disk access storage, replicated (in ZooKeeper) access storage. Note that `SYSTEM RELOAD CONFIG` will only reload users.xml access storage.

## SHUTDOWN

Normally shuts down ClickHouse (like `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL

Aborts ClickHouse process (like `kill -9 {$ pid_clickhouse-server}`)

## Managing Distributed Tables

ClickHouse can manage [distributed](../../engines/table-engines/special/distributed.md) tables. When a user inserts data into these tables, ClickHouse first creates a queue of the data that should be sent to cluster nodes, then asynchronously sends it. You can manage queue processing with the [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed), and [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) queries. You can also synchronously insert distributed data with the [insert_distributed_sync](../../operations/settings/settings.md#insert_distributed_sync) setting.

### STOP DISTRIBUTED SENDS

Disables background data distribution when inserting data into distributed tables.

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED

Forces ClickHouse to send data to cluster nodes synchronously. If any nodes are unavailable, ClickHouse throws an exception and stops query execution. You can retry the query until it succeeds, which will happen when all nodes are back online.

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS

Enables background data distribution when inserting data into distributed tables.

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

## Managing MergeTree Tables

ClickHouse can manage background processes in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

### STOP MERGES

Provides possibility to stop background merges for tables in the MergeTree family:

``` sql
SYSTEM STOP MERGES [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
`DETACH / ATTACH` table will start background merges for the table even in case when merges have been stopped for all MergeTree tables before.
:::

### START MERGES

Provides possibility to start background merges for tables in the MergeTree family:

``` sql
SYSTEM START MERGES [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

### STOP TTL MERGES

Provides possibility to stop background delete old data according to [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist or table has not MergeTree engine. Returns error when database does not exist:

``` sql
SYSTEM STOP TTL MERGES [[db.]merge_tree_family_table_name]
```

### START TTL MERGES

Provides possibility to start background delete old data according to [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist. Returns error when database does not exist:

``` sql
SYSTEM START TTL MERGES [[db.]merge_tree_family_table_name]
```

### STOP MOVES

Provides possibility to stop background move data according to [TTL table expression with TO VOLUME or TO DISK clause](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist. Returns error when database does not exist:

``` sql
SYSTEM STOP MOVES [[db.]merge_tree_family_table_name]
```

### START MOVES

Provides possibility to start background move data according to [TTL table expression with TO VOLUME and TO DISK clause](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist. Returns error when database does not exist:

``` sql
SYSTEM START MOVES [[db.]merge_tree_family_table_name]
```

### SYSTEM UNFREEZE {#query_language-system-unfreeze}

Clears freezed backup with the specified name from all the disks. See more about unfreezing separate parts in [ALTER TABLE table_name UNFREEZE WITH NAME ](alter/partition.md#alter_unfreeze-partition)

``` sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

## Managing ReplicatedMergeTree Tables

ClickHouse can manage background replication related processes in [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md#table_engines-replication) tables.

### STOP FETCHES

Provides possibility to stop background fetches for inserted parts for tables in the `ReplicatedMergeTree` family:
Always returns `Ok.` regardless of the table engine and even if table or database does not exist.

``` sql
SYSTEM STOP FETCHES [[db.]replicated_merge_tree_family_table_name]
```

### START FETCHES

Provides possibility to start background fetches for inserted parts for tables in the `ReplicatedMergeTree` family:
Always returns `Ok.` regardless of the table engine and even if table or database does not exist.

``` sql
SYSTEM START FETCHES [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATED SENDS

Provides possibility to stop background sends to other replicas in cluster for new inserted parts for tables in the `ReplicatedMergeTree` family:

``` sql
SYSTEM STOP REPLICATED SENDS [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATED SENDS

Provides possibility to start background sends to other replicas in cluster for new inserted parts for tables in the `ReplicatedMergeTree` family:

``` sql
SYSTEM START REPLICATED SENDS [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATION QUEUES

Provides possibility to stop background fetch tasks from replication queues which stored in Zookeeper for tables in the `ReplicatedMergeTree` family. Possible background tasks types - merges, fetches, mutation, DDL statements with ON CLUSTER clause:

``` sql
SYSTEM STOP REPLICATION QUEUES [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATION QUEUES

Provides possibility to start background fetch tasks from replication queues which stored in Zookeeper for tables in the `ReplicatedMergeTree` family. Possible background tasks types - merges, fetches, mutation, DDL statements with ON CLUSTER clause:

``` sql
SYSTEM START REPLICATION QUEUES [[db.]replicated_merge_tree_family_table_name]
```

### SYNC REPLICA

Wait until a `ReplicatedMergeTree` table will be synced with other replicas in a cluster. Will run until `receive_timeout` if fetches currently disabled for the table.

``` sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

After running this statement the `[db.]replicated_merge_tree_family_table_name` fetches commands from the common replicated log into its own replication queue, and then the query waits till the replica processes all of the fetched commands.

### RESTART REPLICA

Provides possibility to reinitialize Zookeeper session's state for `ReplicatedMergeTree` table, will compare current state with Zookeeper as source of truth and add tasks to Zookeeper queue if needed.
Initialization of replication queue based on ZooKeeper data happens in the same way as for `ATTACH TABLE` statement. For a short time, the table will be unavailable for any operations.

``` sql
SYSTEM RESTART REPLICA [db.]replicated_merge_tree_family_table_name
```

### RESTORE REPLICA

Restores a replica if data is [possibly] present but Zookeeper metadata is lost.

Works only on readonly `ReplicatedMergeTree` tables.

One may execute query after:

  - ZooKeeper root `/` loss.
  - Replicas path `/replicas` loss.
  - Individual replica path `/replicas/replica_name/` loss.

Replica attaches locally found parts and sends info about them to Zookeeper.
Parts present on a replica before metadata loss are not re-fetched from other ones if not being outdated (so replica restoration does not mean re-downloading all data over the network).

:::warning
Parts in all states are moved to `detached/` folder. Parts active before data loss (committed) are attached.
:::

**Syntax**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

Alternative syntax:

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**Example**

Creating a table on multiple servers. After the replica's metadata in ZooKeeper is lost, the table will attach as read-only as metadata is missing. The last query needs to execute on every replica.

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

Another way:

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

### RESTART REPLICAS

Provides possibility to reinitialize Zookeeper sessions state for all `ReplicatedMergeTree` tables, will compare current state with Zookeeper as source of true and add tasks to Zookeeper queue if needed

### DROP FILESYSTEM CACHE

Allows to drop filesystem cache.

```sql
SYSTEM DROP FILESYSTEM CACHE
```

### SYNC FILE CACHE

:::note
It's too heavy and has potential for misuse.
:::

Will do sync syscall. 

```sql
SYSTEM SYNC FILE CACHE
```
