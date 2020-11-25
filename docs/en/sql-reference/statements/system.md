---
toc_priority: 36
toc_title: SYSTEM
---

# SYSTEM Statements {#query-language-system}

The list of available `SYSTEM` statements:

-   [RELOAD EMBEDDED DICTIONARIES](#query_language-system-reload-emdedded-dictionaries)
-   [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
-   [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
-   [DROP DNS CACHE](#query_language-system-drop-dns-cache)
-   [DROP MARK CACHE](#query_language-system-drop-mark-cache)
-   [DROP UNCOMPRESSED CACHE](#query_language-system-drop-uncompressed-cache)
-   [DROP COMPILED EXPRESSION CACHE](#query_language-system-drop-compiled-expression-cache)
-   [DROP REPLICA](#query_language-system-drop-replica)
-   [FLUSH LOGS](#query_language-system-flush_logs)
-   [RELOAD CONFIG](#query_language-system-reload-config)
-   [SHUTDOWN](#query_language-system-shutdown)
-   [KILL](#query_language-system-kill)
-   [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
-   [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
-   [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)
-   [STOP MERGES](#query_language-system-stop-merges)
-   [START MERGES](#query_language-system-start-merges)
-   [STOP TTL MERGES](#query_language-stop-ttl-merges)
-   [START TTL MERGES](#query_language-start-ttl-merges)
-   [STOP MOVES](#query_language-stop-moves)
-   [START MOVES](#query_language-start-moves)
-   [STOP FETCHES](#query_language-system-stop-fetches)
-   [START FETCHES](#query_language-system-start-fetches)
-   [STOP REPLICATED SENDS](#query_language-system-start-replicated-sends)
-   [START REPLICATED SENDS](#query_language-system-start-replicated-sends)
-   [STOP REPLICATION QUEUES](#query_language-system-stop-replication-queues)
-   [START REPLICATION QUEUES](#query_language-system-start-replication-queues)
-   [SYNC REPLICA](#query_language-system-sync-replica)
-   [RESTART REPLICA](#query_language-system-restart-replica)
-   [RESTART REPLICAS](#query_language-system-restart-replicas)

## RELOAD EMBEDDED DICTIONARIES {#query_language-system-reload-emdedded-dictionaries}

Reload all [Internal dictionaries](../../sql-reference/dictionaries/internal-dicts.md).
By default, internal dictionaries are disabled.
Always returns `Ok.` regardless of the result of the internal dictionary update.

## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

Reloads all dictionaries that have been successfully loaded before.
By default, dictionaries are loaded lazily (see [dictionaries_lazy_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)), so instead of being loaded automatically at startup, they are initialized on first access through dictGet function or SELECT from tables with ENGINE = Dictionary. The `SYSTEM RELOAD DICTIONARIES` query reloads such dictionaries (LOADED).
Always returns `Ok.` regardless of the result of the dictionary update.

## RELOAD DICTIONARY {#query_language-system-reload-dictionary}

Completely reloads a dictionary `dictionary_name`, regardless of the state of the dictionary (LOADED / NOT_LOADED / FAILED).
Always returns `Ok.` regardless of the result of updating the dictionary.
The status of the dictionary can be checked by querying the `system.dictionaries` table.

``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

Resets ClickHouse’s internal DNS cache. Sometimes (for old ClickHouse versions) it is necessary to use this command when changing the infrastructure (changing the IP address of another ClickHouse server or the server used by dictionaries).

For more convenient (automatic) cache management, see disable_internal_dns_cache, dns_cache_update_period parameters.

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

Resets the mark cache. Used in development of ClickHouse and performance tests.

## DROP REPLICA {#query_language-system-drop-replica}

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

## DROP UNCOMPRESSED CACHE {#query_language-system-drop-uncompressed-cache}

Reset the uncompressed data cache. Used in development of ClickHouse and performance tests.
For manage uncompressed data cache parameters use following server level settings [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) and query/user/profile level settings [use_uncompressed_cache](../../operations/settings/settings.md#setting-use_uncompressed_cache)

## DROP COMPILED EXPRESSION CACHE {#query_language-system-drop-compiled-expression-cache}

Reset the compiled expression cache. Used in development of ClickHouse and performance tests.
Complied expression cache used when query/user/profile enable option [compile](../../operations/settings/settings.md#compile)

## FLUSH LOGS {#query_language-system-flush_logs}

Flushes buffers of log messages to system tables (e.g. system.query_log). Allows you to not wait 7.5 seconds when debugging.
This will also create system tables even if message queue is empty.

## RELOAD CONFIG {#query_language-system-reload-config}

Reloads ClickHouse configuration. Used when configuration is stored in ZooKeeeper.

## SHUTDOWN {#query_language-system-shutdown}

Normally shuts down ClickHouse (like `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

Aborts ClickHouse process (like `kill -9 {$ pid_clickhouse-server}`)

## Managing Distributed Tables {#query-language-system-distributed}

ClickHouse can manage [distributed](../../engines/table-engines/special/distributed.md) tables. When a user inserts data into these tables, ClickHouse first creates a queue of the data that should be sent to cluster nodes, then asynchronously sends it. You can manage queue processing with the [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed), and [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) queries. You can also synchronously insert distributed data with the [insert_distributed_sync](../../operations/settings/settings.md#insert_distributed_sync) setting.

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

Disables background data distribution when inserting data into distributed tables.

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

Forces ClickHouse to send data to cluster nodes synchronously. If any nodes are unavailable, ClickHouse throws an exception and stops query execution. You can retry the query until it succeeds, which will happen when all nodes are back online.

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

Enables background data distribution when inserting data into distributed tables.

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

## Managing MergeTree Tables {#query-language-system-mergetree}

ClickHouse can manage background processes in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

### STOP MERGES {#query_language-system-stop-merges}

Provides possibility to stop background merges for tables in the MergeTree family:

``` sql
SYSTEM STOP MERGES [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

!!! note "Note"
    `DETACH / ATTACH` table will start background merges for the table even in case when merges have been stopped for all MergeTree tables before.

### START MERGES {#query_language-system-start-merges}

Provides possibility to start background merges for tables in the MergeTree family:

``` sql
SYSTEM START MERGES [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

### STOP TTL MERGES {#query_language-stop-ttl-merges}

Provides possibility to stop background delete old data according to [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) for tables in the MergeTree family:
Return `Ok.` even table doesn’t exists or table have not MergeTree engine. Return error when database doesn’t exists:

``` sql
SYSTEM STOP TTL MERGES [[db.]merge_tree_family_table_name]
```

### START TTL MERGES {#query_language-start-ttl-merges}

Provides possibility to start background delete old data according to [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) for tables in the MergeTree family:
Return `Ok.` even table doesn’t exists. Return error when database doesn’t exists:

``` sql
SYSTEM START TTL MERGES [[db.]merge_tree_family_table_name]
```

### STOP MOVES {#query_language-stop-moves}

Provides possibility to stop background move data according to [TTL table expression with TO VOLUME or TO DISK clause](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) for tables in the MergeTree family:
Return `Ok.` even table doesn’t exists. Return error when database doesn’t exists:

``` sql
SYSTEM STOP MOVES [[db.]merge_tree_family_table_name]
```

### START MOVES {#query_language-start-moves}

Provides possibility to start background move data according to [TTL table expression with TO VOLUME and TO DISK clause](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) for tables in the MergeTree family:
Return `Ok.` even table doesn’t exists. Return error when database doesn’t exists:

``` sql
SYSTEM STOP MOVES [[db.]merge_tree_family_table_name]
```

## Managing ReplicatedMergeTree Tables {#query-language-system-replicated}

ClickHouse can manage background replication related processes in [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication/#table_engines-replication) tables.

### STOP FETCHES {#query_language-system-stop-fetches}

Provides possibility to stop background fetches for inserted parts for tables in the `ReplicatedMergeTree` family:
Always returns `Ok.` regardless of the table engine and even table or database doesn’t exists.

``` sql
SYSTEM STOP FETCHES [[db.]replicated_merge_tree_family_table_name]
```

### START FETCHES {#query_language-system-start-fetches}

Provides possibility to start background fetches for inserted parts for tables in the `ReplicatedMergeTree` family:
Always returns `Ok.` regardless of the table engine and even table or database doesn’t exists.

``` sql
SYSTEM START FETCHES [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATED SENDS {#query_language-system-start-replicated-sends}

Provides possibility to stop background sends to other replicas in cluster for new inserted parts for tables in the `ReplicatedMergeTree` family:

``` sql
SYSTEM STOP REPLICATED SENDS [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATED SENDS {#query_language-system-start-replicated-sends}

Provides possibility to start background sends to other replicas in cluster for new inserted parts for tables in the `ReplicatedMergeTree` family:

``` sql
SYSTEM START REPLICATED SENDS [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATION QUEUES {#query_language-system-stop-replication-queues}

Provides possibility to stop background fetch tasks from replication queues which stored in Zookeeper for tables in the `ReplicatedMergeTree` family. Possible background tasks types - merges, fetches, mutation, DDL statements with ON CLUSTER clause:

``` sql
SYSTEM STOP REPLICATION QUEUES [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATION QUEUES {#query_language-system-start-replication-queues}

Provides possibility to start background fetch tasks from replication queues which stored in Zookeeper for tables in the `ReplicatedMergeTree` family. Possible background tasks types - merges, fetches, mutation, DDL statements with ON CLUSTER clause:

``` sql
SYSTEM START REPLICATION QUEUES [[db.]replicated_merge_tree_family_table_name]
```

### SYNC REPLICA {#query_language-system-sync-replica}

Wait until a `ReplicatedMergeTree` table will be synced with other replicas in a cluster. Will run until `receive_timeout` if fetches currently disabled for the table.

``` sql
SYSTEM SYNC REPLICA [db.]replicated_merge_tree_family_table_name
```

### RESTART REPLICA {#query_language-system-restart-replica}

Provides possibility to reinitialize Zookeeper sessions state for `ReplicatedMergeTree` table, will compare current state with Zookeeper as source of true and add tasks to Zookeeper queue if needed
Initialization replication quene based on ZooKeeper date happens in the same way as `ATTACH TABLE` statement. For a short time the table will be unavailable for any operations.

``` sql
SYSTEM RESTART REPLICA [db.]replicated_merge_tree_family_table_name
```

### RESTART REPLICAS {#query_language-system-restart-replicas}

Provides possibility to reinitialize Zookeeper sessions state for all `ReplicatedMergeTree` tables, will compare current state with Zookeeper as source of true and add tasks to Zookeeper queue if needed

[Original article](https://clickhouse.tech/docs/en/query_language/system/) <!--hide-->
