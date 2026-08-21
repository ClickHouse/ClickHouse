---
description: 'Documentation for SYSTEM Statements'
sidebar_label: 'SYSTEM'
sidebar_position: 36
slug: /sql-reference/statements/system
title: 'SYSTEM Statements'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# SYSTEM Statements

## SYSTEM RELOAD EMBEDDED DICTIONARIES {#reload-embedded-dictionaries}

Reload all [Internal dictionaries](./create/dictionary/index.md).
By default, internal dictionaries are disabled.
Always returns `Ok.` regardless of the result of the internal dictionary update.

## SYSTEM RELOAD DICTIONARIES {#reload-dictionaries}

The `SYSTEM RELOAD DICTIONARIES` query reloads dictionaries with a status of `LOADED` (see the `status` column of [`system.dictionaries`](/operations/system-tables/dictionaries)), i.e dictionaries that have been successfully loaded before.
By default, dictionaries are loaded lazily (see [dictionaries_lazy_load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load)), so instead of being loaded automatically at startup, they are initialized on first access through use of the [`dictGet`](/sql-reference/functions/ext-dict-functions#dictGet) function or use of `SELECT` from tables with `ENGINE = Dictionary`.

**Syntax**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

## SYSTEM RELOAD DICTIONARY {#reload-dictionary}

Completely reloads a dictionary `dictionary_name`, regardless of the state of the dictionary (LOADED / NOT_LOADED / FAILED).
Always returns `Ok.` regardless of the result of updating the dictionary.

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

The status of the dictionary can be checked by querying the `system.dictionaries` table.

```sql
SELECT name, status FROM system.dictionaries;
```

## SYSTEM RELOAD MODELS {#reload-models}

:::note
This statement and `SYSTEM RELOAD MODEL` merely unload catboost models from the clickhouse-library-bridge. The function `catboostEvaluate()`
loads a model upon first access if it is not loaded yet.
:::

Unloads all CatBoost models.

**Syntax**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

## SYSTEM RELOAD MODEL {#reload-model}

Unloads a CatBoost model at `model_path`.

**Syntax**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

## SYSTEM RELOAD FUNCTIONS {#reload-functions}

Reloads all registered [executable user defined functions](/sql-reference/functions/udf#executable-user-defined-functions) or one of them from a configuration file.

**Syntax**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

## SYSTEM RELOAD ASYNCHRONOUS METRICS {#reload-asynchronous-metrics}

Re-calculates all [asynchronous metrics](../../operations/system-tables/asynchronous_metrics.md). Since asynchronous metrics are periodically updated based on setting [asynchronous_metrics_update_period_s](../../operations/server-configuration-parameters/settings.md), updating them manually using this statement is typically not necessary.

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

## SYSTEM CLEAR|DROP DNS CACHE {#drop-dns-cache}

Clears ClickHouse's internal DNS cache. Sometimes (for old ClickHouse versions) it is necessary to use this command when changing the infrastructure (changing the IP address of another ClickHouse server or the server used by dictionaries).

For more convenient (automatic) cache management, see `disable_internal_dns_cache`, `dns_cache_max_entries`, `dns_cache_update_period` parameters.

## SYSTEM CLEAR|DROP MARK CACHE {#drop-mark-cache}

Clears the mark cache.

## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE {#drop-iceberg-metadata-cache}

Clears the iceberg metadata cache.

## SYSTEM CLEAR|DROP TEXT INDEX CACHES {#drop-text-index-caches}

Clears the text index's header, dictionary and postings caches.

If you like to drop one of these caches individually, you can run

- `SYSTEM CLEAR TEXT INDEX HEADER CACHE`,
- `SYSTEM CLEAR TEXT INDEX DICTIONARY CACHE`, or
- `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

## SYSTEM DROP REPLICA {#drop-replica}

Dead replicas of `ReplicatedMergeTree` tables can be dropped using following syntax:

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

Queries will remove the `ReplicatedMergeTree` replica path in ZooKeeper. It is useful when the replica is dead and its metadata cannot be removed from ZooKeeper by `DROP TABLE` because there is no such table anymore. It will only drop the inactive/stale replica, and it cannot drop local replica, please use `DROP TABLE` for that. `DROP REPLICA` does not drop any tables and does not remove any data or metadata from disk.

The first one removes metadata of `'replica_name'` replica of `database.table` table.
The second one does the same for all replicated tables in the database.
The third one does the same for all replicated tables on the local server.
The fourth one is useful to remove metadata of dead replica when all other replicas of a table were dropped. It requires the table path to be specified explicitly. It must be the same path as was passed to the first argument of `ReplicatedMergeTree` engine on table creation.

## SYSTEM DROP DATABASE REPLICA {#drop-database-replica}

Dead replicas of `Replicated` databases can be dropped using following syntax:

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

Similar to `SYSTEM DROP REPLICA`, but removes the `Replicated` database replica path from ZooKeeper when there's no database to run `DROP DATABASE`. Please note that it does not remove `ReplicatedMergeTree` replicas (so you may need `SYSTEM DROP REPLICA` as well). Shard and replica names are the names that were specified in `Replicated` engine arguments when creating the database. Also, these names can be obtained from `database_shard_name` and `database_replica_name` columns in `system.clusters`. If the `FROM SHARD` clause is missing, then `replica_name` must be a full replica name in `shard_name|replica_name` format.

## SYSTEM CLEAR|DROP UNCOMPRESSED CACHE {#drop-uncompressed-cache}

Clears the uncompressed data cache.
The uncompressed data cache is enabled/disabled with the query/user/profile-level setting [`use_uncompressed_cache`](../../operations/settings/settings.md#use_uncompressed_cache).
Its size can be configured using the server-level setting [`uncompressed_cache_size`](../../operations/server-configuration-parameters/settings.md#uncompressed_cache_size).

## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE {#drop-compiled-expression-cache}

Clears the compiled expression cache.
The compiled expression cache is enabled/disabled with the query/user/profile-level setting [`compile_expressions`](../../operations/settings/settings.md#compile_expressions).

## SYSTEM CLEAR|DROP QUERY CONDITION CACHE {#drop-query-condition-cache}

Clears the query condition cache.

## SYSTEM CLEAR|DROP QUERY CACHE {#drop-query-cache}

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
````

Clears the [query cache](../../operations/query-cache.md).
If a tag is specified, only query cache entries with the specified tag are deleted.

## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE {#system-drop-schema-format}

Clears cache for schemas loaded from [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path).

Supported targets:
- Protobuf: Removes imported Protobuf message definitions from memory.
- Files: Deletes cached schema files stored locally in the [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path), generated when `format_schema_source` is set to `query`.
Note: If no target is specified, both caches are cleared.

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

## SYSTEM FLUSH LOGS {#flush-logs}

Flushes buffered log messages to system tables, e.g. system.query_log. Mainly useful for debugging since most system tables have a default flush interval of 7.5 seconds.
This will also create system tables even if message queue is empty.

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

If you don't want to flush everything, you can flush one or more individual logs by passing either their name or their target table:

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

## SYSTEM RELOAD CONFIG {#reload-config}

Reloads ClickHouse configuration. Used when configuration is stored in ZooKeeper. Note that `SYSTEM RELOAD CONFIG` does not reload `USER` configuration stored in ZooKeeper, it only reloads `USER` configuration that is stored in `users.xml`.  To reload all `USER` config use `SYSTEM RELOAD USERS`

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

## SYSTEM RELOAD USERS {#reload-users}

Reloads all access storages, including: users.xml, local disk access storage, replicated (in ZooKeeper) access storage.

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

## SYSTEM SHUTDOWN {#shutdown}

<CloudNotSupportedBadge/>

Normally shuts down ClickHouse (like `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## SYSTEM KILL {#kill}

Aborts ClickHouse process (like `kill -9 {$ pid_clickhouse-server}`)

## SYSTEM INSTRUMENT {#instrument}

Manages instrumentation points using LLVM's XRay feature which is available when ClickHouse is built using `ENABLE_XRAY=1`.
This enables to debug and profile in production without modifying the source code and with minimal overhead.
When no instrumentation point is added, the performance penalty is negligible because it only adds an extra jump to a nearby
address at the prolog and epilog of those functions that are longer than 200 instructions.

### SYSTEM INSTRUMENT ADD {#instrument-add}

Adds a new instrumentation point. Functions instrumented can be inspected in the [`system.instrumentation`](../../operations/system-tables/instrumentation.md) system table. More than one handler can be added for the same function, and they will be executed in the same order the instrumentation is added.
The functions to be instrumented can be collected from [`system.symbols`](../../operations/system-tables/symbols.md) system table.

There are three different kind of handlers to add to functions:

**Syntax**
```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [PARAMETERS]
```

where `FUNCTION` is any function or substring of a function such as `QueryMetricLog::startQuery`, and the handler one of the following

#### LOG {#instrument-add-log}

Prints the text provided as an argument and the stack trace either on `ENTRY` or `EXIT` of the function.

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

#### SLEEP {#instrument-add-sleep}

Sleeps for a number of fix amount of seconds either on `ENTRY` or `EXIT`:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

or for a uniformly distributed random amount of seconds providing min and max separated by a whitespace:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

#### PROFILE {#instrument-add-profile}

Measures the time spent between `ENTRY` and `EXIT` of a function.
The result of the profiling is stored in [`system.trace_log`](../../operations/system-tables/trace_log.md) and can be converted
to [Chrome Event Trace Format](../../operations/system-tables/trace_log.md#chrome-event-trace-format).

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

### SYSTEM INSTRUMENT REMOVE {#instrument-remove}

Removes either a single instrumentation point with:

```sql
SYSTEM INSTRUMENT REMOVE ID
```

all of them using the `ALL` parameter:

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

a set of IDs from a subquery:

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

or all instrumentation points that match a given function_name:

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

The instrumentation point information can be collected from [`system.instrumentation`](../../operations/system-tables/instrumentation.md) system table.

## Managing Distributed Tables {#managing-distributed-tables}

ClickHouse can manage [distributed](../../engines/table-engines/special/distributed.md) tables. When a user inserts data into these tables, ClickHouse first creates a queue of the data that should be sent to cluster nodes, then asynchronously sends it. You can manage queue processing with the [`STOP DISTRIBUTED SENDS`](#stop-distributed-sends), [FLUSH DISTRIBUTED](#flush-distributed), and [`START DISTRIBUTED SENDS`](#start-distributed-sends) queries. You can also synchronously insert distributed data with the [`distributed_foreground_insert`](../../operations/settings/settings.md#distributed_foreground_insert) setting.

### SYSTEM STOP DISTRIBUTED SENDS {#stop-distributed-sends}

Disables background data distribution when inserting data into distributed tables.

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

:::note
In case of [`prefer_localhost_replica`](../../operations/settings/settings.md#prefer_localhost_replica) is enabled (the default), the data to local shard will be inserted anyway.
:::

### SYSTEM FLUSH DISTRIBUTED {#flush-distributed}

Forces ClickHouse to send data to cluster nodes synchronously. If any nodes are unavailable, ClickHouse throws an exception and stops query execution. You can retry the query until it succeeds, which will happen when all nodes are back online.

You can also override some settings via `SETTINGS` clause, this can be useful to avoid some temporary limitations, like `max_concurrent_queries_for_all_users` or `max_memory_usage`.

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
Each pending block is stored in disk with settings from the initial INSERT query, so that is why sometimes you may want to override settings.
:::

### SYSTEM START DISTRIBUTED SENDS {#start-distributed-sends}

Enables background data distribution when inserting data into distributed tables.

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

### SYSTEM STOP LISTEN {#stop-listen}

Closes the socket and gracefully terminates the existing connections to the server on the specified port with the specified protocol.

However, if the corresponding protocol settings were not specified in the clickhouse-server configuration, this command will have no effect.

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

- If `CUSTOM 'protocol'` modifier is specified, the custom protocol with the specified name defined in the protocols section of the server configuration will be stopped.
- If `QUERIES ALL [EXCEPT .. [,..]]` modifier is specified, all protocols are stopped, unless specified with `EXCEPT` clause.
- If `QUERIES DEFAULT [EXCEPT .. [,..]]` modifier is specified, all default protocols are stopped, unless specified with `EXCEPT` clause.
- If `QUERIES CUSTOM [EXCEPT .. [,..]]` modifier is specified, all custom protocols are stopped, unless specified with `EXCEPT` clause.

### SYSTEM START LISTEN {#start-listen}

Allows new connections to be established on the specified protocols.

However, if the server on the specified port and protocol was not stopped using the SYSTEM STOP LISTEN command, this command will have no effect.

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

## Managing MergeTree Tables {#managing-mergetree-tables}

ClickHouse can manage background processes in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

### SYSTEM STOP MERGES {#stop-merges}

<CloudNotSupportedBadge/>

Provides possibility to stop background merges for tables in the MergeTree family:

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
`DETACH / ATTACH` table will start background merges for the table even in case when merges have been stopped for all MergeTree tables before.
:::

### SYSTEM START MERGES {#start-merges}

<CloudNotSupportedBadge/>

Provides possibility to start background merges for tables in the MergeTree family:

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

### SYSTEM STOP TTL MERGES {#stop-ttl-merges}

Provides possibility to stop background delete old data according to [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist or table has not MergeTree engine. Returns error when database does not exist:

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### SYSTEM START TTL MERGES {#start-ttl-merges}

Provides possibility to start background delete old data according to [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist. Returns error when database does not exist:

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### SYSTEM STOP MOVES {#stop-moves}

Provides possibility to stop background move data according to [TTL table expression with TO VOLUME or TO DISK clause](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist. Returns error when database does not exist:

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### SYSTEM START MOVES {#start-moves}

Provides possibility to start background move data according to [TTL table expression with TO VOLUME and TO DISK clause](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist. Returns error when database does not exist:

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### SYSTEM SYSTEM UNFREEZE {#query_language-system-unfreeze}

Clears a frozen backup with the specified name from all the disks. See more about unfreezing separate parts in [ALTER TABLE table_name UNFREEZE WITH NAME ](/sql-reference/statements/alter/partition#unfreeze-partition)

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

### SYSTEM WAIT LOADING PARTS {#wait-loading-parts}

Wait until all asynchronously loading data parts of a table (outdated data parts) will became loaded.

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

## Managing ReplicatedMergeTree Tables {#managing-replicatedmergetree-tables}

ClickHouse can manage background replication related processes in [ReplicatedMergeTree](/engines/table-engines/mergetree-family/replication) tables.

### SYSTEM STOP FETCHES {#stop-fetches}

<CloudNotSupportedBadge/>

Provides possibility to stop background fetches for inserted parts for tables in the `ReplicatedMergeTree` family:
Always returns `Ok.` regardless of the table engine and even if table or database does not exist.

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM START FETCHES {#start-fetches}

<CloudNotSupportedBadge/>

Provides possibility to start background fetches for inserted parts for tables in the `ReplicatedMergeTree` family:
Always returns `Ok.` regardless of the table engine and even if table or database does not exist.

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM STOP REPLICATED SENDS {#stop-replicated-sends}

Provides possibility to stop background sends to other replicas in cluster for new inserted parts for tables in the `ReplicatedMergeTree` family:

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM START REPLICATED SENDS {#start-replicated-sends}

Provides possibility to start background sends to other replicas in cluster for new inserted parts for tables in the `ReplicatedMergeTree` family:

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM STOP REPLICATION QUEUES {#stop-replication-queues}

Provides possibility to stop background fetch tasks from replication queues which stored in Zookeeper for tables in the `ReplicatedMergeTree` family. Possible background tasks types - merges, fetches, mutation, DDL statements with ON CLUSTER clause:

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM START REPLICATION QUEUES {#start-replication-queues}

Provides possibility to start background fetch tasks from replication queues which stored in Zookeeper for tables in the `ReplicatedMergeTree` family. Possible background tasks types - merges, fetches, mutation, DDL statements with ON CLUSTER clause:

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM STOP PULLING REPLICATION LOG {#stop-pulling-replication-log}

Stops loading new entries from replication log to replication queue in a `ReplicatedMergeTree` table.

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM START PULLING REPLICATION LOG {#start-pulling-replication-log}

Cancels `SYSTEM STOP PULLING REPLICATION LOG`.

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM SYNC REPLICA {#sync-replica}

Wait until a `ReplicatedMergeTree` table will be synced with other replicas in a cluster, but no more than `receive_timeout` seconds.

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

After running this statement the `[db.]replicated_merge_tree_family_table_name` fetches commands from the common replicated log into its own replication queue, and then the query waits till the replica processes all of the fetched commands. The following modifiers are supported:

- With `IF EXISTS` (available since 25.6) the query won't throw an error if the table does not exists. This is useful when adding a new replica to a cluster, when it's already part of the cluster configuration but it is still in the process of creating and synchronizing the table.
- If a `STRICT` modifier was specified then the query waits for the replication queue to become empty. The `STRICT` version may never succeed if new entries constantly appear in the replication queue.
- If a `LIGHTWEIGHT` modifier was specified then the query waits only for `GET_PART`, `ATTACH_PART`, `DROP_RANGE`, `REPLACE_RANGE` and `DROP_PART` entries to be processed.
  Additionally, the LIGHTWEIGHT modifier supports an optional FROM 'srcReplicas' clause, where 'srcReplicas' is a comma-separated list of source replica names. This extension allows for more targeted synchronization by focusing only on replication tasks originating from the specified source replicas.
- If a `PULL` modifier was specified then the query pulls new replication queue entries from ZooKeeper, but does not wait for anything to be processed.

### SYNC DATABASE REPLICA {#sync-database-replica}

Waits until the specified [replicated database](/engines/database-engines/replicated) applies all schema changes from the DDL queue of that database.

**Syntax**
```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

### SYSTEM RESTART REPLICA {#restart-replica}

Provides possibility to reinitialize Zookeeper session's state for `ReplicatedMergeTree` table, will compare current state with Zookeeper as source of truth and add tasks to Zookeeper queue if needed.
Initialization of replication queue based on ZooKeeper data happens in the same way as for `ATTACH TABLE` statement. For a short time, the table will be unavailable for any operations.

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

### SYSTEM RESTORE REPLICA {#restore-replica}

Restores a replica if data is [possibly] present but Zookeeper metadata is lost.

Works only on readonly `ReplicatedMergeTree` tables.

One may execute query after:

- ZooKeeper root `/` loss.
- Replicas path `/replicas` loss.
- Individual replica path `/replicas/replica_name/` loss.

Replica attaches locally found parts and sends info about them to Zookeeper.
Parts present on a replica before metadata loss are not re-fetched from other ones if not being outdated (so replica restoration does not mean re-downloading all data over the network).

:::note
Parts in all states are moved to `detached/` folder. Parts active before data loss (committed) are attached.
:::

### SYSTEM RESTORE DATABASE REPLICA {#restore-database-replica}

Restores a replica if data is [possibly] present but Zookeeper metadata is lost.

**Syntax**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**Example**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

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

### SYSTEM RESTART REPLICAS {#restart-replicas}

Provides possibility to reinitialize Zookeeper sessions state for all `ReplicatedMergeTree` tables, will compare current state with Zookeeper as source of true and add tasks to Zookeeper queue if needed

### SYSTEM CLEAR|DROP FILESYSTEM CACHE {#drop-filesystem-cache}

Allows to drop filesystem cache.

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

### SYSTEM SYNC FILE CACHE {#sync-file-cache}

:::note
It's too heavy and has potential for misuse.
:::

Will do sync syscall.

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

### SYSTEM LOAD PRIMARY KEY {#load-primary-key}

Load the primary keys for the given table or for all tables.

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

### SYSTEM UNLOAD PRIMARY KEY {#unload-primary-key}

Unload the primary keys for the given table or for all tables.

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

## Managing Refreshable Materialized Views {#refreshable-materialized-views}

Commands to control background tasks performed by [Refreshable Materialized Views](../../sql-reference/statements/create/view.md#refreshable-materialized-view)

Keep an eye on [`system.view_refreshes`](../../operations/system-tables/view_refreshes.md) while using them.

### SYSTEM REFRESH VIEW {#refresh-view}

Trigger an immediate out-of-schedule refresh of a given view.

```sql
SYSTEM REFRESH VIEW [db.]name
```

### SYSTEM WAIT VIEW {#wait-view}

Wait for the currently running refresh to complete. If the refresh fails, throws an exception. If no refresh is running, completes immediately, throwing an exception if previous refresh failed.

### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS {#stop-view-stop-views}

Disable periodic refreshing of the given view or all refreshable views. If a refresh is in progress, cancel it too.

If the view is in a Replicated or Shared database, `STOP VIEW` only affects the current replica, while `STOP REPLICATED VIEW` affects all replicas.

:::note
The stopped state does not persist across server restarts. After a restart, views will resume their configured refresh schedules.
In Replicated or Shared databases, `SYSTEM STOP VIEW` only affects the current replica. Use `SYSTEM STOP REPLICATED VIEW` to stop refreshes on all replicas.
:::

```sql
SYSTEM STOP VIEW [db.]name
```
```sql
SYSTEM STOP VIEWS
```

### SYSTEM START [REPLICATED] VIEW, START VIEWS {#start-view-start-views}

Enable periodic refreshing for the given view or all refreshable views. No immediate refresh is triggered.

If the view is in a Replicated or Shared database, `START VIEW` undoes the effect of `STOP VIEW`, and `START REPLICATED VIEW` undoes the effect of `STOP REPLICATED VIEW`.

```sql
SYSTEM START VIEW [db.]name
```
```sql
SYSTEM START VIEWS
```

### SYSTEM CANCEL VIEW {#cancel-view}

If there's a refresh in progress for the given view on the current replica, interrupt and cancel it. Otherwise do nothing.

```sql
SYSTEM CANCEL VIEW [db.]name
```

### SYSTEM WAIT VIEW {#system-wait-view}

Waits for the running refresh to complete. If no refresh is running, returns immediately. If the latest refresh attempt failed, reports an error.

Can be used right after creating a new refreshable materialized view (without EMPTY keyword) to wait for the initial refresh to complete.

If the view is in a Replicated or Shared database, and refresh is running on another replica, waits for that refresh to complete.

```sql
SYSTEM WAIT VIEW [db.]name
```
