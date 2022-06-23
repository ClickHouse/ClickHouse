---
toc_priority: 37
toc_title: SYSTEM
---

# SYSTEM Queries {#query-language-system}

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

## RELOAD EMBEDDED DICTIONARIES\] {#query_language-system-reload-emdedded-dictionaries}

重新加载所有[内置字典](../../sql-reference/dictionaries/internal-dicts.md)。默认情况下内置字典是禁用的。
总是返回 ‘OK.’，不管这些内置字典的更新结果如何。


## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

重载已经被成功加载过的所有字典。
默认情况下，字典是延时加载的（ [dictionaries_lazy_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)），不是在服务启动时自动加载，而是在第一次使用dictGet函数或通过 `SELECT from tables with ENGINE = Dictionary` 进行访问时被初始化。这个命令 `SYSTEM RELOAD DICTIONARIES` 就是针对这类表进行重新加载的。


## RELOAD DICTIONARY Dictionary_name {#query_language-system-reload-dictionary}

完全重新加载指定字典 `dictionary_name`，不管该字典的状态如何(LOADED / NOT_LOADED / FAILED)。不管字典的更新结果如何，总是返回 `OK.`
字典的状态可以通过查询  `system.dictionaries`表来检查。


``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

重置CH的dns缓存。有时候（对于旧的ClickHouse版本）当某些底层环境发生变化时（修改其它Clickhouse服务器的ip或字典所在服务器的ip），需要使用该命令。
更多自动化的缓存管理相关信息，参见disable_internal_dns_cache, dns_cache_update_period这些参数。


## DROP MARK CACHE {#query_language-system-drop-mark-cache}

重置mark缓存。在进行ClickHouse开发或性能测试时使用。

## DROP REPLICA {#query_language-system-drop-replica}

使用下面的语句可以删除已经无效的副本。

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

该操作将副本的路径从Zookeeper中删除。当副本失效，并且由于该副本已经不存在导致它的元数据不能通过 `DROP TABLE`从zookeeper中删除，这种情形下可以使用该命令。它只会删除失效或过期的副本，不会删除本地的副本。请使用 `DROP TABLE` 来删除本地副本。 `DROP REPLICA` 不会删除任何表，并且不会删除磁盘上的任何数据或元数据信息。

第1条语句：删除 `database.table`表的 `replica_name`副本的元数据
第2条语句：删除 `database` 数据库的 所有`replica_name`副本的元数据
第3条语句：删除本地服务器所有 `replica_name`副本的元数据
第4条语句：用于在表的其它所有副本都删除时，删除已失效副本的元数据。使用时需要明确指定表的路径。该路径必须和创建表时 `ReplicatedMergeTree`引擎的第一个参数一致。

## DROP UNCOMPRESSED CACHE {#query_language-system-drop-uncompressed-cache}

重置未压缩数据的缓存。用于ClickHouse开发和性能测试。
管理未压缩数据缓存的参数，使用以下的服务器级别设置 [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size)以及 `query/user/profile`级别设置 [use_uncompressed_cache](../../operations/settings/settings.md#setting-use_uncompressed_cache)


## DROP COMPILED EXPRESSION CACHE {#query_language-system-drop-compiled-expression-cache}

重置已编译的表达式缓存。用于ClickHouse开发和性能测试。
当 `query/user/profile` 启用配置项 [compile-expressions](../../operations/settings/settings.md#compile-expressions)时，编译的表达式缓存开启。

## FLUSH LOGS {#query_language-system-flush_logs}

将日志信息缓冲数据刷入系统表（例如system.query_log）。调试时允许等待不超过7.5秒。当信息队列为空时，会创建系统表。

## RELOAD CONFIG {#query_language-system-reload-config}

重新加载ClickHouse的配置。用于当配置信息存放在ZooKeeper时。

## SHUTDOWN {#query_language-system-shutdown}

关闭ClickHouse服务（类似于 `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`）

## KILL {#query_language-system-kill}

关闭ClickHouse进程 （ `kill -9 {$ pid_clickhouse-server}`）

## Managing Distributed Tables {#query-language-system-distributed}

ClickHouse可以管理 [distribute](../../engines/table-engines/special/distributed.md)表。当用户向这类表插入数据时，ClickHouse首先为需要发送到集群节点的数据创建一个队列，然后异步的发送它们。你可以维护队列的处理过程，通过[STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed), 以及 [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)。你也可以设置 `insert_distributed_sync`参数来以同步的方式插入分布式数据。


### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

当向分布式表插入数据时，禁用后台的分布式数据分发。

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

强制让ClickHouse同步向集群节点同步发送数据。如果有节点失效，ClickHouse抛出异常并停止插入操作。当所有节点都恢复上线时，你可以重试之前的操作直到成功执行。

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

当向分布式表插入数据时，允许后台的分布式数据分发。

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

## Managing MergeTree Tables {#query-language-system-mergetree}

ClickHouse可以管理 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)表的后台处理进程。

### STOP MERGES {#query_language-system-stop-merges}

为MergeTree系列引擎表停止后台合并操作。

``` sql
SYSTEM STOP MERGES [[db.]merge_tree_family_table_name]
```


!!! note "Note"
    `DETACH / ATTACH` 表操作会在后台进行表的merge操作，甚至当所有MergeTree表的合并操作已经停止的情况下。


### START MERGES {#query_language-system-start-merges}

为MergeTree系列引擎表启动后台合并操作。

``` sql
SYSTEM START MERGES [[db.]merge_tree_family_table_name]
```

### STOP TTL MERGES {#query_language-stop-ttl-merges}

根据 [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)，为MergeTree系列引擎表停止后台删除旧数据。
不管表存在与否，都返回 `OK.`。当数据库不存在时返回错误。

``` sql
SYSTEM STOP TTL MERGES [[db.]merge_tree_family_table_name]
```

### START TTL MERGES {#query_language-start-ttl-merges}

根据 [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)，为MergeTree系列引擎表启动后台删除旧数据。不管表存在与否，都返回 `OK.`。当数据库不存在时返回错误。


``` sql
SYSTEM START TTL MERGES [[db.]merge_tree_family_table_name]
```

### STOP MOVES {#query_language-stop-moves}

根据 [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)，为MergeTree系列引擎表停止后台移动数据。不管表存在与否，都返回 `OK.`。当数据库不存在时返回错误。


``` sql
SYSTEM STOP MOVES [[db.]merge_tree_family_table_name]
```

### START MOVES {#query_language-start-moves}

根据 [TTL expression](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)，为MergeTree系列引擎表启动后台移动数据。不管表存在与否，都返回 `OK.`。当数据库不存在时返回错误。


``` sql
SYSTEM STOP MOVES [[db.]merge_tree_family_table_name]
```

## Managing ReplicatedMergeTree Tables {#query-language-system-replicated}

管理 [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replacingmergetree.md)表的后台复制相关进程。

### STOP FETCHES {#query_language-system-stop-fetches}

停止后台获取 `ReplicatedMergeTree`系列引擎表中插入的数据块。
不管表引擎类型如何或表/数据库是否存，都返回 `OK.`。

``` sql
SYSTEM STOP FETCHES [[db.]replicated_merge_tree_family_table_name]
```

### START FETCHES {#query_language-system-start-fetches}

启动后台获取 `ReplicatedMergeTree`系列引擎表中插入的数据块。
不管表引擎类型如何或表/数据库是否存，都返回 `OK.`。

``` sql
SYSTEM START FETCHES [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATED SENDS {#query_language-system-start-replicated-sends}

停止通过后台分发 `ReplicatedMergeTree`系列引擎表中新插入的数据块到集群的其它副本节点。

``` sql
SYSTEM STOP REPLICATED SENDS [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATED SENDS {#query_language-system-start-replicated-sends}

启动通过后台分发 `ReplicatedMergeTree`系列引擎表中新插入的数据块到集群的其它副本节点。

``` sql
SYSTEM START REPLICATED SENDS [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATION QUEUES {#query_language-system-stop-replication-queues}


停止从Zookeeper中获取 `ReplicatedMergeTree`系列表的复制队列的后台任务。可能的后台任务类型包含：merges, fetches, mutation，带有 `ON CLUSTER`的ddl语句

``` sql
SYSTEM STOP REPLICATION QUEUES [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATION QUEUES {#query_language-system-start-replication-queues}

启动从Zookeeper中获取 `ReplicatedMergeTree`系列表的复制队列的后台任务。可能的后台任务类型包含：merges, fetches, mutation，带有 `ON CLUSTER`的ddl语句

``` sql
SYSTEM START REPLICATION QUEUES [[db.]replicated_merge_tree_family_table_name]
```

### SYNC REPLICA {#query_language-system-sync-replica}
直到 `ReplicatedMergeTree`表将要和集群的其它副本进行同步之前会一直运行。如果当前对表的获取操作禁用的话，在达到 `receive_timeout`之前会一直运行。


``` sql
SYSTEM SYNC REPLICA [db.]replicated_merge_tree_family_table_name
```

### RESTART REPLICA {#query_language-system-restart-replica}

重置 `ReplicatedMergeTree`表的Zookeeper会话状态。该操作会以Zookeeper为参照，对比当前状态，有需要的情况下将任务添加到ZooKeeper队列。
基于ZooKeeper的日期初始化复制队列，类似于 `ATTACH TABLE`语句。短时间内不能对表进行任何操作。


``` sql
SYSTEM RESTART REPLICA [db.]replicated_merge_tree_family_table_name
```

### RESTART REPLICAS {#query_language-system-restart-replicas}

重置所有 `ReplicatedMergeTree`表的ZooKeeper会话状态。该操作会以Zookeeper为参照，对比当前状态，有需要的情况下将任务添加到ZooKeeper队列。

[原始文档](https://clickhouse.tech/docs/en/query_language/system/) <!--hide-->
