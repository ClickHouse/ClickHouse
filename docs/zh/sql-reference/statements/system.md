---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_priority: 37
toc_title: SYSTEM
---

# 系统查询 {#query-language-system}

-   [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
-   [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
-   [DROP DNS CACHE](#query_language-system-drop-dns-cache)
-   [DROP MARK CACHE](#query_language-system-drop-mark-cache)
-   [FLUSH LOGS](#query_language-system-flush_logs)
-   [RELOAD CONFIG](#query_language-system-reload-config)
-   [SHUTDOWN](#query_language-system-shutdown)
-   [KILL](#query_language-system-kill)
-   [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
-   [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
-   [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)
-   [STOP MERGES](#query_language-system-stop-merges)
-   [START MERGES](#query_language-system-start-merges)

## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

重新加载之前已成功加载的所有字典。
默认情况下，字典是懒惰加载的（请参阅 [dictionaries\_lazy\_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)），所以不是在启动时自动加载，而是通过dictGet函数在第一次访问时初始化，或者从ENGINE=Dictionary的表中选择。 该 `SYSTEM RELOAD DICTIONARIES` 查询重新加载这样的字典（加载）。
总是返回 `Ok.` 无论字典更新的结果如何。

## 重新加载字典Dictionary\_name {#query_language-system-reload-dictionary}

完全重新加载字典 `dictionary_name`，与字典的状态无关（LOADED/NOT\_LOADED/FAILED）。
总是返回 `Ok.` 无论更新字典的结果如何。
字典的状态可以通过查询 `system.dictionaries` 桌子

``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

重置ClickHouse的内部DNS缓存。 有时（对于旧的ClickHouse版本）在更改基础架构（更改另一个ClickHouse服务器或字典使用的服务器的IP地址）时需要使用此命令。

有关更方便（自动）缓存管理，请参阅disable\_internal\_dns\_cache、dns\_cache\_update\_period参数。

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

重置标记缓存。 用于开发ClickHouse和性能测试。

## FLUSH LOGS {#query_language-system-flush_logs}

Flushes buffers of log messages to system tables (e.g. system.query\_log). Allows you to not wait 7.5 seconds when debugging.

## RELOAD CONFIG {#query_language-system-reload-config}

重新加载ClickHouse配置。 当配置存储在ZooKeeeper中时使用。

## SHUTDOWN {#query_language-system-shutdown}

通常关闭ClickHouse（如 `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

中止ClickHouse进程（如 `kill -9 {$ pid_clickhouse-server}`)

## 管理分布式表 {#query-language-system-distributed}

ClickHouse可以管理 [分布](../../engines/table-engines/special/distributed.md) 桌子 当用户将数据插入到这些表中时，ClickHouse首先创建应发送到群集节点的数据队列，然后异步发送它。 您可以使用 [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)，和 [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) 查询。 您也可以同步插入分布式数据与 `insert_distributed_sync` 设置。

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

将数据插入分布式表时禁用后台数据分发。

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

强制ClickHouse将数据同步发送到群集节点。 如果任何节点不可用，ClickHouse将引发异常并停止查询执行。 您可以重试查询，直到查询成功，这将在所有节点恢复联机时发生。

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

将数据插入分布式表时启用后台数据分发。

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### STOP MERGES {#query_language-system-stop-merges}

提供停止MergeTree系列中表的后台合并的可能性:

``` sql
SYSTEM STOP MERGES [[db.]merge_tree_family_table_name]
```

!!! note "注"
    `DETACH / ATTACH` 即使在之前所有MergeTree表的合并已停止的情况下，table也会为表启动后台合并。

### START MERGES {#query_language-system-start-merges}

为MergeTree系列中的表提供启动后台合并的可能性:

``` sql
SYSTEM START MERGES [[db.]merge_tree_family_table_name]
```

[原始文章](https://clickhouse.tech/docs/en/query_language/system/) <!--hide-->
