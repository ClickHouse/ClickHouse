---
description: '介绍什么是系统表，以及它们为何有用。'
keywords: ['系统表', '概述']
sidebar_label: '概述'
sidebar_position: 52
slug: /operations/system-tables/overview
title: '系统表概述'
doc_type: '参考'
---

<div id="system-tables-introduction">
  ## 系统表概览
</div>

系统表提供以下信息：

* 服务器状态、进程和环境。
* 服务器的内部处理过程。
* 构建 ClickHouse 可执行文件时使用的选项。

系统表：

* 位于 `system` 数据库中。
* 仅可用于读取数据。
* 不能被删除或修改，但可以被分离。

大多数系统表将数据存储在 RAM 中。ClickHouse server 会在启动时创建这类系统表。

与其他系统表不同，系统日志表 [metric&#95;log](../../operations/system-tables/metric_log.md)、[query&#95;log](../../operations/system-tables/query_log.md)、[query&#95;thread&#95;log](../../operations/system-tables/query_thread_log.md)、[trace&#95;log](../../operations/system-tables/trace_log.md)、[part&#95;log](../../operations/system-tables/part_log.md)、[crash&#95;log](../../operations/system-tables/crash_log.md)、[text&#95;log](../../operations/system-tables/text_log.md) 和 [backup&#95;log](../../operations/system-tables/backup_log.md) 由 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表引擎提供支持，并默认将数据存储在文件系统中。如果你从文件系统中删除某个表，ClickHouse server 会在下一次写入数据时重新创建一个空表。如果新版本中系统表 schema 发生变化，ClickHouse 会重命名当前表并创建一个新表。

可以通过在 `/etc/clickhouse-server/config.d/` 下创建与该表同名的 config file，或在 `/etc/clickhouse-server/config.xml` 中设置相应元素，来自定义系统日志表。可自定义的元素有：

* `database`：系统日志表所属的数据库。此选项现已弃用。所有系统日志表都位于 `system` 数据库中。
* `table`：用于插入数据的表。
* `partition_by`：指定 [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) expression。
* `ttl`：指定表的 [TTL](../../sql-reference/statements/alter/ttl.md) expression。
* `flush_interval_milliseconds`：将数据 flush 到 disk 的时间间隔。
* `engine`：提供带参数的完整 engine expression (以 `ENGINE =` 开头) 。此选项与 `partition_by` 和 `ttl` 冲突。如果同时设置，服务器将抛出异常并退出。

示例：

```xml
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 30 DAY DELETE</ttl>
        <!--
        <engine>ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024</engine>
        -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_log>
</clickhouse>
```

默认情况下，表会无限增长。要控制表的大小，可以使用 [生存时间 (TTL)](/zh/sql-reference/statements/alter/ttl) 设置来删除过期的日志记录。也可以利用 `MergeTree` 引擎表的分区功能。

<div id="system-tables-sources-of-system-metrics">
  ## 系统指标来源
</div>

ClickHouse server 使用以下来源收集系统指标：

* `CAP_NET_ADMIN` 能力。
* [procfs](https://en.wikipedia.org/wiki/Procfs) (仅适用于 Linux) 。

**procfs**

如果 ClickHouse server 不具备 `CAP_NET_ADMIN` 能力，则会尝试改为使用 `ProcfsMetricsProvider`。`ProcfsMetricsProvider` 支持收集每个查询的系统指标 (CPU 和 I/O) 。

如果系统支持并启用了 procfs，ClickHouse server 会收集以下指标：

* `OSCPUVirtualTimeMicroseconds`
* `OSCPUWaitMicroseconds`
* `OSIOWaitMicroseconds`
* `OSReadChars`
* `OSWriteChars`
* `OSReadBytes`
* `OSWriteBytes`

:::note
从 5.14.x 开始，Linux 内核默认禁用 `OSIOWaitMicroseconds`。
你可以使用 `sudo sysctl kernel.task_delayacct=1` 启用它，或者在 `/etc/sysctl.d/` 中创建一个 `.conf` 文件，并添加 `kernel.task_delayacct = 1`
:::

<div id="system-tables-in-clickhouse-cloud">
  ## ClickHouse Cloud 中的系统表
</div>

在 ClickHouse Cloud 中，系统表与自管理部署中的系统表一样，能够为服务的状态和性能提供关键洞察。有些系统表是集群级的，尤其是那些从 Keeper 节点获取数据的系统表；Keeper 节点负责管理分布式元数据。这些表反映的是整个集群的整体状态，因此在单个节点上查询时，结果应保持一致。例如，[`parts`](/zh/operations/system-tables/parts) 无论从哪个节点查询，结果都应一致：

```sql
SELECT hostname(), count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-vccsrty-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.005 sec.

SELECT
 hostname(),
    count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-w59bfco-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.004 sec.
```

相对地，其他系统表则是节点级的，例如以 in-memory 方式存储，或使用 MergeTree 表引擎持久化数据。日志和指标这类数据通常就是如此。持久化可确保历史数据始终可供分析。不过，这些节点级表天然是每个节点各自独有的。

通常，判断某个系统表是否为节点级表时，可以参考以下规则：

* 带有 `_log` 后缀的系统表。
* 暴露指标的系统表，例如 `metrics`、`asynchronous_metrics`、`events`。
* 暴露正在运行的进程的系统表，例如 `processes`、`merges`。

此外，系统表在升级或 schema 发生变更后，也可能会创建新版本。这些版本使用数字后缀命名。

例如，以 `system.query_log` 表为例，其中包含该节点执行的每个查询对应的一行：

```sql
SHOW TABLES FROM system LIKE 'query_log%'

┌─name─────────┐
│ query_log    │
│ query_log_1  │
│ query_log_10 │
│ query_log_2  │
│ query_log_3  │
│ query_log_4  │
│ query_log_5  │
│ query_log_6  │
│ query_log_7  │
│ query_log_8  │
│ query_log_9  │
└──────────────┘

11 rows in set. Elapsed: 0.004 sec.
```

<div id="querying-multiple-versions">
  ### 查询多个版本
</div>

我们可以使用 [`merge`](/zh/sql-reference/table-functions/merge) 函数跨这些表进行查询。例如，下面的查询会找出每个 `query_log` 表中在目标节点上发出的最新查询：

```sql
SELECT
    _table,
    max(event_time) AS most_recent
FROM merge('system', '^query_log')
GROUP BY _table
ORDER BY most_recent DESC

┌─_table───────┬─────────most_recent─┐
│ query_log    │ 2025-04-13 10:59:29 │
│ query_log_1  │ 2025-04-09 12:34:46 │
│ query_log_2  │ 2025-04-09 12:33:45 │
│ query_log_3  │ 2025-04-07 17:10:34 │
│ query_log_5  │ 2025-03-24 09:39:39 │
│ query_log_4  │ 2025-03-24 09:38:58 │
│ query_log_6  │ 2025-03-19 16:07:41 │
│ query_log_7  │ 2025-03-18 17:01:07 │
│ query_log_8  │ 2025-03-18 14:36:07 │
│ query_log_10 │ 2025-03-18 14:01:33 │
│ query_log_9  │ 2025-03-18 14:01:32 │
└──────────────┴─────────────────────┘

11 rows in set. Elapsed: 0.373 sec. Processed 6.44 million rows, 25.77 MB (17.29 million rows/s., 69.17 MB/s.)
Peak memory usage: 28.45 MiB.
```

:::note 不要依赖数字后缀来判断顺序
虽然表名中的数字后缀可能暗示数据的顺序，但绝不能据此判断。因此，在针对特定日期范围进行查询时，务必始终将 `merge` 表函数与日期过滤器结合使用。
:::

需要特别注意的是，这些表仍然**仅存在于各个节点的本地**。

<div id="querying-across-nodes">
  ### 跨节点查询
</div>

为了全面查看整个集群的情况，用户可以结合使用 [`clusterAllReplicas`](/zh/sql-reference/table-functions/cluster) 函数和 `merge` 函数。`clusterAllReplicas` 函数允许跨 &quot;default&quot; 集群中的所有副本查询系统表，并将各节点的数据汇总为统一结果。结合 `merge` 函数后，可用于查询集群中特定表的全部系统数据。

这种方法对于监控和调试集群级操作尤其有价值，能够帮助用户有效分析其 ClickHouse Cloud 部署的健康状况和性能。

:::note
ClickHouse Cloud 提供由多个副本组成的集群，以实现冗余和故障转移。这使其能够支持动态自动扩缩容和零停机升级等功能。在某些时刻，新节点可能正在加入集群，或正从集群中移除。要跳过这些节点，请按如下所示，在使用 `clusterAllReplicas` 的查询中添加 `SETTINGS skip_unavailable_shards = 1`。
:::

例如，可以对比查询 `query_log` 表时的差异——这通常对分析至关重要。

```sql
SELECT
    hostname() AS host,
    count()
FROM system.query_log
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.010 sec. Processed 17.87 thousand rows, 71.51 KB (1.75 million rows/s., 7.01 MB/s.)

SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', system.query_log)
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
│ c-ecru-qn-34-server-6em4y4t-0 │  656029 │
│ c-ecru-qn-34-server-iejrkg0-0 │  641155 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.026 sec. Processed 1.97 million rows, 7.88 MB (75.51 million rows/s., 302.05 MB/s.)
```

<div id="querying-across-nodes-and-versions">
  ### 跨节点和版本查询
</div>

由于系统表的版本差异，这仍然不能反映集群中的全部数据。将上述内容与 `merge` 函数结合后，就能得到所选日期范围内的准确结果：

```sql
SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', merge('system', '^query_log'))
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │ 3008000 │
│ c-ecru-qn-34-server-6em4y4t-0 │ 3659443 │
│ c-ecru-qn-34-server-iejrkg0-0 │ 1078287 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.462 sec. Processed 7.94 million rows, 31.75 MB (17.17 million rows/s., 68.67 MB/s.)
```

<div id="related-content">
  ## 相关内容
</div>

* 博客：[系统表：一窥 ClickHouse 内部机制](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
* 博客：[核心监控查询 - 第 1 部分 - INSERT 查询](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
* 博客：[核心监控查询 - 第 2 部分 - SELECT 查询](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)