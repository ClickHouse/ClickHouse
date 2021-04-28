---
toc_priority: 52
toc_title: "系统表"
---

# 系统表 {#system-tables}

## 介绍 {#system-tables-introduction}

系统表提供以下信息:

-   服务器状态、进程和环境。
-   服务器的内部进程。

系统表:

-   位于 `system` 数据库中。
-   仅可用于读取数据。
-   不能删除或更改，但可以剥离(detached)。

大多数系统表将其数据存储在RAM中。 ClickHouse服务器在启动时创建这些系统表。

与其他系统表不同，系统日志表 [metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log), [query_log](../../operations/system-tables/query_log.md#system_tables-query_log), [query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log), [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log), [part_log](../../operations/system-tables/part_log.md#system.part_log), crash_log and text_log 默认采用[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 引擎并将其数据存储在文件系统中。 如果从文件系统中删除表，ClickHouse服务器会在下一次写入数据时再次创建一个空表。 如果系统表模式在新版本中发生更改，则ClickHouse会重命名当前表并创建一个新表。

用户可以通过在`/etc/clickhouse-server/config.d/`下创建与系统表同名的配置文件, 或者在`/etc/clickhouse-server/config.xml`中设置相应元素，来定制系统日志表。可以自定义的元素有:

-   `database`: 系统日志表所属的数据库。现在不推荐使用此选项。所有的系统日志表都位于`system`数据库中。
-   `table`: 系统日志表名。
-   `partition_by`: 指定[PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)表达式。
-   `ttl`: 指定系统日志表的TTL表达式。
-   `flush_interval_milliseconds`: 指定数据落盘的时间间隔。
-   `engine`: 提供带有参数的完整的表引擎(以`ENGINE = `开头)。 这个选项与`partition_by`以及`ttl`冲突。如果一起设置，服务启动时会抛出异常并且退出。

一个配置定义的例子如下：

```
<yandex>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 30 DAY DELETE</ttl>
        <!--
        <engine>ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024</engine>
        -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </query_log>
</yandex>
```

默认情况下，表增长是无限的。 要控制表的大小，可以使用 TTL 设置删除过时的日志记录。 你也可以使用 `MergeTree`-引擎表的分区功能。

## 系统指标的来源 {#system-tables-sources-of-system-metrics}

为了收集系统指标，ClickHouse服务器使用：

-   `CAP_NET_ADMIN` 能力。
-   [procfs](https://en.wikipedia.org/wiki/Procfs) （仅在Linux中）。

**procfs**

如果ClickHouse服务器没有 `CAP_NET_ADMIN` 能力，它将尝试回退到 `ProcfsMetricsProvider`。 `ProcfsMetricsProvider` 允许收集每个查询的系统指标（用于CPU和I/O）。

如果系统上支持并启用了procfs，ClickHouse 服务器将收集这些指标:

-   `OSCPUVirtualTimeMicroseconds`
-   `OSCPUWaitMicroseconds`
-   `OSIOWaitMicroseconds`
-   `OSReadChars`
-   `OSWriteChars`
-   `OSReadBytes`
-   `OSWriteBytes`

[原始文章](https://clickhouse.tech/docs/en/operations/system-tables/) <!--hide-->
