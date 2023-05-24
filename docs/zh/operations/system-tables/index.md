---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
sidebar_position: 52
sidebar_label: "\u7CFB\u7EDF\u8868"
---

# 系统表 {#system-tables}

## 引言 {#system-tables-introduction}

系统表提供的信息如下:

-   服务器的状态、进程以及环境。
-   服务器的内部进程。

系统表:

-   存储于 `system` 数据库。
-   仅提供数据读取功能。
-   不能被删除或更改，但可以对其进行分离(detach)操作。

大多数系统表将其数据存储在RAM中。 一个ClickHouse服务在刚启动时便会创建此类系统表。

不同于其他系统表，系统日志表 [metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log), [query_log](../../operations/system-tables/query_log.md#system_tables-query_log), [query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log), [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log), [part_log](../../operations/system-tables/part_log.md#system.part_log), crash_log and text_log 默认采用[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 引擎并将其数据存储在文件系统中。 如果人为的从文件系统中删除表，ClickHouse服务器会在下一次进行数据写入时再次创建空表。 如果系统表结构在新版本中发生更改，那么ClickHouse会重命名当前表并创建一个新表。

用户可以通过在`/etc/clickhouse-server/config.d/`下创建与系统表同名的配置文件, 或者在`/etc/clickhouse-server/config.xml`中设置相应配置项，来自定义系统日志表的结构。可供自定义的配置项如下:

-   `database`: 系统日志表所在的数据库。这个选项目前已经不推荐使用。所有的系统日表都位于`system`库中。
-   `table`: 接收数据写入的系统日志表。
-   `partition_by`: 指定[PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)表达式。
-   `ttl`: 指定系统日志表TTL选项。
-   `flush_interval_milliseconds`: 指定日志表数据刷新到磁盘的时间间隔。
-   `engine`: 指定完整的表引擎定义。(以`ENGINE = `开头)。 这个选项与`partition_by`以及`ttl`冲突。如果与两者一起设置，服务启动时会抛出异常并且退出。

配置定义的示例如下：

```
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
    </query_log>
</clickhouse>
```

默认情况下，表增长是无限的。可以通过TTL 删除过期日志记录的设置来控制表的大小。 你也可以使用分区功能 `MergeTree`-引擎表。

## 系统指标的来源 {#system-tables-sources-of-system-metrics}

用于收集ClickHouse服务器使用的系统指标:

-   `CAP_NET_ADMIN` 能力。
-   [procfs](https://en.wikipedia.org/wiki/Procfs) （仅限于Linux）。

**procfs**

如果ClickHouse服务器没有 `CAP_NET_ADMIN` 能力，那么它将试图退回到 `ProcfsMetricsProvider`. `ProcfsMetricsProvider` 允许收集每个查询系统指标（包括CPU和I/O）。

如果系统上支持并启用procfs，ClickHouse server将收集如下指标:

-   `OSCPUVirtualTimeMicroseconds`
-   `OSCPUWaitMicroseconds`
-   `OSIOWaitMicroseconds`
-   `OSReadChars`
-   `OSWriteChars`
-   `OSReadBytes`
-   `OSWriteBytes`

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/) <!--hide-->
