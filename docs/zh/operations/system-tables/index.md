---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_priority: 52
toc_title: "\u7CFB\u7EDF\u8868"
---

# 系统表 {#system-tables}

## 导言 {#system-tables-introduction}

系统表提供以下信息:

-   服务器状态、进程和环境。
-   服务器的内部进程。

系统表:

-   坐落于 `system` 数据库。
-   仅适用于读取数据。
-   不能删除或更改，但可以分离。

大多数系统表将数据存储在RAM中。 ClickHouse服务器在开始时创建此类系统表。

与其他系统表不同，系统日志表 [metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log), [query_log](../../operations/system-tables/query_log.md#system_tables-query_log), [query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log), [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log), [part_log](../../operations/system-tables/part_log.md#system.part_log), crash_log and text_log 默认采用[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 引擎并将其数据存储在存储文件系统中。 如果从文件系统中删除表，ClickHouse服务器会在下一次写入数据时再次创建空表。 如果系统表架构在新版本中发生更改，则ClickHouse会重命名当前表并创建一个新表。

用户可以通过在`/etc/clickhouse-server/config.d/`下创建与系统表同名的配置文件, 或者在`/etc/clickhouse-server/config.xml`中设置相应配置项，来自定义系统日志表的结构。可以自定义的配置项如下:

-   `database`: 系统日志表所在的数据库。这个选项目前已经废弃。所有的系统日表都位于`system`库中。
-   `table`: 系统日志表名。
-   `partition_by`: 指定[PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)表达式。
-   `ttl`: 指定系统日志表TTL选项。
-   `flush_interval_milliseconds`: 指定系统日志表数据落盘时间。
-   `engine`: 指定完整的表引擎定义。(以`ENGINE = `开始)。 这个选项与`partition_by`以及`ttl`冲突。如果两者一起设置，服务启动时会抛出异常并且退出。

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

默认情况下，表增长是无限的。 要控制表的大小，可以使用 TTL 删除过期日志记录的设置。 你也可以使用分区功能 `MergeTree`-发动机表。

## 系统指标的来源 {#system-tables-sources-of-system-metrics}

用于收集ClickHouse服务器使用的系统指标:

-   `CAP_NET_ADMIN` 能力。
-   [procfs](https://en.wikipedia.org/wiki/Procfs) （仅在Linux中）。

**procfs**

如果ClickHouse服务器没有 `CAP_NET_ADMIN` 能力，它试图回落到 `ProcfsMetricsProvider`. `ProcfsMetricsProvider` 允许收集每个查询系统指标（用于CPU和I/O）。

如果系统上支持并启用procfs，ClickHouse server将收集这些指标:

-   `OSCPUVirtualTimeMicroseconds`
-   `OSCPUWaitMicroseconds`
-   `OSIOWaitMicroseconds`
-   `OSReadChars`
-   `OSWriteChars`
-   `OSReadBytes`
-   `OSWriteBytes`

[原始文章](https://clickhouse.tech/docs/en/operations/system-tables/) <!--hide-->
