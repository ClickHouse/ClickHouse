---
description: '此引擎支持以记录流的形式处理应用程序日志文件。'
sidebar_label: 'FileLog'
sidebar_position: 160
slug: /engines/table-engines/special/filelog
title: 'FileLog 表引擎'
doc_type: 'reference'
---

此引擎支持以记录流的形式处理应用程序日志文件。

`FileLog` 可用于：

* 订阅日志文件。
* 在新记录追加到已订阅的日志文件时处理这些记录。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = FileLog('path_to_logs', 'format_name') SETTINGS
    [poll_timeout_ms = 0,]
    [poll_max_batch_size = 0,]
    [max_block_size = 0,]
    [max_threads = 0,]
    [poll_directory_watch_events_backoff_init = 500,]
    [poll_directory_watch_events_backoff_max = 32000,]
    [poll_directory_watch_events_backoff_factor = 2,]
    [handle_error_mode = 'default']
```

引擎参数：

* `path_to_logs` – 要订阅的日志文件路径。可以是包含日志文件的目录路径，也可以是单个日志文件的路径。请注意，ClickHouse 只允许使用 `user_files` 目录内的路径。
* `format_name` - 记录格式。请注意，FileLog 会将文件中的每一行都作为一条单独的记录处理，因此并非所有数据格式都适用。

可选参数：

* `poll_timeout_ms` - 单次从日志文件执行 poll 的超时时间。默认值：[stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms)。
* `poll_max_batch_size` — 单次 poll 可拉取的最大记录数。默认值：[max&#95;block&#95;size](/zh/operations/settings/settings#max_block_size)。
* `max_block_size` — poll 的最大批次大小 (按记录数计) 。默认值：[max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size)。
* `max_threads` - 用于解析文件的最大线程数，默认值为 0，表示该值将设为 max(1, physical&#95;cpu&#95;cores / 4)。
* `poll_directory_watch_events_backoff_init` - 目录 watch 线程的初始 sleep 值。默认值：`500`。
* `poll_directory_watch_events_backoff_max` - 目录 watch 线程的最大 sleep 值。默认值：`32000`。
* `poll_directory_watch_events_backoff_factor` - backoff 的速率，默认使用指数退避。默认值：`2`。
* `handle_error_mode` — FileLog 引擎的错误处理方式。可能的值：default (如果消息解析失败，将抛出异常) ，stream (异常消息和原始消息将保存在虚拟列 `_error` 和 `_raw_message` 中) 。

<div id="description">
  ## 描述
</div>

已传输的记录会被自动跟踪，因此日志文件中的每条记录只会被统计一次。

`SELECT` 并不特别适合用来读取记录 (调试除外) ，因为每条记录只能读取一次。更实用的做法是使用 [materialized views](../../../sql-reference/statements/create/view.md) 创建实时处理流程。为此：

1. 使用该引擎创建一个 FileLog 表，并将其视为数据 stream。
2. 创建一个具有所需结构的表。
3. 创建一个 materialized view，将来自该引擎的数据转换后写入之前创建的表中。

当 `MATERIALIZED VIEW` 关联到该引擎后，它就会开始在后台收集数据。这样一来，你就可以持续接收来自日志文件的记录，并使用 `SELECT` 将其转换为所需的格式。
一个 FileLog 表可以拥有任意多个 materialized views；它们不会直接从该表读取数据，而是接收新的记录 (以块的形式) ，因此你可以写入多个明细程度不同的表 (带分组聚合或不带分组聚合) 。

示例：

```sql
  CREATE TABLE logs (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = FileLog('user_files/my_app/app.log', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM logs GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

如需停止接收流数据或更改转换逻辑，请对 materialized view 执行 detach 操作：

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

如果要通过 `ALTER` 修改目标表，建议先禁用materialized view，以避免目标表与视图数据之间出现不一致。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_filename` - 日志文件名。数据类型：`LowCardinality(String)`。
* `_offset` - 日志文件中的偏移量。数据类型：`UInt64`。

当 `handle_error_mode='stream'` 时，还会有以下虚拟列：

* `_raw_record` - 未能成功解析的原始记录。数据类型：`Nullable(String)`。
* `_error` - 解析失败时产生的异常消息。数据类型：`Nullable(String)`。

注意：`_raw_record` 和 `_error` 这两个虚拟列仅会在解析过程中发生异常时填充；当消息成功解析时，它们始终为 `NULL`。