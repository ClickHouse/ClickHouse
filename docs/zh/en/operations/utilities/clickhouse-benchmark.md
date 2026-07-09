---
description: 'clickhouse-benchmark 文档'
sidebar_label: 'clickhouse-benchmark'
sidebar_position: 61
slug: /operations/utilities/clickhouse-benchmark
title: 'clickhouse-benchmark'
doc_type: 'reference'
---

连接到 ClickHouse 服务器，并反复发送指定的查询。

**语法**

```bash
$ clickhouse-benchmark --query ["single query"] [keys]
```

或

```bash
$ echo "single query" | clickhouse-benchmark [keys]
```

或

```bash
$ clickhouse-benchmark [keys] <<< "single query"
```

如果您想发送一组查询，请创建一个文本文件，并将每条查询单独写在文件中的一行。例如：

```sql
SELECT * FROM system.numbers LIMIT 10000000;
SELECT 1;
```

然后将此文件传给 `clickhouse-benchmark` 的标准输入：

```bash
clickhouse-benchmark [keys] < queries_file;
```

<div id="clickhouse-benchmark-command-line-options">
  ## 命令行选项
</div>

* `--query=QUERY` — 要执行的查询。如果未传递此参数，`clickhouse-benchmark` 将从标准输入读取查询。
* `--query_id=ID` — 查询 Id。
* `--query_id_prefix=ID_PREFIX` — 查询 Id 前缀。
* `--queries-format=FORMAT` — 从标准输入读取查询时使用的格式。可能的值：`tsv` (默认值，每行一个经过 tab 转义的查询) 和 `script` (将输入解析为由分号分隔的多查询脚本) 。`script` 的限制：`INSERT ... FORMAT` 查询必须写在单行中。
* `-c N`, `--concurrency=N` — `clickhouse-benchmark` 同时发送的查询数量。默认值：1。
* `-C N`, `--max_concurrency=N` — 逐步将并行查询数增加到指定值，并为每个并发级别生成一份报告。
* `--precise` — 启用按时间间隔生成的精确报告，并包含加权指标。
* `-d N`, `--delay=N` — 中间报告之间的时间间隔 (秒)  (如需禁用报告，请设为 0) 。默认值：1。
* `-h HOST`, `--host=HOST` — 服务器 host。默认值：`localhost`。在[比较模式](#clickhouse-benchmark-comparison-mode)下，可以使用多个 `-h` 参数。
* `-i N`, `--iterations=N` — 查询总数。默认值：0 (无限重复) 。
* `-r`, `--randomize` — 如果输入的查询多于一个，则随机顺序执行查询。
* `-s`, `--secure` — 使用 `TLS` 连接。
* `-t N`, `--timelimit=N` — 时间限制 (秒) 。达到指定时间限制后，`clickhouse-benchmark` 将停止发送查询。默认值：0 (禁用时间限制) 。
* `--port=N` — 服务器端口。默认值：9000。在[比较模式](#clickhouse-benchmark-comparison-mode)下，可以使用多个 `--port` 参数。
* `--confidence=N` — T-test 的置信水平。可能的值：0 (80%) 、1 (90%) 、2 (95%) 、3 (98%) 、4 (99%) 、5 (99.5%) 。默认值：5。在[比较模式](#clickhouse-benchmark-comparison-mode)中，`clickhouse-benchmark` 会执行 [Independent two-sample Student&#39;s t-test](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test)，以判断在所选置信水平下两个分布是否不存在差异。
* `--cumulative` — 输出累计数据，而不是按时间间隔输出的数据。
* `--database=DATABASE_NAME` — ClickHouse 数据库名称。默认值：`default`。
* `--user=USERNAME` — ClickHouse 用户名。默认值：`default`。
* `--password=PSWD` — ClickHouse 用户密码。默认值：空字符串。
* `--stacktrace` — 堆栈跟踪输出。设置此参数后，`clickhouse-benchmark` 会输出异常的堆栈跟踪。
* `--stage=WORD` — server 端的查询处理阶段。ClickHouse 会在指定阶段停止查询处理，并向 `clickhouse-benchmark` 返回结果。可能的值：`complete`、`fetch_columns`、`with_mergeable_state`。默认值：`complete`。
* `--roundrobin` — 不再比较不同 `--host`/`--port` 的查询，而是为每个查询随机选择一个 `--host`/`--port` 并将查询发送到该地址。
* `--reconnect=N` — 控制重连行为。可能的值：0 (从不重连) 、1 (每个查询都重连) 或 N (每 N 个查询后重连) 。默认值：0。
* `--max-consecutive-errors=N` — 允许的连续错误次数。默认值：0。
* `--ignore-error`,`--continue_on_errors` — 即使查询失败也继续测试。
* `--client-side-time` — 显示包含网络通信在内的时间，而不是 server-side 时间；请注意，对于 22.8 之前的服务器版本，我们始终显示 client-side 时间。
* `--proto-caps` — 启用/禁用数据传输中的 chunking。可选值 (可用逗号分隔) ：`chunked_optional`、`notchunked`、`notchunked_optional`、`send_chunked`、`send_chunked_optional`、`send_notchunked`、`send_notchunked_optional`、`recv_chunked`、`recv_chunked_optional`、`recv_notchunked`、`recv_notchunked_optional`。默认值：`notchunked`。
* `--help` — 显示帮助信息。
* `--verbose` — 提高帮助信息的详细程度。

如果你想为查询应用某些[设置](/zh/operations/settings/overview)，请以键 `--<session setting name>= SETTING_VALUE` 的形式传递这些设置。例如，`--max_memory_usage=1048576`。

<div id="clickhouse-benchmark-environment-variable-options">
  ## 环境变量选项
</div>

可通过环境变量 `CLICKHOUSE_USER`、`CLICKHOUSE_PASSWORD` 和 `CLICKHOUSE_HOST` 设置用户名、密码和 host。
命令行参数 `--user`、`--password` 或 `--host` 的优先级高于环境变量。

<div id="clickhouse-benchmark-output">
  ## 输出
</div>

默认情况下，`clickhouse-benchmark` 会按每个 `--delay` 时间间隔输出报告。

报告示例：

```text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%      0.145 sec.
10.000%     0.146 sec.
20.000%     0.146 sec.
30.000%     0.146 sec.
40.000%     0.147 sec.
50.000%     0.148 sec.
60.000%     0.148 sec.
70.000%     0.148 sec.
80.000%     0.149 sec.
90.000%     0.150 sec.
95.000%     0.150 sec.
99.000%     0.150 sec.
99.900%     0.150 sec.
99.990%     0.150 sec.
```

在报告中，你可以看到：

* `Queries executed:` 字段中的查询数量。

* 状态字符串，包含以下内容 (按顺序) ：

  * ClickHouse 服务器的端点。
  * 已处理的查询数量。
  * QPS：服务器在 `--delay` 参数指定的时间段内每秒执行的查询数。
  * RPS：服务器在 `--delay` 参数指定的时间段内每秒读取的行数。
  * MiB/s：服务器在 `--delay` 参数指定的时间段内每秒读取的 mebibyte 数。
  * result RPS：服务器在 `--delay` 参数指定的时间段内每秒写入查询结果的行数。
  * result MiB/s：服务器在 `--delay` 参数指定的时间段内每秒写入查询结果的 mebibyte 数。

* 查询执行时间的百分位数。

<div id="clickhouse-benchmark-comparison-mode">
  ## 比较模式
</div>

`clickhouse-benchmark` 可以比较两台正在运行的 ClickHouse 服务器的性能。

要使用比较模式，请使用两组 `--host`、`--port` 参数指定这两个服务器的端点。参数会按参数列表中的位置一一配对：第一个 `--host` 与第一个 `--port` 配对，依此类推。`clickhouse-benchmark` 会与这两个服务器建立连接，然后发送查询。每个查询都会发送到随机选中的一台服务器。结果会显示在表中。

<div id="clickhouse-benchmark-example">
  ## 示例
</div>

```bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark --host=localhost --port=9001 --host=localhost --port=9000 -i 10
```

```text
Loaded 1 queries.

Queries executed: 5.

localhost:9001, queries 2, QPS: 3.764, RPS: 75446929.370, MiB/s: 575.614, result RPS: 37639659.982, result MiB/s: 287.168.
localhost:9000, queries 3, QPS: 3.815, RPS: 76466659.385, MiB/s: 583.394, result RPS: 38148392.297, result MiB/s: 291.049.

0.000%          0.258 sec.      0.250 sec.
10.000%         0.258 sec.      0.250 sec.
20.000%         0.258 sec.      0.250 sec.
30.000%         0.258 sec.      0.267 sec.
40.000%         0.258 sec.      0.267 sec.
50.000%         0.273 sec.      0.267 sec.
60.000%         0.273 sec.      0.267 sec.
70.000%         0.273 sec.      0.267 sec.
80.000%         0.273 sec.      0.269 sec.
90.000%         0.273 sec.      0.269 sec.
95.000%         0.273 sec.      0.269 sec.
99.000%         0.273 sec.      0.269 sec.
99.900%         0.273 sec.      0.269 sec.
99.990%         0.273 sec.      0.269 sec.

No difference proven at 99.5% confidence
```