---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\uFF82\u6697\uFF6A\uFF82\u6C3E\u73AF\u50AC\uFF82\u56E3"
---

# ﾂ暗ｪﾂ氾环催ﾂ団 {#clickhouse-benchmark}

连接到ClickHouse服务器并重复发送指定的查询。

语法:

``` bash
$ echo "single query" | clickhouse-benchmark [keys]
```

或

``` bash
$ clickhouse-benchmark [keys] <<< "single query"
```

如果要发送一组查询，请创建一个文本文件，并将每个查询放在此文件中的单个字符串上。 例如:

``` sql
SELECT * FROM system.numbers LIMIT 10000000
SELECT 1
```

然后将此文件传递给标准输入 `clickhouse-benchmark`.

``` bash
clickhouse-benchmark [keys] < queries_file
```

## 键 {#clickhouse-benchmark-keys}

-   `-c N`, `--concurrency=N` — Number of queries that `clickhouse-benchmark` 同时发送。 默认值：1。
-   `-d N`, `--delay=N` — Interval in seconds between intermediate reports (set 0 to disable reports). Default value: 1.
-   `-h WORD`, `--host=WORD` — Server host. Default value: `localhost`. 为 [比较模式](#clickhouse-benchmark-comparison-mode) 您可以使用多个 `-h` 钥匙
-   `-p N`, `--port=N` — Server port. Default value: 9000. For the [比较模式](#clickhouse-benchmark-comparison-mode) 您可以使用多个 `-p` 钥匙
-   `-i N`, `--iterations=N` — Total number of queries. Default value: 0.
-   `-r`, `--randomize` — Random order of queries execution if there is more then one input query.
-   `-s`, `--secure` — Using TLS connection.
-   `-t N`, `--timelimit=N` — Time limit in seconds. `clickhouse-benchmark` 达到指定的时间限制时停止发送查询。 默认值：0（禁用时间限制）。
-   `--confidence=N` — Level of confidence for T-test. Possible values: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99.5%). Default value: 5. In the [比较模式](#clickhouse-benchmark-comparison-mode) `clickhouse-benchmark` 执行 [独立双样本学生的t测试](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test) 测试以确定两个分布是否与所选置信水平没有不同。
-   `--cumulative` — Printing cumulative data instead of data per interval.
-   `--database=DATABASE_NAME` — ClickHouse database name. Default value: `default`.
-   `--json=FILEPATH` — JSON output. When the key is set, `clickhouse-benchmark` 将报告输出到指定的JSON文件。
-   `--user=USERNAME` — ClickHouse user name. Default value: `default`.
-   `--password=PSWD` — ClickHouse user password. Default value: empty string.
-   `--stacktrace` — Stack traces output. When the key is set, `clickhouse-bencmark` 输出异常的堆栈跟踪。
-   `--stage=WORD` — Query processing stage at server. ClickHouse stops query processing and returns answer to `clickhouse-benchmark` 在指定的阶段。 可能的值: `complete`, `fetch_columns`, `with_mergeable_state`. 默认值: `complete`.
-   `--help` — Shows the help message.

如果你想申请一些 [设置](../../operations/settings/index.md) 对于查询，请将它们作为键传递 `--<session setting name>= SETTING_VALUE`. 例如, `--max_memory_usage=1048576`.

## 输出 {#clickhouse-benchmark-output}

默认情况下, `clickhouse-benchmark` 每个报表 `--delay` 间隔。

报告示例:

``` text
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

在报告中，您可以找到:

-   在查询的数量 `Queries executed:` 场。

-   状态字符串包含（按顺序):

    -   ClickHouse服务器的端点。
    -   已处理的查询数。
    -   QPS：QPS：在指定的时间段内每秒执行多少个查询服务器 `--delay` 争论。
    -   RPS：在指定的时间段内，服务器每秒读取多少行 `--delay` 争论。
    -   MiB/s：在指定的时间段内每秒读取多少mebibytes服务器 `--delay` 争论。
    -   结果RPS：在指定的时间段内，服务器每秒放置到查询结果的行数 `--delay` 争论。
    -   结果MiB/s.在指定的时间段内，服务器每秒将多少mebibytes放置到查询结果中 `--delay` 争论。

-   查询执行时间的百分位数。

## 比较模式 {#clickhouse-benchmark-comparison-mode}

`clickhouse-benchmark` 可以比较两个正在运行的ClickHouse服务器的性能。

要使用比较模式，请通过以下两对指定两个服务器的端点 `--host`, `--port` 钥匙 键在参数列表中的位置匹配在一起，第一 `--host` 与第一匹配 `--port` 等等。 `clickhouse-benchmark` 建立到两个服务器的连接，然后发送查询。 每个查询寻址到随机选择的服务器。 每个服务器的结果分别显示。

## 示例 {#clickhouse-benchmark-example}

``` bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark -i 10
```

``` text
Loaded 1 queries.

Queries executed: 6.

localhost:9000, queries 6, QPS: 6.153, RPS: 123398340.957, MiB/s: 941.455, result RPS: 61532982.200, result MiB/s: 469.459.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.159 sec.
30.000%     0.160 sec.
40.000%     0.160 sec.
50.000%     0.162 sec.
60.000%     0.164 sec.
70.000%     0.165 sec.
80.000%     0.166 sec.
90.000%     0.166 sec.
95.000%     0.167 sec.
99.000%     0.167 sec.
99.900%     0.167 sec.
99.990%     0.167 sec.



Queries executed: 10.

localhost:9000, queries 10, QPS: 6.082, RPS: 121959604.568, MiB/s: 930.478, result RPS: 60815551.642, result MiB/s: 463.986.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.160 sec.
30.000%     0.163 sec.
40.000%     0.164 sec.
50.000%     0.165 sec.
60.000%     0.166 sec.
70.000%     0.166 sec.
80.000%     0.167 sec.
90.000%     0.167 sec.
95.000%     0.170 sec.
99.000%     0.172 sec.
99.900%     0.172 sec.
99.990%     0.172 sec.
```
