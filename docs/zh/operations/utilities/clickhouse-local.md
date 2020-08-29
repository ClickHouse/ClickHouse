---
toc_priority: 60
toc_title: clickhouse-local
---

# ﾂ环板-ｮﾂ嘉ｯﾂ偲 {#clickhouse-local}

该 `clickhouse-local` 程序使您能够对本地文件执行快速处理，而无需部署和配置ClickHouse服务器。

接受表示表的数据并使用以下方式查询它们 [ﾂ环板ECTｮﾂ嘉ｯﾂ偲](../../operations/utilities/clickhouse-local.md).

`clickhouse-local` 使用与ClickHouse server相同的核心，因此它支持大多数功能以及相同的格式和表引擎。

默认情况下 `clickhouse-local` 不能访问同一主机上的数据，但它支持使用以下方式加载服务器配置 `--config-file` 争论。

!!! warning "警告"
    不建议将生产服务器配置加载到 `clickhouse-local` 因为数据可以在人为错误的情况下被损坏。

## 用途 {#usage}

基本用法:

``` bash
clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

参数:

-   `-S`, `--structure` — table structure for input data.
-   `-if`, `--input-format` — input format, `TSV` 默认情况下。
-   `-f`, `--file` — path to data, `stdin` 默认情况下。
-   `-q` `--query` — queries to execute with `;` 如delimeter。
-   `-N`, `--table` — table name where to put output data, `table` 默认情况下。
-   `-of`, `--format`, `--output-format` — output format, `TSV` 默认情况下。
-   `--stacktrace` — whether to dump debug output in case of exception.
-   `--verbose` — more details on query execution.
-   `-s` — disables `stderr` 记录。
-   `--config-file` — path to configuration file in same format as for ClickHouse server, by default the configuration empty.
-   `--help` — arguments references for `clickhouse-local`.

还有每个ClickHouse配置变量的参数，这些变量更常用，而不是 `--config-file`.

## 例 {#examples}

``` bash
echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1 2
3 4
```

前面的例子是一样的:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1 2
3 4
```

现在让我们为每个Unix用户输出内存用户:

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' | clickhouse-local -S "user String, mem Float64" -q "SELECT user, round(sum(mem), 2) as memTotal FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

[原始文章](https://clickhouse.tech/docs/en/operations/utils/clickhouse-local/) <!--hide-->
