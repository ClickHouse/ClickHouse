---
sidebar_position: 60
sidebar_label: clickhouse-local
---

# clickhouse-local {#clickhouse-local}

`clickhouse-local`模式可以使您能够对本地文件执行快速处理，而无需部署和配置ClickHouse服务器。

接受表示表格tables的数据，并使用[ClickHouse SQL方言](../../operations/utilities/clickhouse-local.md)查询它们。

`clickhouse-local`使用与ClickHouse Server相同的核心，因此它支持大多数功能以及相同的格式和表引擎。

默认情况下`clickhouse-local`不能访问同一主机上的数据，但它支持使用`--config-file`方式加载服务器配置。

!!! warning "警告"
    不建议将生产服务器配置加载到`clickhouse-local`因为数据可以在人为错误的情况下被损坏。

对于临时数据，默认情况下会创建一个唯一的临时数据目录。

## 用途 {#usage}

基本用法:

``` bash
clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

参数:

-   `-S`, `--structure` — 输入数据的表结构。
-   `--input-format` — 输入格式化类型, 默认是`TSV`。
-   `-f`, `--file` — 数据路径, 默认是`stdin`。
-   `-q`, `--query` — 要查询的SQL语句使用`;`做分隔符。您必须指定`query`或`queries-file`选项。
-   `--queries-file` - 包含执行查询的文件路径。您必须指定`query`或`queries-file`选项。
-   `-N`, `--table` — 数据输出的表名，默认是`table`。
-   `--format`, `--output-format` — 输出格式化类型, 默认是`TSV`。
-   `-d`, `--database` — 默认数据库名，默认是`_local`。
-   `--stacktrace` — 是否在出现异常时输出栈信息。
-   `--echo` — 执行前打印查询。
-   `--verbose` — debug显示查询的详细信息。
-   `--logger.console` — 日志显示到控制台。
-   `--logger.log` — 日志文件名。
-   `--logger.level` — 日志级别。
-   `--ignore-error` — 当查询失败时，不停止处理。
-   `-c`, `--config-file` — 与ClickHouse服务器格式相同配置文件的路径，默认情况下配置为空。
-   `--no-system-tables` — 不附加系统表。
-   `--help` — `clickhouse-local`使用帮助信息。
-   `-V`, `--version` — 打印版本信息并退出。

对于每个ClickHouse配置的参数，也可以单独使用，可以不使用`--config-file`指定。

## 示例 {#examples}

``` bash
echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" --input-format "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1 2
3 4
```

另一个示例，类似上一个使用示例:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1 2
3 4
```

你可以使用`stdin`或`--file`参数, 打开任意数量的文件来使用多个文件[`file` table function](../../sql-reference/table-functions/file.md):

```bash
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1   2
```

现在让我们查询每个Unix用户使用内存:

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

[原始文章](https://clickhouse.com/docs/en/operations/utils/clickhouse-local/) <!--hide-->
