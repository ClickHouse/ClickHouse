---
toc_priority: 60
toc_title: clickhouse-local
---

# ClickHouse Local {#clickhouse-local}

`clickhouse-local`模式可以使您能够对本地文件执行快速处理，而无需部署和配置ClickHouse服务器。

[ClickHouse SQL语法](../../operations/utilities/clickhouse-local.md)支持对表格数据的查询.

`clickhouse-local`使用与ClickHouse Server相同的核心，因此它支持大多数功能以及相同的格式和表引擎。

默认情况下`clickhouse-local`不能访问同一主机上的数据，但它支持使用`--config-file`方式加载服务器配置。

!!! warning "警告"
    不建议将生产服务器配置加载到`clickhouse-local`因为数据可以在人为错误的情况下被损坏。

## 用途 {#usage}

基本用法:

``` bash
clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

参数:

-   `-S`, `--structure` — 输入数据的表结构。
-   `-if`, `--input-format` — 输入格式化类型, 默认是`TSV`。
-   `-f`, `--file` — 数据路径, 默认是`stdin`。
-   `-q` `--query` — 要查询的SQL语句使用`;`做分隔符。
-   `-N`, `--table` — 数据输出的表名，默认是`table`。
-   `-of`, `--format`, `--output-format` — 输出格式化类型, 默认是`TSV`。
-   `--stacktrace` — 是否在出现异常时输出栈信息。
-   `--verbose` — debug显示查询的详细信息。
-   `-s` — 禁用`stderr`输出信息。
-   `--config-file` — 与ClickHouse服务器格式相同配置文件的路径，默认情况下配置为空。
-   `--help` — `clickhouse-local`使用帮助信息。

对于每个ClickHouse配置的参数，也可以单独使用，可以不使用`--config-file`指定。

## 示例 {#examples}

``` bash
echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
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

[原始文章](https://clickhouse.tech/docs/en/operations/utils/clickhouse-local/) <!--hide-->
