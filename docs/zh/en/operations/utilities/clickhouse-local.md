---
description: '使用 clickhouse-local 在无服务器情况下处理数据的指南'
sidebar_label: 'clickhouse-local'
sidebar_position: 60
slug: /operations/utilities/clickhouse-local
title: 'clickhouse-local'
doc_type: 'reference'
---

<div id="when-to-use-clickhouse-local-vs-clickhouse">
  ## 何时使用 clickhouse-local 与 ClickHouse
</div>

`clickhouse-local` 是一个易于使用的 ClickHouse 版本，非常适合需要使用 SQL 对本地和远程文件进行快速处理、又不想安装完整数据库服务器的开发者。借助 `clickhouse-local`，开发者可以直接通过命令行使用 SQL 命令 (采用 [ClickHouse SQL](../../sql-reference/index.md) 方言) ，从而以简单高效的方式使用 ClickHouse 的功能，而无需完整安装 ClickHouse。`clickhouse-local` 的一大优势是，安装 [clickhouse-client](/zh/operations/utilities/clickhouse-local) 时已默认包含它。这意味着开发者无需复杂的安装流程，即可快速开始使用 `clickhouse-local`。

尽管 `clickhouse-local` 非常适合用于开发、测试和文件处理，但它并不适合为最终用户或应用程序提供服务。在这些场景中，建议使用开源版 [ClickHouse](/zh/install)。ClickHouse 是一个强大的 OLAP 数据库，专为处理大规模分析工作负载而设计。它能够对大型数据集上的复杂查询进行快速高效的处理，因此非常适合用于对高性能要求极高的生产环境。此外，ClickHouse 还提供了丰富的功能，例如复制、分片和高可用性，这些对于扩展以处理大型数据集并为应用程序提供服务至关重要。如果你需要处理更大的数据集，或者需要为最终用户或应用程序提供服务，我们建议使用开源版 ClickHouse，而不是 `clickhouse-local`。

请阅读下面的文档，了解 `clickhouse-local` 的一些示例用法，例如[查询本地文件](#query_data_in_file)或[读取 S3 中的 Parquet 文件](#query-data-in-a-parquet-file-in-aws-s3)。

<div id="download-clickhouse-local">
  ## 下载 clickhouse-local
</div>

`clickhouse-local` 使用与运行 ClickHouse 服务器和 `clickhouse-client` 相同的 `clickhouse` 可执行文件。下载最新版本的最简便方式是使用以下命令：

```bash
curl https://clickhouse.com/ | sh
```

:::note
你刚下载的二进制文件可以运行各种 ClickHouse 工具和实用程序。如果你想将 ClickHouse 作为数据库服务器运行，请参阅[快速入门](/zh/get-started/quick-start)。
:::

<div id="query_data_in_file">
  ## 使用 SQL 查询文件中的数据
</div>

`clickhouse-local` 的一个常见用途是对文件执行临时查询：无需先将数据插入表中。`clickhouse-local` 可以将文件中的数据流式读入临时表，并执行 SQL。

如果文件与 `clickhouse-local` 位于同一台机器上，只需指定要加载的文件即可。下面的 `reviews.tsv` 文件包含一部分亚马逊产品评论样本：

```bash
./clickhouse local -q "SELECT * FROM 'reviews.tsv'"
```

此命令是下面命令的简写：

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv')"
```

ClickHouse 会根据文件扩展名识别该文件采用的是制表符分隔格式。如果你需要显式指定格式，只需添加[众多 ClickHouse 输入格式](../../interfaces/formats.md)中的一种：

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv', 'TabSeparated')"
```

`file` 表函数会创建一个表，您可以使用 `DESCRIBE` 查看推断得到的 schema：

```bash
./clickhouse local -q "DESCRIBE file('reviews.tsv')"
```

:::tip
可在文件名中使用通配符 (参见[通配符替换](/zh/sql-reference/table-functions/file.md/#globs-in-path)) 。

示例：

```bash
./clickhouse local -q "SELECT * FROM 'reviews*.jsonl'"
./clickhouse local -q "SELECT * FROM 'review_?.csv'"
./clickhouse local -q "SELECT * FROM 'review_{1..3}.csv'"
```

:::

```response
marketplace    Nullable(String)
customer_id    Nullable(Int64)
review_id    Nullable(String)
product_id    Nullable(String)
product_parent    Nullable(Int64)
product_title    Nullable(String)
product_category    Nullable(String)
star_rating    Nullable(Int64)
helpful_votes    Nullable(Int64)
total_votes    Nullable(Int64)
vine    Nullable(String)
verified_purchase    Nullable(String)
review_headline    Nullable(String)
review_body    Nullable(String)
review_date    Nullable(Date)
```

我们来找出评分最高的产品：

```bash
./clickhouse local -q "SELECT
    argMax(product_title,star_rating),
    max(star_rating)
FROM file('reviews.tsv')"
```

```response
Monopoly Junior Board Game    5
```

<div id="query-data-in-a-parquet-file-in-aws-s3">
  ## 在 AWS S3 中查询 Parquet 文件中的数据
</div>

如果你在 S3 中有一个文件，可以使用 `clickhouse-local` 和 `s3` 表函数直接查询该文件 (无需先将数据插入 ClickHouse 表中) 。我们在一个公网 bucket 中有一个名为 `house_0.parquet` 的文件，其中包含英国已售房产的价格数据。下面来看看它有多少行：

```bash
./clickhouse local -q "
SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

该文件包含 270 万行：

```response
2772030
```

查看 ClickHouse 从文件中推断出的 schema 通常很有帮助：

```bash
./clickhouse local -q "DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

```response
price    Nullable(Int64)
date    Nullable(UInt16)
postcode1    Nullable(String)
postcode2    Nullable(String)
type    Nullable(String)
is_new    Nullable(UInt8)
duration    Nullable(String)
addr1    Nullable(String)
addr2    Nullable(String)
street    Nullable(String)
locality    Nullable(String)
town    Nullable(String)
district    Nullable(String)
county    Nullable(String)
```

我们来看看哪些社区最贵：

```bash
./clickhouse local -q "
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 10"
```

```response
LONDON    CITY OF LONDON    886    2271305    █████████████████████████████████████████████▍
LEATHERHEAD    ELMBRIDGE    206    1176680    ███████████████████████▌
LONDON    CITY OF WESTMINSTER    12577    1108221    ██████████████████████▏
LONDON    KENSINGTON AND CHELSEA    8728    1094496    █████████████████████▉
HYTHE    FOLKESTONE AND HYTHE    130    1023980    ████████████████████▍
CHALFONT ST GILES    CHILTERN    113    835754    ████████████████▋
AMERSHAM    BUCKINGHAMSHIRE    113    799596    ███████████████▉
VIRGINIA WATER    RUNNYMEDE    356    789301    ███████████████▊
BARNET    ENFIELD    282    740514    ██████████████▊
NORTHWOOD    THREE RIVERS    184    731609    ██████████████▋
```

:::tip
当您准备好将文件插入 ClickHouse 时，请启动一个 ClickHouse server，并将 `file` 和 `s3` 表函数的结果插入 `MergeTree` 表中。更多详情，请参阅[快速入门](/zh/get-started/quick-start)。
:::

<div id="format-conversions">
  ## 格式转换
</div>

你可以使用 `clickhouse-local` 在不同格式之间进行数据转换。示例：

```bash
$ clickhouse-local --input-format JSONLines --output-format CSV --query "SELECT * FROM table" < data.json > data.csv
```

系统会根据文件扩展名自动识别格式：

```bash
$ clickhouse-local --query "SELECT * FROM table" < data.json > data.csv
```

作为快捷方式，你可以使用 `--copy` 参数来编写：

```bash
$ clickhouse-local --copy < data.json > data.csv
```

<div id="usage">
  ## 用法
</div>

默认情况下，`clickhouse-local` 可以访问同一主机上 ClickHouse 服务器的数据，且不依赖服务器配置。它还支持通过 `--config-file` 参数加载服务器配置。对于临时数据，默认会创建一个唯一的临时数据目录。

基本用法 (Linux) ：

```bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

基本用法 (Mac) ：

```bash
$ ./clickhouse local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

:::note
`clickhouse-local` 也支持通过 WSL2 在 Windows 上运行。
:::

参数：

* `-S`, `--structure` — 输入数据的表结构。
* `--input-format` — 输入格式，默认为 `TSV`。
* `-F`, `--file` — 数据路径，默认为 `stdin`。
* `-q`, `--query` — 要执行的查询，以 `;` 作为分隔符。`--query` 可指定多次，例如 `--query "SELECT 1" --query "SELECT 2"`。不能与 `--queries-file` 同时使用。
* `--queries-file` - 包含待执行查询的文件路径。`--queries-file` 可指定多次，例如 `--query queries1.sql --query queries2.sql`。不能与 `--query` 同时使用。
* `--multiquery, -n` – 如果指定此参数，则可在 `--query` 选项后列出多个以分号分隔的查询。为方便起见，也可以省略 `--query`，直接在 `--multiquery` 后传入查询。
* `-N`, `--table` — 用于存放输出数据的表名，默认为 `table`。
* `-f`, `--format`, `--output-format` — 输出格式，默认为 `TSV`。
* `-d`, `--database` — 默认数据库，默认为 `_local`。
* `--stacktrace` — 是否在发生异常时转储调试输出。
* `--echo [ <bool> ]` — 在执行前打印每条查询。接受可选的布尔值。在交互模式下默认启用，在批次模式下默认禁用。注意：由于 `--echo` 现在接受可选值，紧跟在不带值的 `--echo` 后面的定位查询会被当作它的值；请改用 `--echo --query "..."`、`--echo -q "..."`、`--echo=false` 或通过管道传入 `stdin`。
* `--echo-formatted [ <bool> ]` — 格式化回显的查询。接受可选的布尔值。在交互模式下默认启用，在批次模式下默认禁用。
* `--echo-query-id [ <bool> ]` — 在执行前打印 `query_id`。接受可选的布尔值。在交互模式下默认启用，在批次模式下默认禁用。
* `--echo-query-separator <string>` — 在格式化后的回显查询前打印此分隔符 (需要 `--echo-formatted`) ，以便更容易区分输入的查询和重新格式化后的回显内容。默认为空 (禁用) 。
* `--highlight`, `--hilite` `<bool>` — 切换命令提示符和回显查询的语法高亮。默认启用。仅在输出到终端时应用高亮。
* `--hints <bool>` — 当光标位于输入末尾时，显示输入过程中的自动补全提示 (内联 &quot;ghost&quot; 文本) ，给出最佳匹配建议。使用上/下键 (或 Ctrl-Up/Ctrl-Down) 浏览提示；使用 Tab 或 Right 接受内联提示；`Enter` 仅在已显式选择某个提示后才会接受提示，否则会执行查询；`Tab` 还会打开传统补全列表。需要 `--highlight` (提示需要颜色) 以及建议机制 (因此 `--disable_suggestion` 也会将其关闭) 。默认启用。
* `--verbose` — 显示查询执行的更多细节。
* `--logger.console` — 记录到控制台。
* `--logger.log` — 日志文件名。
* `--logger.level` — 日志级别。
* `--ignore-error` — 查询失败时不停止处理。
* `-c`, `--config-file` — 配置文件路径，格式与 ClickHouse server 相同；默认情况下配置为空。
* `--no-system-tables` — 不附加系统表。
* `--help` — `clickhouse-local` 的参数参考。
* `-V`, `--version` — 打印版本信息并退出。

此外，每个 ClickHouse 配置变量也都有对应的参数，通常比 `--config-file` 更常用。

<div id="commands">
  ## 命令
</div>

<div id="ls-command">
  ### LS 命令
</div>

列出当前工作目录中 clickhouse-local 可访问的所有文件。

你可以像下面这样在交互模式下运行：

```sql title="Query"
ClickHouse local version 26.3.1.1.

:) ls

SELECT _file AS file
FROM file('*', 'One')
ORDER BY file ASC
```

```text title="Response"
┌─file────────┐
│ file1.csv   │
│ file2.json  │
│ file3.xml   │
└─────────────┘
```

你也可以使用参数 `-q` 将其作为查询执行：

```sh
./clickhouse-local -q ls
```

```text title="Response"
file1.csv
file2.json
file3.xml
```

<div id="clear-command">
  ### CLEAR 命令
</div>

清空终端屏幕 (类似于 Linux 上的 `clear` 命令，或许多终端中的 Ctrl+L) 。这是一个客户端操作：不会发送到 SQL 引擎。

在 `clickhouse-local` 中，该元命令会在 **交互式** 模式，以及 **`-q`** 和 **`--queries-file`** 输入中被识别 (与 `-q` 走相同的客户端路径，思路类似 `ls`) ，因此单独输入 `clear` 不会产生 `UNKNOWN_IDENTIFIER` 错误。远程 **`clickhouse-client --queries-file`** 的行为保持不变：文件内容仍仅作为 SQL 执行 (不支持文本层面的元命令) 。

在 `clickhouse-client` 中，它仅在 **交互式** 模式下会被识别。使用 **`-q`** 或查询文件时，`clear` 仍会被解析为 SQL，因此自动化场景会保持此前的报错行为，而不会把拼写错误变成静默的空操作。

支持的形式：`clear`、`CLEAR`、`/clear` (末尾可选的 `;` 会被忽略) 。如果标准输出不是终端 (例如将输出通过管道传递时) ，该元命令在可识别的情况下仍会被接受，但不会输出控制序列。

在 `clickhouse-local` 中配合 `-q` 使用时：

```sh
./clickhouse-local -q clear
```

<div id="examples">
  ## 示例
</div>

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

前面的示例与以下内容相同：

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local -n --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table;"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

你无需使用 `stdin` 或 `--file` 参数，也可以使用 [`file` 表函数](../../sql-reference/table-functions/file.md) 打开任意数量的文件：

```bash title="Query"
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1    2
```

现在，我们来输出每个 Unix 用户对应的 memory user：

```bash title="Query"
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

```text title="Response"
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

<div id="starting-listeners">
  ## 启动 TCP 和 HTTP 监听器
</div>

`clickhouse-local` 可以作为一个轻量级 server 运行，接受 TCP (native protocol) 和 HTTP 连接。当你希望让其他 ClickHouse 工具或应用访问正在运行的 `clickhouse-local` 实例中的 database 和表时，这会很有用。请注意，每个传入 connection 都会获得各自独立的 session：交互式 `clickhouse-local` session 中的 temporary tables 和会话级 settings 对外部连接不可见。

使用 `SYSTEM START LISTEN` 打开 listener，使用 `SYSTEM STOP LISTEN` 关闭它：

```bash
clickhouse-local \
    --listen_host 127.0.0.1 \
    --tcp_port 9000 \
    --http_port 8123 \
    --query "
        SYSTEM START LISTEN TCP;
        SYSTEM START LISTEN HTTP;
        SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+42', LineAsString);
        SYSTEM STOP LISTEN TCP;
        SYSTEM STOP LISTEN HTTP;
    "
```

`--listen_host`、`--tcp_port` 和 `--http_port` 选项用于配置绑定地址和端口。默认端口分别为 TCP 的 `9000` 和 HTTP 的 `8123`。

:::warning 安全
默认情况下，`clickhouse-local` 会使用临时用户配置运行，因此它打开的任何监听端点都不进行身份验证。除非你已通过将 `users_config` 设置为指向自定义 `users.xml` (例如通过 `--config-file`) 显式配置了用户和访问控制，否则请绑定到回环地址 (`127.0.0.1` 或 `::1`) 。如果在未进行身份验证的情况下监听非回环地址，本地实例中的数据会暴露给任何能够访问所选端口的人。
:::

<div id="related-content-1">
  ## 相关内容
</div>

* [使用 clickhouse-local 提取、转换并查询本地文件中的数据](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)
* [将数据导入 ClickHouse - 第 1 部分](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)
* [探索海量真实世界数据集：ClickHouse 中 100 多年的天气记录](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
* 博客：[使用 clickhouse-local 提取、转换并查询本地文件中的数据](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)