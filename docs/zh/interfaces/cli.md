# 命令行客户端 {#ming-ling-xing-ke-hu-duan}

通过命令行来访问 ClickHouse，您可以使用 `clickhouse-client`

``` bash
$ clickhouse-client
ClickHouse client version 0.0.26176.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.26176.:)
```

该客户端支持命令行参数以及配置文件。查看更多，请看 «[配置](#interfaces_cli_configuration)»

## 使用方式 {#shi-yong-fang-shi}

这个客户端可以选择使用交互式与非交互式（批量）两种模式。
使用批量模式，要指定 `query` 参数，或者发送数据到 `stdin`（它会检查 `stdin` 是否是 Terminal），或者两种同时使用。
它与 HTTP 接口很相似，当使用 `query` 参数发送数据到 `stdin` 时，客户端请求就是一行一行的 `stdin` 输入作为 `query` 的参数。这种方式在大规模的插入请求中非常方便。

使用这个客户端插入数据的示例：

``` bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

在批量模式中，默认的数据格式是 `TabSeparated` 分隔的。您可以根据查询来灵活设置 FORMAT 格式。

默认情况下，在批量模式中只能执行单个查询。为了从一个 Script 中执行多个查询，可以使用 `--multiquery` 参数。除了 INSERT 请求外，这种方式在任何地方都有用。查询的结果会连续且不含分隔符地输出。
同样的，为了执行大规模的查询，您可以为每个查询执行一次 `clickhouse-client`。但注意到每次启动 `clickhouse-client` 程序都需要消耗几十毫秒时间。

在交互模式下，每条查询过后，你可以直接输入下一条查询命令。

如果 `multiline` 没有指定（默认没指定）：为了执行查询，按下 Enter 即可。查询语句不是必须使用分号结尾。如果需要写一个多行的查询语句，可以在换行之前输入一个反斜杠`\`，然后在您按下 Enter 键后，您就可以输入当前语句的下一行查询了。

如果 `multiline` 指定了：为了执行查询，需要以分号结尾并且按下 Enter 键。如果行末没有分号，将认为当前语句并没有输入完而要求继续输入下一行。

若只运行单个查询，分号后面的所有内容都会被忽略。

您可以指定 `\G` 来替代分号或者在分号后面，这表示 `Vertical` 的格式。在这种格式下，每一个值都会打印在不同的行中，这种方式对于宽表来说很方便。这个不常见的特性是为了兼容 MySQL 命令而加的。

命令行客户端是基于 `replxx`。换句话说，它可以使用我们熟悉的快捷键方式来操作以及保留历史命令。
历史命令会写入在 `~/.clickhouse-client-history` 中。

默认情况下，输出的格式是 `PrettyCompact`。您可以通过 FORMAT 设置根据不同查询来修改格式，或者通过在查询末尾指定 `\G` 字符，或通过在命令行中使用 `--format` 或 `--vertical` 参数，或使用客户端的配置文件。

若要退出客户端，使用 Ctrl+D （或 Ctrl+C），或者输入以下其中一个命令：`exit`, `quit`, `logout`, `учше`, `йгше`, `дщпщге`, `exit;`, `quit;`, `logout;`, `учшеж`, `йгшеж`, `дщпщгеж`, `q`, `й`, `q`, `Q`, `:q`, `й`, `Й`, `Жй`

当执行一个查询的时候，客户端会显示：

1.  进度, 进度会每秒更新十次 （默认情况下）。 对于很快的查询，进度可能没有时间显示。
2.  为了调试会显示解析且格式化后的查询语句。
3.  指定格式的输出结果。
4.  输出结果的行数的行数，经过的时间，以及查询处理的速度。

您可以通过 Ctrl+C 来取消一个长时间的查询。然而，您依然需要等待服务端来中止请求。在某个阶段去取消查询是不可能的。如果您不等待并再次按下 Ctrl + C，客户端将会退出。

命令行客户端允许通过外部数据 （外部临时表） 来查询。更多相关信息，请参考 «[外部数据查询处理](../engines/table-engines/special/external-data.md)».

## 配置 {#interfaces_cli_configuration}

您可以通过以下方式传入参数到 `clickhouse-client` 中 （所有的参数都有默认值）：

-   通过命令行

    命令行参数会覆盖默认值和配置文件的配置。

-   配置文件

    配置文件的配置会覆盖默认值

### 命令行参数 {#ming-ling-xing-can-shu}

-   `--host, -h` -– 服务端的 host 名称, 默认是 ‘localhost’。 您可以选择使用 host 名称或者 IPv4 或 IPv6 地址。
-   `--port` – 连接的端口，默认值： 9000。注意 HTTP 接口以及 TCP 原生接口是使用不同端口的。
-   `--user, -u` – 用户名。 默认值： default。
-   `--password` – 密码。 默认值： 空字符串。
-   `--query, -q` – 非交互模式下的查询语句.
-   `--database, -d` – 默认当前操作的数据库. 默认值： 服务端默认的配置 （默认是 `default`）。
-   `--multiline, -m` – 如果指定，允许多行语句查询（Enter 仅代表换行，不代表查询语句完结）。
-   `--multiquery, -n` – 如果指定, 允许处理用逗号分隔的多个查询，只在非交互模式下生效。
-   `--format, -f` – 使用指定的默认格式输出结果。
-   `--vertical, -E` – 如果指定，默认情况下使用垂直格式输出结果。这与 ‘–format=Vertical’ 相同。在这种格式中，每个值都在单独的行上打印，这种方式对显示宽表很有帮助。
-   `--time, -t` – 如果指定，非交互模式下会打印查询执行的时间到 ‘stderr’ 中。
-   `--stacktrace` – 如果指定，如果出现异常，会打印堆栈跟踪信息。
-   `--config-file` – 配置文件的名称。

### 配置文件 {#pei-zhi-wen-jian}

`clickhouse-client` 使用一下第一个存在的文件：

-   通过 `--config-file` 参数指定的文件.
-   `./clickhouse-client.xml`
-   `\~/.clickhouse-client/config.xml`
-   `/etc/clickhouse-client/config.xml`

配置文件示例:

``` xml
<config>
    <user>username</user>
    <password>password</password>
</config>
```

[来源文章](https://clickhouse.tech/docs/zh/interfaces/cli/) <!--hide-->
