---
toc_priority: 17
toc_title: 命令行客户端
---

# 命令行客户端 {#command-line-client}

ClickHouse提供了一个原生命令行客户端`clickhouse-client`客户端支持命令行支持的更多信息详见[Configuring](#interfaces_cli_configuration)。

[安装部署](../getting-started/index.md)后，系统默认会安装`clickhouse-client`(同时它属于`clickhouse-client`安装包中)。

``` bash
$ clickhouse-client
ClickHouse client version 19.17.1.1579 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.17.1 revision 54428.

:)
```

不同的客户端和服务器版本彼此兼容，但是一些特性可能在旧客户机中不可用。我们建议使用与服务器应用相同版本的客户端。当你尝试使用旧版本的客户端时，服务器上的`clickhouse-client`会显示如下信息:

      ClickHouse client version is older than ClickHouse server. It may lack support for new features.

## 使用方式 {#cli_usage}

客户端可以在交互和非交互(批处理)模式下使用。要使用批处理模式，请指定`query`参数，或将数据发送到`stdin`(它会验证`stdin`是否是终端)，或两者同时进行。与HTTP接口类似，当使用`query`参数并向`stdin`发送数据时，客户端请求就是一行一行的`stdin`输入作为`query`的参数。这种方式在大规模的插入请求中非常方便。

使用客户端插入数据的示例：

``` bash
$ echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

$ cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

$ cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

在批量模式中，默认的数据格式是`TabSeparated`分隔的。您可以根据查询来灵活设置FORMAT格式。

默认情况下，在批量模式中只能执行单个查询。为了从一个Script中执行多个查询，可以使用`--multiquery`参数。除了INSERT请求外，这种方式在任何地方都有用。查询的结果会连续且不含分隔符地输出。
同样的，为了执行大规模的查询，您可以为每个查询执行一次`clickhouse-client`。但注意到每次启动`clickhouse-client`程序都需要消耗几十毫秒时间。

在交互模式下，每条查询过后，你可以直接输入下一条查询命令。

如果`multiline`没有指定（默认没指定）：为了执行查询，按下Enter即可。查询语句不是必须使用分号结尾。如果需要写一个多行的查询语句，可以在换行之前输入一个反斜杠`\`，然后在您按下Enter键后，您就可以输入当前语句的下一行查询了。

如果指定了`multiline`：为了执行查询，需要以分号结尾并且按下Enter键。如果行末没有分号，将认为当前语句并没有输入完而要求继续输入下一行。

若只运行单个查询，分号后面的所有内容都会被忽略。

您可以指定`\G`来替代分号或者在分号后面，这表示使用`Vertical`的格式。在这种格式下，每一个值都会打印在不同的行中，这种方式对于宽表来说很方便。这个不常见的特性是为了兼容MySQL命令而加的。

命令行客户端是基于`replxx`(类似于`readline`)。换句话说，它可以使用我们熟悉的快捷键方式来操作以及保留历史命令。
历史命令会写入在`~/.clickhouse-client-history`中。

默认情况下，输出的格式是`PrettyCompact`。您可以通过FORMAT设置根据不同查询来修改格式，或者通过在查询末尾指定`\G`字符，或通过在命令行中使用`--format`或`--vertical`参数，或使用客户端的配置文件。

若要退出客户端，使用Ctrl+D（或Ctrl+C），或者输入以下其中一个命令：`exit`, `quit`, `logout`, `учше`, `йгше`, `дщпщге`, `exit;`, `quit;`, `logout;`, `q`, `Q`, `:q`

当执行一个查询的时候，客户端会显示：

1.  进度, 进度会每秒更新十次（默认情况下）。对于很快的查询，进度可能没有时间显示。
2.  为了调试会显示解析且格式化后的查询语句。
3.  指定格式的输出结果。
4.  输出结果的行数的行数，经过的时间，以及查询处理的速度。

您可以通过Ctrl+C来取消一个长时间的查询。然而，您依然需要等待服务端来中止请求。在某个阶段去取消查询是不可能的。如果您不等待并再次按下Ctrl + C,客户端将会退出。

命令行客户端允许通过外部数据（外部临时表）来查询。更多相关信息，请参考 «[外部数据查询处理](../engines/table-engines/special/external-data.md)».

### 查询参数 {#cli-queries-with-parameters}

您可以创建带有参数的查询，并将值从客户端传递给服务器。这允许避免在客户端使用特定的动态值格式化查询。例如:

``` bash
$ clickhouse-client --param_parName="[1, 2]"  -q "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

#### 查询语法 {#cli-queries-with-parameters-syntax}

像平常一样格式化一个查询，然后把你想要从app参数传递到查询的值用大括号格式化，格式如下:

``` sql
{<name>:<data type>}
```

-   `name` — 占位符标识符。在控制台客户端，使用`--param_<name> = value`来指定
-   `data type` — [数据类型](../sql-reference/data-types/index.md)参数值。例如，一个数据结构`(integer, ('string', integer))`拥有`Tuple(UInt8, Tuple(String, UInt8))`数据类型(你也可以用另一个[integer](../sql-reference/data-types/int-uint.md)类型)。

#### 示例 {#example}

``` bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" -q "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"
```

## 配置 {#interfaces_cli_configuration}

您可以通过以下方式传入参数到`clickhouse-client`中（所有的参数都有默认值）：

-   通过命令行

    命令行参数会覆盖默认值和配置文件的配置。

-   配置文件

    配置文件的配置会覆盖默认值

### 命令行参数 {#command-line-options}

-   `--host, -h` -– 服务端的host名称, 默认是`localhost`。您可以选择使用host名称或者IPv4或IPv6地址。
-   `--port` – 连接的端口，默认值：9000。注意HTTP接口以及TCP原生接口使用的是不同端口。
-   `--user, -u` – 用户名。 默认值：`default`。
-   `--password` – 密码。 默认值：空字符串。
-   `--query, -q` – 使用非交互模式查询。
-   `--database, -d` – 默认当前操作的数据库. 默认值：服务端默认的配置（默认是`default`）。
-   `--multiline, -m` – 如果指定，允许多行语句查询（Enter仅代表换行，不代表查询语句完结）。
-   `--multiquery, -n` – 如果指定, 允许处理用`;`号分隔的多个查询，只在非交互模式下生效。
-   `--format, -f` – 使用指定的默认格式输出结果。
-   `--vertical, -E` – 如果指定，默认情况下使用垂直格式输出结果。这与`–format=Vertical`相同。在这种格式中，每个值都在单独的行上打印，这种方式对显示宽表很有帮助。
-   `--time, -t` – 如果指定，非交互模式下会打印查询执行的时间到`stderr`中。
-   `--stacktrace` – 如果指定，如果出现异常，会打印堆栈跟踪信息。
-   `--config-file` – 配置文件的名称。
-   `--secure` – 如果指定，将通过安全连接连接到服务器。
-   `--history_file` — 存放命令历史的文件的路径。
-   `--param_<name>` — 查询参数配置[查询参数](#cli-queries-with-parameters).

### 配置文件 {#configuration_files}

`clickhouse-client`使用以下第一个配置文件：

-   通过`--config-file`参数指定。
-   `./clickhouse-client.xml`
-   `~/.clickhouse-client/config.xml`
-   `/etc/clickhouse-client/config.xml`

配置文件示例:

``` xml
<config>
    <user>username</user>
    <password>password</password>
    <secure>False</secure>
</config>
```

[来源文章](https://clickhouse.com/docs/zh/interfaces/cli/) <!--hide-->
