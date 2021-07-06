---
toc_priority: 46
toc_title: "常见问题"
---

# 常见问题 {#troubleshooting}

-   [安装](#troubleshooting-installation-errors)
-   [连接到服务器](#troubleshooting-accepts-no-connections)
-   [查询处理](#troubleshooting-does-not-process-queries)
-   [查询处理效率](#troubleshooting-too-slow)

## 安装 {#troubleshooting-installation-errors}

### 您无法使用Apt-get从ClickHouse存储库获取Deb软件包 {#you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get}

-   检查防火墙设置。
-   如果出于任何原因无法访问存储库，请按照[开始](../getting-started/index.md)中的描述下载软件包，并使用命令 `sudo dpkg -i <packages>` 手动安装它们。除此之外你还需要 `tzdata` 包。

## 连接到服务器 {#troubleshooting-accepts-no-connections}

可能出现的问题:

-   服务器未运行。
-   意外或错误的配置参数。

### 服务器未运行 {#server-is-not-running}

**检查服务器是否运行nnig**

命令:

``` bash
$ sudo service clickhouse-server status
```

如果服务器没有运行，请使用以下命令启动它:

``` bash
$ sudo service clickhouse-server start
```

**检查日志**

主日志 `clickhouse-server` 默认情况是在 `/var/log/clickhouse-server/clickhouse-server.log` 下。

如果服务器成功启动，您应该看到字符串:

-   `<Information> Application: starting up.` — Server started.
-   `<Information> Application: Ready for connections.` — Server is running and ready for connections.

如果 `clickhouse-server` 启动失败与配置错误，你应该看到 `<Error>` 具有错误描述的字符串。 例如:

``` text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

如果在文件末尾没有看到错误，请从如下字符串开始查看整个文件:

``` text
<Information> Application: starting up.
```

如果您尝试在服务器上启动第二个实例 `clickhouse-server` ，您将看到以下日志:

``` text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**查看系统日志**

如果你在 `clickhouse-server` 没有找到任何有用的信息或根本没有任何日志，您可以使用命令查看 `system.d` :

``` bash
$ sudo journalctl -u clickhouse-server
```

**在交互模式下启动clickhouse服务器**

``` bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

此命令将服务器作为带有自动启动脚本标准参数的交互式应用程序启动。 在这种模式下 `clickhouse-server` 打印控制台中的所有事件消息。

### 配置参数 {#configuration-parameters}

检查:

-   Docker设置。

    如果您在IPv6网络中的Docker中运行ClickHouse，请确保 `network=host` 被设置。

-   端点设置。

    检查 [listen_host](server-configuration-parameters/settings.md#server_configuration_parameters-listen_host) 和 [tcp_port](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port) 设置。

    ClickHouse服务器默认情况下仅接受本地主机连接。

-   HTTP协议设置。

    检查HTTP API的协议设置。

-   安全连接设置。

    检查:

    -   [tcp_port_secure](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) 设置。
    -   [SSL证书](server-configuration-parameters/settings.md#server_configuration_parameters-openssl) 设置.

    连接时使用正确的参数。 例如，使用 `clickhouse_client` 的时候使用 `port_secure` 参数 .

-   用户设置。

    您可能使用了错误的用户名或密码。

## 查询处理 {#troubleshooting-does-not-process-queries}

如果ClickHouse无法处理查询，它会向客户端发送错误描述。 在 `clickhouse-client` 您可以在控制台中获得错误的描述。 如果您使用的是HTTP接口，ClickHouse会在响应正文中发送错误描述。 例如:

``` bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

如果你使用 `clickhouse-client` 时设置了 `stack-trace` 参数，ClickHouse返回包含错误描述的服务器堆栈跟踪信息。

您可能会看到一条关于连接中断的消息。 在这种情况下，可以重复查询。 如果每次执行查询时连接中断，请检查服务器日志中是否存在错误。

## 查询处理效率 {#troubleshooting-too-slow}

如果您发现ClickHouse工作速度太慢，则需要为查询分析服务器资源和网络的负载。

您可以使用clickhouse-benchmark实用程序来分析查询。 它显示每秒处理的查询数、每秒处理的行数以及查询处理时间的百分位数。
