---
title: 故障排查
---

[//]: # "此文件包含在 FAQ > 故障排查中"

* [安装](#troubleshooting-installation-errors)
* [连接服务器](#troubleshooting-accepts-no-connections)
* [查询处理](#troubleshooting-does-not-process-queries)
* [查询处理效率](#troubleshooting-too-slow)

<div id="troubleshooting-installation-errors">
  ## 安装
</div>

<div id="you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get">
  ### 无法使用 apt-get 从 ClickHouse 软件源获取 deb 软件包
</div>

* 检查防火墙设置。
* 如果因任何原因无法访问该软件源，请按照[安装指南](../getting-started/install.md)中的说明下载软件包，并使用 `sudo dpkg -i <packages>` 命令手动安装。您还需要安装 `tzdata` 软件包。

<div id="you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get">
  ### 无法使用 apt-get 更新来自 ClickHouse 软件源的 deb 软件包
</div>

* GPG 密钥变更时，可能会出现此问题。

请按照 [setup](../getting-started/install.md#setup-the-debian-repository) 页面中的说明更新软件源配置。

<div id="you-get-different-warnings-with-apt-get-update">
  ### 运行 `apt-get update` 时可能会看到不同的警告
</div>

* 常见的已完成警告信息如下：

```bash
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```bash
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```text
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```bash
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

要解决上述问题，请使用以下脚本：

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

<div id="you-cant-get-packages-with-yum-because-of-wrong-signature">
  ### 因签名错误，无法使用 yum 获取软件包
</div>

可能的原因：缓存有问题，可能是在 2022-09 更新 GPG 密钥后损坏了。

解决方法是清理 yum 的缓存和 lib 目录：

```bash
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

然后按照[安装指南](../getting-started/install.md#from-rpm-packages)进行操作

<div id="you-cant-run-docker-container">
  ### 你无法运行 Docker 容器
</div>

你运行了一个简单的 `docker run clickhouse/clickhouse-server`，但它崩溃了，并显示出类似下面的 堆栈跟踪：

```bash
$ docker run -it clickhouse/clickhouse-server
........
Poco::Exception. Code: 1000, e.code() = 0, System exception: cannot start thread, Stack trace (when copying this message, always include the lines below):

0. Poco::ThreadImpl::startImpl(Poco::SharedPtr<Poco::Runnable, Poco::ReferenceCounter, Poco::ReleasePolicy<Poco::Runnable>>) @ 0x00000000157c7b34
1. Poco::Thread::start(Poco::Runnable&) @ 0x00000000157c8a0e
2. BaseDaemon::initializeTerminationAndSignalProcessing() @ 0x000000000d267a14
3. BaseDaemon::initialize(Poco::Util::Application&) @ 0x000000000d2652cb
4. DB::Server::initialize(Poco::Util::Application&) @ 0x000000000d128b38
5. Poco::Util::Application::run() @ 0x000000001581cfda
6. DB::Server::run() @ 0x000000000d1288f0
7. Poco::Util::ServerApplication::run(int, char**) @ 0x0000000015825e27
8. mainEntryClickHouseServer(int, char**) @ 0x000000000d125b38
9. main @ 0x0000000007ea4eee
10. ? @ 0x00007f67ff946d90
11. ? @ 0x00007f67ff946e40
12. _start @ 0x00000000062e802e
 (version 24.10.1.2812 (official build))
```

原因是 docker daemon 版本过旧，低于 `20.10.10`。可通过升级它，或运行 `docker run [--privileged | --security-opt seccomp=unconfined]` 来解决。后者会带来安全风险。

<div id="troubleshooting-accepts-no-connections">
  ## 连接到服务器
</div>

可能存在以下问题：

* 服务器未启动。
* 配置参数异常或不正确。

<div id="server-is-not-running">
  ### 服务器未运行
</div>

**检查服务器是否在运行**

命令：

```bash
$ sudo service clickhouse-server status
```

如果服务器未运行，请使用以下命令启动：

```bash
$ sudo service clickhouse-server start
```

**检查日志**

`clickhouse-server` 的主日志默认位于 `/var/log/clickhouse-server/clickhouse-server.log`。

如果服务器已成功启动，你应该会看到以下字符串：

* `<Information> Application: starting up.` — 服务器已启动。
* `<Information> Application: Ready for connections.` — 服务器正在运行，并已准备好接受连接。

如果 `clickhouse-server` 因配置错误而启动失败，你应该会看到包含错误描述的 `<Error>` 字符串。例如：

```text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

如果你在文件末尾没有看到错误，请从以下字符串开始检查整个文件：

```text
<Information> Application: starting up.
```

如果你尝试在服务器上启动第二个 `clickhouse-server` 实例，就会看到如下日志：

```text
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

**查看 system.d 日志**

如果在 `clickhouse-server` 日志中找不到任何有用信息，或者根本没有日志，可以使用以下命令查看 `system.d` 日志：

```bash
$ sudo journalctl -u clickhouse-server
```

**在交互模式下启动 clickhouse-server**

```bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

此命令会以交互式应用的方式启动服务器，并使用自动启动脚本的标准参数。在此模式下，`clickhouse-server` 会在控制台中打印所有事件消息。

<div id="configuration-parameters">
  ### 配置参数
</div>

检查以下各项：

* Docker 设置。

  如果您在 IPv6 网络中的 Docker 里运行 ClickHouse，请确保已设置 `network=host`。

* 端点设置。

  检查 [listen&#95;host](../operations/server-configuration-parameters/settings.md#listen_host) 和 [tcp&#95;port](../operations/server-configuration-parameters/settings.md#tcp_port) 设置。

  默认情况下，ClickHouse server 仅接受来自 localhost 的连接。

* HTTP 协议设置。

  检查 HTTP API 的协议设置。

* 安全连接设置。

  检查：

  * [tcp&#95;port&#95;secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure) 设置。
  * [SSL certificates](../operations/server-configuration-parameters/settings.md#openssl) 相关设置。

    连接时请使用正确的参数。例如，对 `clickhouse_client` 使用 `port_secure` 参数。

* 用户设置。

  您使用的用户名或密码可能不正确。

<div id="troubleshooting-does-not-process-queries">
  ## 查询处理
</div>

如果 ClickHouse 无法处理查询，它会向客户端返回错误说明。在 `clickhouse-client` 中，你会在控制台看到错误说明。如果使用的是 HTTP 接口，ClickHouse 会在响应正文中返回错误说明。例如：

```bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

如果使用 `stack-trace` 参数启动 `clickhouse-client`，ClickHouse 会返回服务器堆栈跟踪以及错误说明。

你可能会看到连接中断的消息。在这种情况下，可以重新执行该查询。如果每次执行该查询时连接都会中断，请检查服务器日志中是否有错误。

<div id="troubleshooting-too-slow">
  ## 查询处理效率
</div>

如果你发现 ClickHouse 运行过慢，就需要分析你的查询给服务器资源和网络带来的负载。

你可以使用 clickhouse-benchmark 工具来分析查询。它会显示每秒处理的查询数、每秒处理的行数，以及查询处理时间的百分位数。