---
toc_priority: 11
toc_title: 安装部署
---

# 安装 {#clickhouse-an-zhuang}

## 系统要求 {#xi-tong-yao-qiu}

ClickHouse可以在任何具有x86_64，AArch64或PowerPC64LE CPU架构的Linux，FreeBSD或Mac OS X上运行。

官方预构建的二进制文件通常针对x86_64进行编译，并利用`SSE 4.2`指令集，因此，除非另有说明，支持它的CPU使用将成为额外的系统需求。下面是检查当前CPU是否支持SSE 4.2的命令:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

要在不支持`SSE 4.2`或`AArch64`，`PowerPC64LE`架构的处理器上运行ClickHouse，您应该通过适当的配置调整从[源代码构建ClickHouse](#from-sources)。

## 可用安装包 {#install-from-deb-packages}

### `DEB`安装包

建议使用Debian或Ubuntu的官方预编译`deb`软件包。运行以下命令来安装包:

``` bash
{% include 'install/deb.sh' %}
```

如果您想使用最新的版本，请用`testing`替代`stable`(我们只推荐您用于测试环境)。

你也可以从这里手动下载安装包：[下载](https://repo.clickhouse.tech/deb/stable/main/)。

安装包列表：

- `clickhouse-common-static` — ClickHouse编译的二进制文件。
- `clickhouse-server` — 创建`clickhouse-server`软连接，并安装默认配置服务
- `clickhouse-client` — 创建`clickhouse-client`客户端工具软连接，并安装客户端配置文件。
- `clickhouse-common-static-dbg` — 带有调试信息的ClickHouse二进制文件。

### `RPM`安装包 {#from-rpm-packages}

推荐使用CentOS、RedHat和所有其他基于rpm的Linux发行版的官方预编译`rpm`包。

首先，您需要添加官方存储库：

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

如果您想使用最新的版本，请用`testing`替代`stable`(我们只推荐您用于测试环境)。`prestable`有时也可用。

然后运行命令安装：

``` bash
sudo yum install clickhouse-server clickhouse-client
```

你也可以从这里手动下载安装包：[下载](https://repo.clickhouse.tech/rpm/stable/x86_64)。

### `Tgz`安装包 {#from-tgz-archives}

如果您的操作系统不支持安装`deb`或`rpm`包，建议使用官方预编译的`tgz`软件包。

所需的版本可以通过`curl`或`wget`从存储库`https://repo.clickhouse.tech/tgz/`下载。

下载后解压缩下载资源文件并使用安装脚本进行安装。以下是一个最新版本的安装示例:

``` bash
export LATEST_VERSION=`curl https://api.github.com/repos/ClickHouse/ClickHouse/tags 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -n 1`
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-dbg-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-server-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-client-$LATEST_VERSION.tgz

tar -xzvf clickhouse-common-static-$LATEST_VERSION.tgz
sudo clickhouse-common-static-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-common-static-dbg-$LATEST_VERSION.tgz
sudo clickhouse-common-static-dbg-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-server-$LATEST_VERSION.tgz
sudo clickhouse-server-$LATEST_VERSION/install/doinst.sh
sudo /etc/init.d/clickhouse-server start

tar -xzvf clickhouse-client-$LATEST_VERSION.tgz
sudo clickhouse-client-$LATEST_VERSION/install/doinst.sh
```

对于生产环境，建议使用最新的`stable`版本。你可以在GitHub页面https://github.com/ClickHouse/ClickHouse/tags找到它，它以后缀`-stable`标志。

### `Docker`安装包 {#from-docker-image}

要在Docker中运行ClickHouse，请遵循[Docker Hub](https://hub.docker.com/r/yandex/clickhouse-server/)上的指南。它是官方的`deb`安装包。

### 其他环境安装包 {#from-other}

对于非linux操作系统和Arch64 CPU架构，ClickHouse将会以`master`分支的最新提交的进行编译提供(它将会有几小时的延迟)。

-   [macOS](https://builds.clickhouse.tech/master/macos/clickhouse) — `curl -O 'https://builds.clickhouse.tech/master/macos/clickhouse' && chmod a+x ./clickhouse`
-   [FreeBSD](https://builds.clickhouse.tech/master/freebsd/clickhouse) — `curl -O 'https://builds.clickhouse.tech/master/freebsd/clickhouse' && chmod a+x ./clickhouse`
-   [AArch64](https://builds.clickhouse.tech/master/aarch64/clickhouse) — `curl -O 'https://builds.clickhouse.tech/master/aarch64/clickhouse' && chmod a+x ./clickhouse`

下载后，您可以使用`clickhouse client`连接服务，或者使用`clickhouse local`模式处理数据，不过您必须要额外在GitHub下载[server](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.xml)和[users](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/users.xml)配置文件。

不建议在生产环境中使用这些构建版本，因为它们没有经过充分的测试，但是您可以自行承担这样做的风险。它们只是ClickHouse功能的一个部分。

### 使用源码安装 {#from-sources}

要手动编译ClickHouse, 请遵循[Linux](../development/build.md)或[Mac OS X](../development/build-osx.md)说明。

您可以编译并安装它们，也可以使用不安装包的程序。通过手动构建，您可以禁用`SSE 4.2`或`AArch64 cpu`。

      Client: programs/clickhouse-client
      Server: programs/clickhouse-server

您需要创建一个数据和元数据文件夹，并为所需的用户`chown`授权。它们的路径可以在服务器配置(`src/programs/server/config.xml`)中改变，默认情况下它们是:

      /opt/clickhouse/data/default/
      /opt/clickhouse/metadata/default/

在Gentoo上，你可以使用`emerge clickhouse`从源代码安装ClickHouse。

## 启动 {#qi-dong}

如果没有`service`，可以运行如下命令在后台启动服务：

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

日志文件将输出在`/var/log/clickhouse-server/`文件夹。

如果服务器没有启动，检查`/etc/clickhouse-server/config.xml`中的配置。

您也可以手动从控制台启动服务器:

```bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

在这种情况下，日志将被打印到控制台，这在开发过程中很方便。

如果配置文件在当前目录中，则不需要指定`——config-file`参数。默认情况下，它的路径为`./config.xml`。

ClickHouse支持访问限制设置。它们位于`users.xml`文件(与`config.xml`同级目录)。
默认情况下，允许`default`用户从任何地方访问，不需要密码。可查看`user/default/networks`。
更多信息，请参见[Configuration Files](../operations/configuration-files.md)。

启动服务后，您可以使用命令行客户端连接到它:

``` bash
$ clickhouse-client
```

默认情况下，使用`default`用户并不携带密码连接到`localhost:9000`。还可以使用`--host`参数连接到指定服务器。

终端必须使用UTF-8编码。
更多信息，请参阅[Command-line client](../interfaces/cli.md)。

示例：

```
$ ./clickhouse-client
ClickHouse client version 0.0.18749.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.18749.

:) SELECT 1

SELECT 1

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.003 sec.

:)
```

**恭喜，系统已经工作了!**

为了继续进行实验，你可以尝试下载测试数据集或查看[教程](https://clickhouse.tech/tutorial.html)。

[原始文章](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
