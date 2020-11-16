# 安装 {#clickhouse-an-zhuang}

## 系统要求 {#xi-tong-yao-qiu}

ClickHouse可以在任何具有x86\_64，AArch64或PowerPC64LE CPU架构的Linux，FreeBSD或Mac OS X上运行。

虽然预构建的二进制文件通常是为x86  \_64编译并利用SSE 4.2指令集，但除非另有说明，否则使用支持它的CPU将成为额外的系统要求。这是检查当前CPU是否支持SSE 4.2的命令：

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

要在不支持SSE 4.2或具有AArch64或PowerPC64LE体系结构的处理器上运行ClickHouse，您应该[通过源构建ClickHouse](#from-sources)进行适当的配置调整。

## 可用的安装选项 {#install-from-deb-packages}

建议为Debian或Ubuntu使用官方的预编译`deb`软件包。 运行以下命令以安装软件包：

然后运行：

``` bash
{% include 'install/deb.sh' %}
```

你也可以从这里手动下载安装包：https://repo.clickhouse.tech/deb/stable/main/。

如果你想使用最新的测试版本，请使用`testing`替换`stable`。

### 来自RPM包 {#from-rpm-packages}

Yandex ClickHouse团队建议使用官方预编译的`rpm`软件包，用于CentOS，RedHat和所有其他基于rpm的Linux发行版。

首先，您需要添加官方存储库：

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

如果您想使用最新版本，请将`stable`替换为`testing`（建议您在测试环境中使用）。

然后运行这些命令以实际安装包：

``` bash
sudo yum install clickhouse-server clickhouse-client
```

您也可以从此处手动下载和安装软件包：https://repo.clickhouse.tech/rpm/stable/x86_64。

### 来自Docker {#from-docker-image}

要在Docker中运行ClickHouse，请遵循[码头工人中心](https://hub.docker.com/r/yandex/clickhouse-server/)上的指南。那些图像使用官方的`deb`包。

### 使用源码安装 {#from-sources}

具体编译方式可以参考build.md。

你可以编译并安装它们。
你也可以直接使用而不进行安装。

``` text
Client: programs/clickhouse-client
Server: programs/clickhouse-server
```

在服务器中为数据创建如下目录：

``` text
/opt/clickhouse/data/default/
/opt/clickhouse/metadata/default/
```

(它们可以在server config中配置。)
为需要的用户运行’chown’

日志的路径可以在server config (src/programs/server/config.xml)中配置。

## 启动 {#qi-dong}

可以运行如下命令在后台启动服务：

``` bash
sudo service clickhouse-server start
```

可以在`/var/log/clickhouse-server/`目录中查看日志。

如果服务没有启动，请检查配置文件 `/etc/clickhouse-server/config.xml`。

你也可以在控制台中直接启动服务：

``` bash
clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

在这种情况下，日志将被打印到控制台中，这在开发过程中很方便。
如果配置文件在当前目录中，你可以不指定’–config-file’参数。它默认使用’./config.xml’。

你可以使用命令行客户端连接到服务：

``` bash
clickhouse-client
```

默认情况下它使用’default’用户无密码的与localhost:9000服务建立连接。
客户端也可以用于连接远程服务，例如：

``` bash
clickhouse-client --host=example.com
```

有关更多信息，请参考«Command-line client»部分。

检查系统是否工作：

``` bash
milovidov@hostname:~/work/metrica/src/src/Client$ ./clickhouse-client
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

为了继续进行实验，你可以尝试下载测试数据集。

[原始文章](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
