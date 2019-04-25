# 入门指南

## 系统要求

如果从官方仓库安装，需要确保您使用的是x86\_64处理器构架的Linux并且支持SSE 4.2指令集

检查是否支持SSE 4.2：

```bash
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

我们推荐使用Ubuntu或者Debian。终端必须使用UTF-8编码。

基于rpm的系统,你可以使用第三方的安装包：https://packagecloud.io/altinity/clickhouse 或者直接安装debian安装包。

ClickHouse还可以在FreeBSD与Mac OS X上工作。同时它可以在不支持SSE 4.2的x86\_64构架和AArch64 CPUs上编译。

## 安装

### 为Debian/Ubuntu安装

在`/etc/apt/sources.list` (或创建`/etc/apt/sources.list.d/clickhouse.list`文件)中添加仓库：

```text
deb http://repo.yandex.ru/clickhouse/deb/stable/ main/
```

如果你想使用最新的测试版本，请使用'testing'替换'stable'。

然后运行：

```bash
sudo apt-get install dirmngr    # optional
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4    # optional
sudo apt-get update
sudo apt-get install clickhouse-client clickhouse-server
```

你也可以从这里手动下载安装包：<https://repo.yandex.ru/clickhouse/deb/stable/main/>。

ClickHouse包含访问控制配置，它们位于`users.xml`文件中(与'config.xml'同目录)。
默认情况下，允许从任何地方使用默认的‘default’用户无密码的访问ClickHouse。参考‘user/default/networks’。
有关更多信息，请参考"Configuration files"部分。

### 使用源码安装

具体编译方式可以参考build.md。

你可以编译并安装它们。
你也可以直接使用而不进行安装。

```text
Client: dbms/programs/clickhouse-client
Server: dbms/programs/clickhouse-server
```

在服务器中为数据创建如下目录：

```text
/opt/clickhouse/data/default/
/opt/clickhouse/metadata/default/
```

(它们可以在server config中配置。)
为需要的用户运行‘chown’

日志的路径可以在server config (src/dbms/programs/server/config.xml)中配置。

### 其他的安装方法

Docker image：<https://hub.docker.com/r/yandex/clickhouse-server/>

CentOS或RHEL安装包：<https://github.com/Altinity/clickhouse-rpm-install>

Gentoo：`emerge clickhouse`

## 启动

可以运行如下命令在后台启动服务：

```bash
sudo service clickhouse-server start
```

可以在`/var/log/clickhouse-server/`目录中查看日志。

如果服务没有启动，请检查配置文件 `/etc/clickhouse-server/config.xml`。

你也可以在控制台中直接启动服务：

```bash
clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

在这种情况下，日志将被打印到控制台中，这在开发过程中很方便。
如果配置文件在当前目录中，你可以不指定‘--config-file’参数。它默认使用‘./config.xml’。

你可以使用命令行客户端连接到服务：

```bash
clickhouse-client
```

默认情况下它使用‘default’用户无密码的与localhost:9000服务建立连接。
客户端也可以用于连接远程服务，例如：

```bash
clickhouse-client --host=example.com
```

有关更多信息，请参考"Command-line client"部分。

检查系统是否工作：

```bash
milovidov@hostname:~/work/metrica/src/dbms/src/Client$ ./clickhouse-client
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


[Original article](https://clickhouse.yandex/docs/en/getting_started/) <!--hide-->
