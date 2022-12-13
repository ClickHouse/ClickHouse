---
sidebar_position: 66
sidebar_label: ClickHouse Keeper
---

# [预发生产] ClickHouse Keeper {#clickHouse-keeper}

ClickHouse 服务为了 [副本](../engines/table-engines/mergetree-family/replication.md) 和 [分布式DDL](../sql-reference/distributed-ddl.md) 查询执行使用 [ZooKeeper](https://zookeeper.apache.org/) 协调系统. ClickHouse Keeper 和 ZooKeeper是相互兼容的，可互相替代.

:::danger "警告"
这个功能当前还在预发生产阶段. 我们只是在内部部分使用于生产环境和测试CI中.
:::

## 实现细节 {#implementation-details}

ZooKeeper最早的非常著名的开源协调系统之一. 它是通过Java语言实现的, 有一个相当节点和强大的数据模型. ZooKeeper的协调算法叫做 ZAB (ZooKeeper Atomic Broadcast) zk不能保证读取的线性化, 以为每个zk节点服务都是通过本地线性读的. ClickHouse Keeper是通过C++写的，和zookeeper不一样， ClickHouse Keeper使用的[RAFT algorithm](https://raft.github.io/) [implementation](https://github.com/eBay/NuRaft)算法. 这个算法允许线性读和写, 已经有几种不同的语言的开源实现.

ClickHouse Keeper 默认提供了一些保证和ZooKeeper是一样的 (线性写, 非线性读)和. clickhouse keeper有一个兼容的客户端服务端协议, 所以任何标准的zookeeper客户端都可以用来与clickhouse keeper进行交互. 快照和日志的格式与ZooKeeper不兼容, 但是通过`clickhouse-keeper-converter` 允许转换 ZooKeeper 数据到 ClickHouse Keeper 快照. ClickHouse Keeper的interserver协议和zookeeper也不兼容，所以ZooKeeper / ClickHouse Keeper 混合部署集群是不可能的.

ClickHouse Keeper支持访问控制列表(ACL)的方式和[ZooKeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) 一样. ClickHouse Keeper支持相同的权限集合并且有完全相同的内置方案如：`world`, `auth`, `digest`, `host` and `ip`. 权限校验使用 `用户名:密码`方式配对. 密码通过Base64算法加密.

:::info "注意"
不支持外部集成
:::

## 配置 {#configuration}

ClickHouse Keeper 完全可以作为ZooKeeper的独立替代品或者作为ClickHouse server服务的内部组件, 但是这两种方式下的配置使用 `.xml` 格式也几乎都是相同的. ClickHouse Keeper 配置的标签是 `<keeper_server>`. Keeper 配置有以下这些参数:

-    `tcp_port` — 客户端连接的端口(ZooKeeper默认是`2181`).
-    `tcp_port_secure` — client 和 keeper-server之间的SSL连接的安全端口.
-    `server_id` — 唯一的服务器ID, ClickHouse Keeper 集群的每个参与组件都必须有一个唯一的编号(1, 2, 3, 等等).
-    `log_storage_path` — 协调日志的路径, 最好存放在不繁忙的机器上 (和ZooKeeper一样).
-    `snapshot_storage_path` — 协调快照的路径.

其他常见参数继承自ClickHouse server的配置 (`listen_host`, `logger`, 等等).

内部协调配置位于`<keeper_server>.<coordination_settings>`部分:

-    `operation_timeout_ms` — 单个客户端操作的超时时间(ms)(默认值:10000)。
-    `min_session_timeout_ms` — 客户端会话的最小超时时间(ms)(默认值:10000)。
-    `session_timeout_ms` — 客户端会话最大超时时间(ms)(默认100000)。
-    `dead_session_check_period_ms` — ClickHouse Keeper检查死会话并删除它们的频率(毫秒)(默认值:500)。
-    `heart_beat_interval_ms` — ClickHouse Keeper的leader发送心跳频率(毫秒)(默认为500)。
-    `election_timeout_lower_bound_ms` — 如果follower在此间隔内没有收到leader的心跳，那么它可以启动leader选举(默认为1000).
-    `election_timeout_upper_bound_ms` — 如果follower在此间隔内没有收到leader的心跳，那么它必须启动leader选举(默认为2000)。
-    `rotate_log_storage_interval` — 单个文件中存储的日志记录数量(默认100000条)。
-    `reserved_log_items` — 在压缩之前需要存储多少协调日志记录(默认100000)。
-    `snapshot_distance` — ClickHouse Keeper创建新快照的频率(以日志记录的数量为单位)(默认100000)。
-    `snapshots_to_keep` — 保留多少个快照(默认值:3)。
-    `stale_log_gap` — 当leader认为follower过时并发送快照给follower而不是日志时的阈值(默认值:10000)。
-    `fresh_log_gap` — 当节点变成新鲜时的间隔(默认值:200)。
-    `max_requests_batch_size` - 发送到RAFT之前的最大批量请求数(默认值:100)。
-    `force_sync` — 在每次写入协调日志时是否调用' fsync '(默认值:true)。
-    `quorum_reads` — 通过整个RAFT共识以类似的速度执行读请求和写请求(默认值:false)。
-    `raft_logs_level` — 关于协调的文本日志级别 (trace, debug, 等等) (默认: system default).
-    `auto_forwarding` — 允许将follower的请求转发到leader (默认: true).
-    `shutdown_timeout` — 等待内部连接完成并关闭(ms)(默认值:5000)。
-    `startup_timeout` — 如果服务器在指定的超时时间内没有连接到其他仲裁参与者，它将终止(ms)(默认值:30000)。
-    `four_letter_word_white_list` — 4个字母的白名单列表 (默认: "conf,cons,crst,envi,ruok,srst,srvr,stat,wchc,wchs,dirs,mntr,isro").

仲裁配置位于 `<keeper_server>.<raft_configuration>` 部分，并且保护一些描述

整个仲裁的唯一参数是“secure”，它为仲裁参与者之间的通信启用加密连接。如果节点之间的内部通信需要SSL连接，则该参数可以设置为“true”，否则不指定。

每个`<server>`的主要参数是:

-    `id` — 仲裁中的服务器标识符。
-    `hostname` — 放置该服务器的主机名。
-    `port` — 服务器监听连接的端口。


在[integration tests](https://github.com/ClickHouse/ClickHouse/tree/master/tests/integration)可以找到带有 `test_keeper_` 前缀的3个节点的仲裁配置示例. 服务配置举例如下 #1:

```xml
<keeper_server>
    <tcp_port>2181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

    <coordination_settings>
        <operation_timeout_ms>10000</operation_timeout_ms>
        <session_timeout_ms>30000</session_timeout_ms>
        <raft_logs_level>trace</raft_logs_level>
    </coordination_settings>

    <raft_configuration>
        <server>
            <id>1</id>
            <hostname>zoo1</hostname>
            <port>9444</port>
        </server>
        <server>
            <id>2</id>
            <hostname>zoo2</hostname>
            <port>9444</port>
        </server>
        <server>
            <id>3</id>
            <hostname>zoo3</hostname>
            <port>9444</port>
        </server>
    </raft_configuration>
</keeper_server>
```

## 如何运行 {#how-to-run}

ClickHouse Keeper被绑定到ClickHouse服务器包中，只需添加配置' <keeper_server> '，并像往常一样启动ClickHouse服务器。如果你想运行独立的ClickHouse Keeper，你可以用类似的方式启动它:

```bash
clickhouse-keeper --config /etc/your_path_to_config/config.xml
```

如果你没有符号链接(' clickhouse-keeper ')，你可以创建它或指定' keeper '作为参数:

```bash
clickhouse keeper --config /etc/your_path_to_config/config.xml
```

## 四字母命令 {#four-letter-word-commands}

ClickHouse Keeper还提供了与Zookeeper几乎相同的4lw命令。每个命令由4个字母组成，如“mntr”、“stat”等。还有一些更有趣的命令:' stat '给出了服务器和连接客户端的一般信息，而' srvr '和' cons '分别给出了服务器和连接的详细信息。

4lw命令有一个白名单配置“four_letter_word_white_list”，它的默认值为“conf,cons,crst,envi,ruok,srst,srvr,stat,wchc,wchs,dirs,mntr,isro”。

您可以通过telnet或nc在客户端端口向ClickHouse Keeper发出命令。

```
echo mntr | nc localhost 9181
```

下面是4lw的详细命令:

- `ruok`: 测试服务器运行时是否处于无错误状态。如果服务器正在运行，它将用imok响应。否则它将完全不响应。“imok”的响应并不一定表明服务器已加入仲裁，只是表明服务器进程处于活动状态并绑定到指定的客户端端口。使用“stat”获取状态wrt仲裁和客户端连接信息的详细信息。

```
imok
```

- `mntr`: 输出可用于监视集群运行状况的变量列表。

```
zk_version      v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
zk_avg_latency  0
zk_max_latency  0
zk_min_latency  0
zk_packets_received     68
zk_packets_sent 68
zk_num_alive_connections        1
zk_outstanding_requests 0
zk_server_state leader
zk_znode_count  4
zk_watch_count  1
zk_ephemerals_count     0
zk_approximate_data_size        723
zk_open_file_descriptor_count   310
zk_max_file_descriptor_count    10240
zk_followers    0
zk_synced_followers     0
```

- `srvr`: 列出服务器的完整详细信息。

```
ClickHouse Keeper version: v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
Latency min/avg/max: 0/0/0
Received: 2
Sent : 2
Connections: 1
Outstanding: 0
Zxid: 34
Mode: leader
Node count: 4
```

- `stat`: 列出服务器和连接客户机的简要详细信息。

```
ClickHouse Keeper version: v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
Clients:
 192.168.1.1:52852(recved=0,sent=0)
 192.168.1.1:52042(recved=24,sent=48)
Latency min/avg/max: 0/0/0
Received: 4
Sent : 4
Connections: 1
Outstanding: 0
Zxid: 36
Mode: leader
Node count: 4
```

- `srst`: 重置服务器统计数据。该命令将影响' srvr '， ' mntr '和' stat '的结果。

```
Server stats reset.
```

- `conf`: 打印服务配置详细信息。

```
server_id=1
tcp_port=2181
four_letter_word_white_list=*
log_storage_path=./coordination/logs
snapshot_storage_path=./coordination/snapshots
max_requests_batch_size=100
session_timeout_ms=30000
operation_timeout_ms=10000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
election_timeout_lower_bound_ms=1000
election_timeout_upper_bound_ms=2000
reserved_log_items=1000000000000000
snapshot_distance=10000
auto_forwarding=true
shutdown_timeout=5000
startup_timeout=240000
raft_logs_level=information
snapshots_to_keep=3
rotate_log_storage_interval=100000
stale_log_gap=10000
fresh_log_gap=200
max_requests_batch_size=100
quorum_reads=false
force_sync=false
compress_logs=true
compress_snapshots_with_zstd_format=true
configuration_change_tries_count=20
```

- `cons`: 列出所有连接到此服务器的客户端的完整连接/会话详细信息。包括接收/发送的包数、会话id、操作延迟、最后执行的操作等信息。

```
 192.168.1.1:52163(recved=0,sent=0,sid=0xffffffffffffffff,lop=NA,est=1636454787393,to=30000,lzxid=0xffffffffffffffff,lresp=0,llat=0,minlat=0,avglat=0,maxlat=0)
 192.168.1.1:52042(recved=9,sent=18,sid=0x0000000000000001,lop=List,est=1636454739887,to=30000,lcxid=0x0000000000000005,lzxid=0x0000000000000005,lresp=1636454739892,llat=0,minlat=0,avglat=0,maxlat=0)
```

- `crst`: 重置所有连接的连接/会话统计信息。

```
Connection stats reset.
```

- `envi`: 打印服务环境详细信息

```
Environment:
clickhouse.keeper.version=v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
host.name=ZBMAC-C02D4054M.local
os.name=Darwin
os.arch=x86_64
os.version=19.6.0
cpu.count=12
user.name=root
user.home=/Users/JackyWoo/
user.dir=/Users/JackyWoo/project/jd/clickhouse/cmake-build-debug/programs/
user.tmp=/var/folders/b4/smbq5mfj7578f2jzwn602tt40000gn/T/
```


- `dirs`: 以字节为单位显示快照和日志文件的总大小

```
snapshot_dir_size: 0
log_dir_size: 3875
```

- `isro`: 测试服务器是否以只读模式运行。如果处于只读模式，服务器将响应“ro”，如果不是只读模式，则响应“rw”。

```
rw
```

- `wchs`: 列出服务器的监视的简要信息。

```
1 connections watching 1 paths
Total watches:1
```

- `wchc`: 按会话列出服务器的监视的详细信息。这将输出一个会话(连接)列表和相关的监视(路径)。注意，根据监视的数量，此操作可能会很昂贵(即影响服务器性能)，请谨慎使用。

```
0x0000000000000001
    /clickhouse/task_queue/ddl
```

- `wchp`: 按路径列出有关服务器的监视的详细信息。这将输出一个带有关联会话的路径(znode)列表。注意，根据监视的数量，此操作可能昂贵(即影响服务器性能)，请谨慎使用。

```
/clickhouse/task_queue/ddl
    0x0000000000000001
```

- `dump`: 列出未完成的会话和临时节点。这只对领导者有效。

```
Sessions dump (2):
0x0000000000000001
0x0000000000000002
Sessions with Ephemerals (1):
0x0000000000000001
 /clickhouse/task_queue/ddl
```

## [实现] 从ZooKeeper迁移 {#migration-from-zookeeper}

从ZooKeeper无缝迁移到ClickHouse Keeper是不可能的，你必须停止你的ZooKeeper集群，转换数据并启动ClickHouse Keeper。' ClickHouse - Keeper -converter '工具允许将ZooKeeper日志和快照转换为ClickHouse Keeper快照。它只适用于ZooKeeper 大于 3.4。迁移的步骤:

1. 停掉ZooKeeper节点.

2. 可选，但建议:找到ZooKeeper leader节点，重新启停。它会强制ZooKeeper创建一致的快照。

3. 在leader节点运行`clickhouse-keeper-converter`, 如下:

```bash
clickhouse-keeper-converter --zookeeper-logs-dir /var/lib/zookeeper/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/version-2 --output-dir /path/to/clickhouse/keeper/snapshots
```

4. 将快照复制到配置了“keeper”的ClickHouse服务器节点，或者启动ClickHouse keeper而不是ZooKeeper。快照必须在所有节点上持久保存，否则，空节点可能更快，其中一个节点可能成为leader.

[Original article](https://clickhouse.com/docs/en/operations/clickhouse-keeper/) <!--hide-->