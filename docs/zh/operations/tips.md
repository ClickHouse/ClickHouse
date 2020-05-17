# 使用建议 {#usage-recommendations}

## CPU {#cpu}

必须支持SSE4.2指令集。 现代处理器（自2008年以来）支持它。

选择处理器时，与较少的内核和较高的时钟速率相比，更喜欢大量内核和稍慢的时钟速率。
例如，具有2600MHz的16核心比具有3600MHz的8核心更好。

## 超线程 {#hyper-threading}

不要禁用超线程。 它有助于某些查询，但不适用于其他查询。

## 涡轮增压 {#turbo-boost}

强烈推荐涡轮增压。 它显着提高了典型负载的性能。
您可以使用 `turbostat` 要查看负载下的CPU的实际时钟速率。

## CPU缩放调控器 {#cpu-scaling-governor}

始终使用 `performance` 缩放调控器。 该 `on-demand` 随着需求的不断增加，缩放调节器的工作要糟糕得多。

``` bash
echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## CPU限制 {#cpu-limitations}

处理器可能会过热。 使用 `dmesg` 看看CPU的时钟速率是否由于过热而受到限制。
此限制也可以在数据中心级别的外部设置。 您可以使用 `turbostat` 在负载下监视它。

## RAM {#ram}

对于少量数据（高达-200GB压缩），最好使用与数据量一样多的内存。
对于大量数据和处理交互式（在线）查询时，应使用合理数量的RAM（128GB或更多），以便热数据子集适合页面缓存。
即使对于每台服务器约50TB的数据量，使用128GB的RAM与64GB相比显着提高了查询性能。

## 交换文件 {#swap-file}

始终禁用交换文件。 不这样做的唯一原因是，如果您使用的ClickHouse在您的个人笔记本电脑。

## 巨大的页面 {#huge-pages}

始终禁用透明巨大的页面。 它会干扰内存分alloc，从而导致显着的性能下降。

``` bash
echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

使用 `perf top` 观看内核中用于内存管理的时间。
永久巨大的页面也不需要被分配。

## 存储子系统 {#storage-subsystem}

如果您的预算允许您使用SSD，请使用SSD。
如果没有，请使用硬盘。 SATA硬盘7200转就行了。

优先选择带有本地硬盘驱动器的大量服务器，而不是带有附加磁盘架的小量服务器。
但是对于存储具有罕见查询的档案，货架将起作用。

## RAID {#raid}

当使用硬盘，你可以结合他们的RAID-10，RAID-5，RAID-6或RAID-50。
对于Linux，软件RAID更好（与 `mdadm`). 我们不建议使用LVM。
当创建RAID-10，选择 `far` 布局。
如果您的预算允许，请选择RAID-10。

如果您有超过4个磁盘，请使用RAID-6（首选）或RAID-50，而不是RAID-5。
当使用RAID-5、RAID-6或RAID-50时，始终增加stripe\_cache\_size，因为默认值通常不是最佳选择。

``` bash
echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

使用以下公式，从设备数量和块大小计算确切数量: `2 * num_devices * chunk_size_in_bytes / 4096`.

1025KB的块大小足以满足所有RAID配置。
切勿将块大小设置得太小或太大。

您可以在SSD上使用RAID-0。
无论使用何种RAID，始终使用复制来保证数据安全。

使用长队列启用NCQ。 对于HDD，选择CFQ调度程序，对于SSD，选择noop。 不要减少 ‘readahead’ 设置。
对于HDD，启用写入缓存。

## 文件系统 {#file-system}

Ext4是最可靠的选择。 设置挂载选项 `noatime, nobarrier`.
XFS也是合适的，但它还没有经过ClickHouse的彻底测试。
大多数其他文件系统也应该正常工作。 具有延迟分配的文件系统工作得更好。

## Linux内核 {#linux-kernel}

不要使用过时的Linux内核。

## 网络 {#network}

如果您使用的是IPv6，请增加路由缓存的大小。
3.2之前的Linux内核在IPv6实现方面遇到了许多问题。

如果可能的话，至少使用一个10GB的网络。 1Gb也可以工作，但对于使用数十tb的数据修补副本或处理具有大量中间数据的分布式查询，情况会更糟。

## 动物园管理员 {#zookeeper}

您可能已经将ZooKeeper用于其他目的。 您可以使用相同的zookeeper安装，如果它还没有超载。

It’s best to use a fresh version of ZooKeeper – 3.4.9 or later. The version in stable Linux distributions may be outdated.

You should never use manually written scripts to transfer data between different ZooKeeper clusters, because the result will be incorrect for sequential nodes. Never use the «zkcopy» utility for the same reason: https://github.com/ksprojects/zkcopy/issues/15

如果要将现有ZooKeeper集群分为两个，正确的方法是增加其副本的数量，然后将其重新配置为两个独立的集群。

不要在与ClickHouse相同的服务器上运行ZooKeeper。 由于ZooKeeper对延迟非常敏感，ClickHouse可能会利用所有可用的系统资源。

使用默认设置，ZooKeeper是一个定时炸弹:

> 使用默认配置时，ZooKeeper服务器不会从旧快照和日志中删除文件（请参阅autopurge），这是操作员的责任。

必须拆除炸弹

下面的ZooKeeper（3.5.1）配置在Yandex中使用。梅地卡生产环境截至2017年5月20日:

动物园cfg:

``` bash
# http://hadoop.apache.org/zookeeper/docs/current/zookeeperAdmin.html

# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
initLimit=30000
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=10

maxClientCnxns=2000

maxSessionTimeout=60000000
# the directory where the snapshot is stored.
dataDir=/opt/zookeeper/{{ '{{' }} cluster['name'] {{ '}}' }}/data
# Place the dataLogDir to a separate physical disc for better performance
dataLogDir=/opt/zookeeper/{{ '{{' }} cluster['name'] {{ '}}' }}/logs

autopurge.snapRetainCount=10
autopurge.purgeInterval=1


# To avoid seeks ZooKeeper allocates space in the transaction log file in
# blocks of preAllocSize kilobytes. The default block size is 64M. One reason
# for changing the size of the blocks is to reduce the block size if snapshots
# are taken more often. (Also, see snapCount).
preAllocSize=131072

# Clients can submit requests faster than ZooKeeper can process them,
# especially if there are a lot of clients. To prevent ZooKeeper from running
# out of memory due to queued requests, ZooKeeper will throttle clients so that
# there is no more than globalOutstandingLimit outstanding requests in the
# system. The default limit is 1,000.ZooKeeper logs transactions to a
# transaction log. After snapCount transactions are written to a log file a
# snapshot is started and a new transaction log file is started. The default
# snapCount is 10,000.
snapCount=3000000

# If this option is defined, requests will be will logged to a trace file named
# traceFile.year.month.day.
#traceFile=

# Leader accepts client connections. Default value is "yes". The leader machine
# coordinates updates. For higher update throughput at thes slight expense of
# read throughput the leader can be configured to not accept clients and focus
# on coordination.
leaderServes=yes

standaloneEnabled=false
dynamicConfigFile=/etc/zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }}/conf/zoo.cfg.dynamic
```

Java版本:

    Java(TM) SE Runtime Environment (build 1.8.0_25-b17)
    Java HotSpot(TM) 64-Bit Server VM (build 25.25-b02, mixed mode)

JVM参数:

``` bash
NAME=zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }}
ZOOCFGDIR=/etc/$NAME/conf

# TODO this is really ugly
# How to find out, which jars are needed?
# seems, that log4j requires the log4j.properties file to be in the classpath
CLASSPATH="$ZOOCFGDIR:/usr/build/classes:/usr/build/lib/*.jar:/usr/share/zookeeper/zookeeper-3.5.1-metrika.jar:/usr/share/zookeeper/slf4j-log4j12-1.7.5.jar:/usr/share/zookeeper/slf4j-api-1.7.5.jar:/usr/share/zookeeper/servlet-api-2.5-20081211.jar:/usr/share/zookeeper/netty-3.7.0.Final.jar:/usr/share/zookeeper/log4j-1.2.16.jar:/usr/share/zookeeper/jline-2.11.jar:/usr/share/zookeeper/jetty-util-6.1.26.jar:/usr/share/zookeeper/jetty-6.1.26.jar:/usr/share/zookeeper/javacc.jar:/usr/share/zookeeper/jackson-mapper-asl-1.9.11.jar:/usr/share/zookeeper/jackson-core-asl-1.9.11.jar:/usr/share/zookeeper/commons-cli-1.2.jar:/usr/src/java/lib/*.jar:/usr/etc/zookeeper"

ZOOCFG="$ZOOCFGDIR/zoo.cfg"
ZOO_LOG_DIR=/var/log/$NAME
USER=zookeeper
GROUP=zookeeper
PIDDIR=/var/run/$NAME
PIDFILE=$PIDDIR/$NAME.pid
SCRIPTNAME=/etc/init.d/$NAME
JAVA=/usr/bin/java
ZOOMAIN="org.apache.zookeeper.server.quorum.QuorumPeerMain"
ZOO_LOG4J_PROP="INFO,ROLLINGFILE"
JMXLOCALONLY=false
JAVA_OPTS="-Xms{{ '{{' }} cluster.get('xms','128M') {{ '}}' }} \
    -Xmx{{ '{{' }} cluster.get('xmx','1G') {{ '}}' }} \
    -Xloggc:/var/log/$NAME/zookeeper-gc.log \
    -XX:+UseGCLogFileRotation \
    -XX:NumberOfGCLogFiles=16 \
    -XX:GCLogFileSize=16M \
    -verbose:gc \
    -XX:+PrintGCTimeStamps \
    -XX:+PrintGCDateStamps \
    -XX:+PrintGCDetails
    -XX:+PrintTenuringDistribution \
    -XX:+PrintGCApplicationStoppedTime \
    -XX:+PrintGCApplicationConcurrentTime \
    -XX:+PrintSafepointStatistics \
    -XX:+UseParNewGC \
    -XX:+UseConcMarkSweepGC \
-XX:+CMSParallelRemarkEnabled"
```

盐初始化:

    description "zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }} centralized coordination service"

    start on runlevel [2345]
    stop on runlevel [!2345]

    respawn

    limit nofile 8192 8192

    pre-start script
        [ -r "/etc/zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }}/conf/environment" ] || exit 0
        . /etc/zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }}/conf/environment
        [ -d $ZOO_LOG_DIR ] || mkdir -p $ZOO_LOG_DIR
        chown $USER:$GROUP $ZOO_LOG_DIR
    end script

    script
        . /etc/zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }}/conf/environment
        [ -r /etc/default/zookeeper ] && . /etc/default/zookeeper
        if [ -z "$JMXDISABLE" ]; then
            JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=$JMXLOCALONLY"
        fi
        exec start-stop-daemon --start -c $USER --exec $JAVA --name zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }} \
            -- -cp $CLASSPATH $JAVA_OPTS -Dzookeeper.log.dir=${ZOO_LOG_DIR} \
            -Dzookeeper.root.logger=${ZOO_LOG4J_PROP} $ZOOMAIN $ZOOCFG
    end script

[原始文章](https://clickhouse.tech/docs/en/operations/tips/) <!--hide-->
