# 使用建议 {#usage-recommendations}

## CPU频率调节器 {#cpu-scaling-governor}

始终使用 `performance` 频率调节器。  `on-demand` 频率调节器在持续高需求的情况下，效果更差。

``` bash
echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## CPU限制 {#cpu-limitations}

处理器可能会过热。 使用 `dmesg` 查看CPU的时钟速率是否由于过热而受到限制。
该限制也可以在数据中心级别外部设置。 您可以使用 `turbostat` 在负载下对其进行监控。

## RAM {#ram}

对于少量数据（压缩后约200GB），最好使用与数据量一样多的内存。
对于大量数据，以及在处理交互式（在线）查询时，应使用合理数量的RAM（128GB或更多），以便热数据子集适合页面缓存。
即使对于每台服务器约50TB的数据量，与64GB相比，使用128GB的RAM也可以显着提高查询性能。

不要禁用 overcommit。`cat /proc/sys/vm/overcommit_memory` 的值应该为0或1。运行

``` bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

## 大页(Huge Pages) {#huge-pages}

始终禁用透明大页(transparent huge pages)。 它会干扰内存分配器，从而导致显着的性能下降。

``` bash
echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

使用 `perf top` 来查看内核在内存管理上花费的时间。
永久大页(permanent huge pages)也不需要被分配。

## 存储子系统 {#storage-subsystem}

如果您的预算允许您使用SSD，请使用SSD。
如果没有，请使用硬盘。 SATA硬盘7200转就行了。

优先选择许多带有本地硬盘驱动器的服务器，而不是少量带有附加磁盘架的服务器。
但是对于存储极少查询的档案，架子可以使用。

## RAID {#raid}

当使用硬盘，你可以结合他们的RAID-10，RAID-5，RAID-6或RAID-50。
对于Linux，软件RAID更好（使用 `mdadm`). 我们不建议使用LVM。
当创建RAID-10，选择 `far` 布局。
如果您的预算允许，请选择RAID-10。

如果您有4个以上的磁盘，请使用RAID-6（首选）或RAID-50，而不是RAID-5。
当使用RAID-5、RAID-6或RAID-50时，始终增加stripe_cache_size，因为默认值通常不是最佳选择。

``` bash
echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

使用以下公式从设备数量和块大小中计算出确切的数量: `2 * num_devices * chunk_size_in_bytes / 4096`。

1024KB的块大小足以满足所有RAID配置。
切勿将块大小设置得太小或太大。

您可以在SSD上使用RAID-0。
无论使用哪种RAID，始终使用复制来保证数据安全。

启用有长队列的NCQ。 对于HDD，选择CFQ调度程序，对于SSD，选择noop。 不要减少 ‘readahead’ 设置。
对于HDD，启用写入缓存。

## 文件系统 {#file-system}

Ext4是最可靠的选择。 设置挂载选项 `noatime`.
XFS也是合适的，但它还没有经过ClickHouse的全面测试。
大多数其他文件系统也应该可以正常工作。 具有延迟分配的文件系统工作得更好。

## Linux内核 {#linux-kernel}

不要使用过时的Linux内核。

## 网络 {#network}

如果使用的是IPv6，请增加路由缓存的大小。
3.2之前的Linux内核在IPv6实现方面存在许多问题。

如果可能的话，至少使用10GB的网络。1GB也可以工作，但对于使用数十TB的数据修补副本或处理具有大量中间数据的分布式查询，情况会更糟。

## 虚拟机监视器(Hypervisor)配置

如果您使用的是OpenStack，请在nova.conf中设置
```
cpu_mode=host-passthrough
```
。

如果您使用的是libvirt，请在XML配置中设置
```
<cpu mode='host-passthrough'/>
```
。

这对于ClickHouse能够通过 `cpuid` 指令获取正确的信息非常重要。
否则，当在旧的CPU型号上运行虚拟机监视器时，可能会导致 `Illegal instruction` 崩溃。

## Zookeeper {#zookeeper}

您可能已经将ZooKeeper用于其他目的。 如果它还没有超载，您可以使用相同的zookeeper。

最好使用新版本的Zookeeper – 3.4.9 或更高的版本. 稳定的Liunx发行版中的Zookeeper版本可能已过时。

你永远不要使用手动编写的脚本在不同的Zookeeper集群之间传输数据, 这可能会导致序列节点的数据不正确。出于相同的原因，永远不要使用 zkcopy 工具: https://github.com/ksprojects/zkcopy/issues/15

如果要将现有的ZooKeeper集群分为两个，正确的方法是增加其副本的数量，然后将其重新配置为两个独立的集群。

不要在ClickHouse所在的服务器上运行ZooKeeper。 因为ZooKeeper对延迟非常敏感，而ClickHouse可能会占用所有可用的系统资源。

默认设置下，ZooKeeper 就像是一个定时炸弹:

当使用默认配置时，ZooKeeper服务器不会从旧的快照和日志中删除文件（请参阅autopurge），这是操作员的责任。

必须拆除炸弹。

下面的ZooKeeper（3.5.1）配置在 Yandex.Metrica 的生产环境中使用截至2017年5月20日:

zoo.cfg:

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

初始化:

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

[原始文章](https://clickhouse.com/docs/en/operations/tips/) <!--hide-->
