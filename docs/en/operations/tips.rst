Operations Tips
===============

CPU
---

SSE 4.2 instruction set support is required. Most recent (since 2008) CPUs have this instruction set.

When choosing between CPU with more cores and slightly less frequency and CPU with less cores and more frequency, choose first.
For example, 16 cores with 2600 MHz is better than 8 cores with 3600 MHz.

Hyper-Threading
---------------

Don't disable hyper-threading. Some queries will benefit from hyper-threading and some will not.


Turbo-Boost
-----------

Don't disable turbo-boost. It will do significant performance gain on typical load.
Use ``turbostat`` tool to show real CPU frequency under load.


CPU scaling governor
--------------------

Always use ``performance`` scaling governor. ``ondemand`` scaling governor performs much worse even on constantly high demand.

    .. code-block:: bash

    sudo echo 'performance' | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor


CPU throttling
--------------
Your CPU could be overheated. Use ``dmesg`` to show if it was thermal throttled.
Your CPU could be power capped in datacenter. Use ``turbostat`` tool under load to monitor that.


RAM
---

For small amount of data (up to ~200 GB compressed) prefer to use as much RAM as data volume.
For larger amount of data, if you run interactive (online) queries, use reasonable amount of RAM (128 GB or more) to hot data fit in page cache.
Even for data volumes of ~50 TB per server, using 128 GB of RAM is much better for query performance than 64 GB.


Swap
----

Disable swap. The only possible reason to not disable swap is when you are running ClickHouse on your personal laptop/desktop.


Huge pages
----------

Disable transparent huge pages. It interferes badly with memory allocators, leading to major performance degradation.

    .. code-block:: bash

    echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

Use ``perf top`` to monitor time spent in kernel on doing memory management.
Don't allocate permanent huge pages.


Storage subsystem
-----------------

If you could afford SSD, use SSD.
Otherwise use HDD. SATA HDDs 7200 RPM are Ok.

Prefer more servers with in place storage to less servers with huge disk shelves.
Of course you could use huge disk shelves for archive storage with rare queries.


RAID
----

When using HDDs, you could use RAID-10, RAID-5, RAID-6 or RAID-50.
Use Linux software RAID (``mdadm``). Better to not use LVM.
When creating RAID-10, choose ``far`` layout.
Prefer RAID-10 if you could afford it.

Don't use RAID-5 on more than 4 HDDs - use RAID-6 or RAID-50. RAID-6 is better.
When using RAID-5, RAID-6, or RAID-50, always increase stripe_cache_size, because default setting is awful.

    .. code-block:: bash

    echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size

Exact number is calculated from number of devices and chunk size: ``2 * num_devices * chunk_size_in_bytes / 4096``.

Chunk size 1024K is Ok for all RAID configurations.
Never use too small or too large chunk sizes.

On SSDs, you could use RAID-0.
Regardless to RAID, always use replication for data safety.

Enable NCQ. Use high queue depth. Use CFQ scheduler for HDDs and noop for SSDs. Don't lower readahead setting.
Enable write cache on HDDs.


Filesystem
----------

Ext4 is Ok. Mount with ``noatime,nobarrier``.
XFS is Ok too, but less tested with ClickHouse.
Most other filesystems should work fine. Filesystems with delayed allocation are better.


Linux kernel
------------

Don't use too old Linux kernel. For example, on 2015, 3.18.19 is Ok.
You could use Yandex kernel: https://github.com/yandex/smart which gives at least 5% performance increase.


Network
-------

When using IPv6, you must increase route cache.
Linux kernels before 3.2 has awful bugs in IPv6 implementation.

Prefer at least 10 Gbit network. 1 Gbit will also work, but much worse for repairing replicas with tens of terabytes of data and for processing huge distributed queries with much intermediate data.


ZooKeeper
---------

Probably you already have ZooKeeper for other purposes.
It's Ok to use existing ZooKeeper installation if it is not overloaded.

Use recent version of ZooKeeper. At least 3.4.9 is Ok. Version in your Linux package repository might be outdated.

With default settings, ZooKeeper have time bomb:

    A ZooKeeper server will not remove old snapshots and log files when using the default configuration (see autopurge below), this is the responsibility of the operator.

You need to defuse the bomb.

Below is ZooKeeper (3.5.1) configuration used by Yandex.Metrica in production as of 2017-05-20.

zoo.cfg:

.. code-block:: bash

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
    dataDir=/opt/zookeeper/{{ cluster['name'] }}/data
    # Place the dataLogDir to a separate physical disc for better performance
    dataLogDir=/opt/zookeeper/{{ cluster['name'] }}/logs

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
    dynamicConfigFile=/etc/zookeeper-{{ cluster['name'] }}/conf/zoo.cfg.dynamic
    ```

Java version:

.. code-block:: text

    Java(TM) SE Runtime Environment (build 1.8.0_25-b17)
    Java HotSpot(TM) 64-Bit Server VM (build 25.25-b02, mixed mode)

JVM parameters:

.. code-block:: bash

    NAME=zookeeper-{{ cluster['name'] }}
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
    JAVA_OPTS="-Xms{{ cluster.get('xms','128M') }} \
        -Xmx{{ cluster.get('xmx','1G') }} \
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


Инициализация через Salt:

.. code-block:: text

    description "zookeeper-{{ cluster['name'] }} centralized coordination service"

    start on runlevel [2345]
    stop on runlevel [!2345]

    respawn

    limit nofile 8192 8192

    pre-start script
        [ -r "/etc/zookeeper-{{ cluster['name'] }}/conf/environment" ] || exit 0
        . /etc/zookeeper-{{ cluster['name'] }}/conf/environment
        [ -d $ZOO_LOG_DIR ] || mkdir -p $ZOO_LOG_DIR
        chown $USER:$GROUP $ZOO_LOG_DIR
    end script

    script
        . /etc/zookeeper-{{ cluster['name'] }}/conf/environment
        [ -r /etc/default/zookeeper ] && . /etc/default/zookeeper
        if [ -z "$JMXDISABLE" ]; then
            JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=$JMXLOCALONLY"
        fi
        exec start-stop-daemon --start -c $USER --exec $JAVA --name zookeeper-{{ cluster['name'] }} \
            -- -cp $CLASSPATH $JAVA_OPTS -Dzookeeper.log.dir=${ZOO_LOG_DIR} \
            -Dzookeeper.root.logger=${ZOO_LOG4J_PROP} $ZOOMAIN $ZOOCFG
    end script
