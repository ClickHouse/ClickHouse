---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "\u4F7F\u7528\u6CD5\u306E\u63A8\u5968\u4E8B\u9805"
---

# 使用法の推奨事項 {#usage-recommendations}

## CPUのスケール知事 {#cpu-scaling-governor}

常に `performance` スケーリング知事。 その `on-demand` スケーリング総裁の作品もないと常に高いです。

``` bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## CPUの制限 {#cpu-limitations}

プロセッサでの過熱を防止します。 使用 `dmesg` cpuのクロックレートが過熱により制限されているかどうかを確認します。
制限は、データセンターレベルで外部で設定することもできます。 以下を使用できます `turbostat` 負荷の下でそれを監視する。

## RAM {#ram}

少量のデータ（最大200GB圧縮）の場合は、データ量と同じくらいのメモリを使用することをお勧めします。
大量のデータの場合、および対話型(オンライン)クエリを処理する場合は、ホットデータサブセットがページのキャッシュに収まるように、妥当な量のRAM(128GB
でもデータ量の50TBサーバ用のもの128GB RAMを大幅に向上するクエリの性能に比べて64GBにサンプルがあります。

オーバーコミットを無効にしません。 値 `cat /proc/sys/vm/overcommit_memory` 0または1にする必要があります。 走れ。

``` bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

## 巨大なページ {#huge-pages}

常に透明な巨大ページを無効にします。 これはメモリアロケータと干渉し、パフォーマンスが大幅に低下します。

``` bash
$ echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

使用 `perf top` メモリ管理のためにカーネルで費やされた時間を監視する。
永続的な巨大なページも割り当てる必要はありません。

## Storageサブシステム {#storage-subsystem}

予算でSSDを使用できる場合は、SSDを使用してください。
そうでない場合は、HDDを使用します。 SATA Hdd7200RPMが行います。

優先のサーバー地域のハードディスク上に小さな複数のサーバーが付属ディスクが増す。
ものの保存アーカイブでクエリー、棚します。

## RAID {#raid}

HDDを使用する場合は、RAID-10、RAID-5、RAID-6またはRAID-50を組み合わせることができます。
Linuxでは、ソフトウェアRAIDが優れています（ `mdadm`). LVMの使用はお勧めしません。
RAID-10を作成するときは、 `far` レイアウト。
予算が許せば、RAID-10を選択します。

4つ以上のディスクがある場合は、RAID-6(優先)またはRAID-50を使用してください。
RAID-5、RAID-6、またはRAID-50を使用する場合は、常にstripe_cache_sizeを増やしてください。

``` bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

式を使用して、デバイスの数とブロックサイズから正確な数を計算します: `2 * num_devices * chunk_size_in_bytes / 4096`.

すべてのRAID構成では、1024KBのブロックサイズで十分です。
ないセットのブロックサイズでは多すぎます。

SSDではRAID-0を使用できます。
に関わらずRAIDの利用、使用複製のためのデータです。

長いキューでNCQを有効にします。 HDDの場合はCFQスケジューラを選択し、SSDの場合はnoopを選択します。 減らさないで下さい ‘readahead’ 設定。
HDDの場合、ライトキャッシュを有効にします。

## ファイルシス {#file-system}

Ext4は最も信頼性の高いオプションです。 マウントオプションの設定 `noatime, nobarrier`.
XFSも適していますが、ClickHouseで徹底的にテストされていません。
他のほとんどのファイルシステム仕様。 ファイルシステムの遅配ます。

## Linuxカーネル {#linux-kernel}

古いLinuxカーネルを使用しないでください。

## ネット {#network}

IPv6を使用している場合は、ルートキャッシュのサイズを増やします。
3.2より前のLinuxカーネルには、IPv6実装に関して多くの問題がありました。

可能であれば、10GB以上のネットワークを使用してください。 1Gbも動作しますが、数十テラバイトのデータを使用してレプリカにパッチを適用する場合や、大量の中間データを使用して分散クエリを処理する場合

## 飼育係 {#zookeeper}

おそらく既に他の目的のためにZooKeeperを使用しています。 まだ過負荷になっていない場合は、ZooKeeperと同じインストールを使用できます。

It's best to use a fresh version of ZooKeeper – 3.4.9 or later. The version in stable Linux distributions may be outdated.

異なるZooKeeperクラスター間でデータを転送するには、手動で書かれたスクリプトを使用しないでください。 決して使用しない “zkcopy” 同じ理由でユーティリティ：https://github.com/ksprojects/zkcopy/issues/15

既存のZooKeeperクラスタを二つに分割する場合、正しい方法は、そのレプリカの数を増やし、それを二つの独立したクラスタとして再構成することです。

ClickHouseと同じサーバー上でZooKeeperを実行しないでください。 で飼育係が非常に敏感なために時間遅れとClickHouseを利用することも可能で利用可能なすべてシステム資源です。

デフォルト設定では、飼育係は時限爆弾です:

> ZooKeeperサーバーは、デフォルト設定を使用するときに古いスナップショットとログからファイルを削除しません(autopurgeを参照)。

この爆弾は取り除かなければならない

以下の飼育係（3.5.1）設定はYandexで使用されています。月のようMetricaの生産環境20,2017:

動物園cfg:

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

Javaバージョン:

``` text
Java(TM) SE Runtime Environment (build 1.8.0_25-b17)
Java HotSpot(TM) 64-Bit Server VM (build 25.25-b02, mixed mode)
```

JVMパラメータ:

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

塩init:

``` text
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
```

{## [元の記事](https://clickhouse.tech/docs/en/operations/tips/) ##}
