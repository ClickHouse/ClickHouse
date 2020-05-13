---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 58
toc_title: "\u4F7F\u7528\u306E\u63A8\u5968\u4E8B\u9805"
---

# 使用の推奨事項 {#usage-recommendations}

## CPUスケールガバナー {#cpu-scaling-governor}

常に使用する `performance` スケーリング知事。 その `on-demand` スケーリング知事は、常に高い需要とはるかに悪い作品。

``` bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## CPUの制限 {#cpu-limitations}

プロセッサでの過熱を防止します。 使用 `dmesg` 過熱によりCPUのクロックレートが制限されているかどうかを確認します。
の制限を設定することもできます外部のデータセンターです。 を使用することができ `turbostat` 負荷の下でそれを監視する。

## RAM {#ram}

少量のデータ（最大-200gb圧縮）の場合は、データ量と同じくらいのメモリを使用するのが最善です。
大量のデータと対話型（オンライン）クエリを処理する場合は、ホットデータサブセットがページのキャッシュに収まるように、妥当な量のram（128gb以上）を使
でもデータ量の50tbサーバ用のもの128gb ramを大幅に向上するクエリの性能に比べて64gbにサンプルがあります。

Overcommitを無効にしないでください。 を値 `cat /proc/sys/vm/overcommit_memory` 0または1である必要があります。 走れ。

``` bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

## ヒュージページ {#huge-pages}

常に透明な巨大ページを無効にします。 これはメモリアロケータに干渉し、パフォーマンスが大幅に低下します。

``` bash
$ echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

使用 `perf top` メモリ管理のためにカーネルに費やされた時間を監視する。
永続的なヒュージページも割り当てる必要はありません。

## 格納サブシステム {#storage-subsystem}

予算でssdを使用できる場合は、ssdを使用してください。
そうでない場合は、hddを使用します。 sata hdd7200rpmが実行されます。

優先のサーバー地域のハードディスク上に小さな複数のサーバーが付属ディスクが増す。
ものの保存アーカイブでクエリー、棚します。

## RAID {#raid}

HDDを使用する場合は、RAID-10、RAID-5、RAID-6またはRAID-50を組み合わせることができます。
Linuxでは、ソフトウェアRAIDが優れている（と `mdadm`). LVMの使用はお勧めしません。
RAID-10を作成するときは、以下を選択します。 `far` レイアウト。
予算が許せば、raid-10を選択します。

4台以上のディスクがある場合は、raid-6(優先)またはraid-50を使用します(raid-5の代わりに使用します)。
RAID-5、RAID-6またはRAID-50を使用する場合、デフォルト値は通常最良の選択ではないので、常にstripe\_cache\_sizeを増加させます。

``` bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

次の式を使用して、デバイスの数とブロックサイズから正確な数を計算します: `2 * num_devices * chunk_size_in_bytes / 4096`.

1024kbのブロックサイズは、すべてのraid構成で十分です。
ないセットのブロックサイズでは多すぎます。

SSDにRAID-0を使用できます。
に関わらずraidの利用、使用複製のためのデータです。

長いキューでncqを有効にします。 hddの場合はcfqスケジューラを選択し、ssdの場合はnoopを選択します。 減らしてはいけない ‘readahead’ 設定。
のためのハードディスク（hdd）を、書き込みます。

## ファイル {#file-system}

Ext4は最も信頼性の高いオプションです。 マウントオプションの設定 `noatime, nobarrier`.
XFSも適していますが、ClickHouseで徹底的にテストされていません。
他のほとんどのファイルシステム仕様。 ファイルシステムの遅配ます。

## Linuxカーネル {#linux-kernel}

古いlinuxカーネルを使用しないでください。

## ネットワーク {#network}

IPv6を使用している場合は、ルートキャッシュのサイズを大きくします。
3.2より前のlinuxカーネルでは、ipv6の実装に多くの問題がありました。

可能な場合は、少なくとも10gbのネットワークを使用します。 1gbも動作しますが、数十テラバイトのデータを含むレプリカにパッチを適用したり、大量の中間データを含む分散クエリを処理する場合は、さらに悪

## ZooKeeper {#zookeeper}

おそらく既にzookeeperを他の目的で使用しているでしょう。 それがまだ過負荷になっていない場合は、zookeeperと同じインストールを使用できます。

It’s best to use a fresh version of ZooKeeper – 3.4.9 or later. The version in stable Linux distributions may be outdated.

異なるzookeeperクラスタ間でデータを転送するために手動で記述されたスクリプトを使用することはありません。 決して使用 “zkcopy” 同じ理由でユーティリティ：https://github.com/ksprojects/zkcopy/issues/15

既存のzookeeperクラスターを二つに分割したい場合、正しい方法はレプリカの数を増やし、それを二つの独立したクラスターとして再構成することです。

ClickHouseと同じサーバーでZooKeeperを実行しないでください。 で飼育係が非常に敏感なために時間遅れとClickHouseを利用することも可能で利用可能なすべてシステム資源です。

デフォルトの設定では、zookeeperは時限爆弾です:

> ZooKeeperサーバーは、デフォルト設定（autopurgeを参照）を使用するときに古いスナップショットやログからファイルを削除することはありません。

この爆弾は取り除かれなければならない

以下のzookeeper（3.5.1）設定はyandexで使用されています。月のメトリカの生産環境20,2017:

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
