---
description: 'オープンソース版 ClickHouse の利用に関する推奨事項を説明するページ'
sidebar_label: 'OSS の利用に関する推奨事項'
sidebar_position: 58
slug: /operations/tips
title: 'OSS の利用に関する推奨事項'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

<div id="cpu-scaling-governor">
  ## CPU スケーリングガバナー
</div>

常に `performance` スケーリングガバナーを使用してください。負荷が継続的に高い環境では、`on-demand` スケーリングガバナーは大幅に性能が劣ります。

```bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

<div id="cpu-limitations">
  ## CPU の制限
</div>

CPU は過熱することがあります。過熱によって CPU のクロック周波数が制限されたかどうかは、`dmesg` で確認できます。
この制限は、データセンターレベルで外部から設定されることもあります。負荷をかけた状態で監視するには、`turbostat` を使用できます。

<div id="ram">
  ## RAM
</div>

少量のデータ (圧縮後で最大約 200 GB) の場合は、データ量と同程度のメモリを搭載するのが最適です。
大量のデータを扱い、対話型 (オンライン) のクエリを処理する場合は、頻繁にアクセスされるデータの一部がページcacheに収まるよう、適切な量の RAM (128 GB 以上) を搭載する必要があります。
サーバーあたり約 50 TB のデータ量であっても、128 GB の RAM を使用すると、64 GB と比べてクエリパフォーマンスが大幅に向上します。

overcommit は無効にしないでください。`cat /proc/sys/vm/overcommit_memory` の値は 0 または 1 にする必要があります。次を実行します

```bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

`perf top` を使って、メモリ管理でカーネル内に費やされている時間を確認してください。
永続ヒュージページも割り当てる必要はありません。

<div id="using-less-than-16gb-of-ram">
  ### 16GB未満のRAMを使用する場合
</div>

推奨されるRAM容量は32 GB以上です。

システムのRAMが16 GB未満の場合、デフォルト設定がそのメモリ容量に合っていないため、さまざまなメモリ関連の例外が発生することがあります。RAMの少ないシステム (最小で2 GB程度) でもClickHouseは使用できますが、そのような構成では追加のチューニングが必要で、低いレートでしか取り込めません。

RAMが16GB未満の環境でClickHouseを使用する場合は、以下を推奨します。

* `config.xml`内のmark cacheのサイズを小さくします。500 MBまで下げられますが、0には設定できません。
* クエリ処理スレッド数を`1`まで減らします。
* `max_block_size`を`8192`に下げます。`1024`程度の値でも実用的です。
* `max_download_threads`を`1`に下げます。
* `input_format_parallel_parsing`と`output_format_parallel_formatting`を`0`に設定します。
* ログテーブルへの書き込みを無効にします。これにより、バックグラウンドのマージタスクがログテーブルのマージ実行用にRAMを確保し続けるのを防げます。`asynchronous_metric_log`、`metric_log`、`text_log`、`trace_log`を無効にします。

補足:

* メモリアロケータによってキャッシュされたメモリを解放するには、`SYSTEM JEMALLOC PURGE`
  コマンドを実行できます。
* 低メモリのマシンでは、バッファ用に大量のメモリを必要とするため、S3やKafkaのインテグレーションの使用は推奨しません。

<div id="storage-subsystem">
  ## ストレージサブシステム
</div>

予算的にSSDを使えるのであれば、SSDを使用してください。
難しい場合は、HDDを使用してください。7200 RPMのSATA HDDで十分です。

接続型のディスクシェルフを備えた少数のサーバーよりも、ローカルハードドライブを搭載した多数のサーバーを優先してください。
ただし、まれにしかクエリされないアーカイブを保存するのであれば、ディスクシェルフでも問題ありません。

<div id="raid">
  ## RAID
</div>

HDD を使用する場合は、RAID-10、RAID-5、RAID-6、または RAID-50 を構成できます。
Linux では、ソフトウェア RAID (`mdadm` の使用) が適しています。
RAID-10 を作成する場合は、`far` レイアウトを選択してください。
予算に余裕があるなら、RAID-10 を選んでください。

LVM 単体 (RAID や `mdadm` なし) でも問題ありませんが、LVM で RAID を構成したり `mdadm` と組み合わせたりする方法は、あまり検証されていないため、ミスが発生しやすくなります
 (誤った chunk サイズの選択、chunk の不整合、不適切な RAID タイプの選択、ディスクのクリーンアップ忘れなど) 。LVM の利用に自信があるのであれば、
使用しても問題ありません。

ディスクが 4 台を超える場合は、RAID-5 ではなく、RAID-6 (推奨) または RAID-50 を使用してください。
RAID-5、RAID-6、または RAID-50 を使用する場合は、デフォルト値が通常は最適ではないため、常に stripe&#95;cache&#95;size を増やしてください。

```bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

次の式を使用して、デバイス数とブロックサイズから正確な値を計算します: `2 * num_devices * chunk_size_in_bytes / 4096`.

64 KB のブロックサイズは、ほとんどの RAID 構成で十分です。clickhouse-server の平均書き込みサイズは約 1 MB (1024 KB) であるため、推奨されるストライプサイズも 1 MB です。必要に応じて、RAID アレイ内の非パリティディスク数で 1 MB を割った値に設定することで、ブロックサイズを最適化できます。これにより、各書き込みが利用可能なすべての非パリティディスクにまたがって並列化されます。
ブロックサイズを小さすぎたり大きすぎたりしないでください。

SSD では RAID-0 を使用できます。
RAID の使用有無にかかわらず、データ保護のために常にレプリケーションを使用してください。

キュー長を長く設定して NCQ を有効にしてください。HDD では mq-deadline または CFQ スケジューラを選択し、SSD では noop を選択してください。&#39;readahead&#39; 設定は減らさないでください。
HDD では書き込みキャッシュを有効にしてください。

OS で NVME および SSD ディスクに対して [`fstrim`](https://en.wikipedia.org/wiki/Trim_\(computing\)) が有効になっていることを確認してください (通常は cron ジョブまたは systemd サービスで実装されます) 。

<div id="file-system">
  ## ファイルシステム
</div>

Ext4 が最も信頼性の高い選択肢です。マウントオプション `noatime` を設定してください。XFS も問題なく使用できます。
そのほかのほとんどのファイルシステムも、通常は問題なく使用できます。

FAT-32 と exFAT は、ハードリンクに対応していないためサポートされていません。

圧縮ファイルシステムは使用しないでください。ClickHouse 自体がより適切に圧縮を行えるためです。
暗号化ファイルシステムの使用は推奨されません。ClickHouse には組み込みの暗号化機能があり、そちらのほうが優れているためです。

ClickHouse は NFS 上でも動作しますが、最適な選択肢ではありません。

<div id="linux-kernel">
  ## Linux カーネル
</div>

古くなった Linux カーネルは使用しないでください。

<div id="network">
  ## ネットワーク
</div>

IPv6 を使用している場合は、ルート cache のサイズを増やしてください。
3.2 より前の Linux カーネルには、IPv6 の実装に関して多くの問題がありました。

可能であれば、少なくとも 10 GB のネットワークを使用してください。1 Gb でも動作はしますが、数十テラバイトのデータを持つレプリカへのパッチ適用や、大量の中間データを伴う分散クエリの処理では、かなり不利になります。

<div id="huge-pages">
  ## Huge Pages
</div>

transparent huge pages は常に `madvise` に設定してください。古いカーネル (5.9 より前) では、THP が `always` に設定されていると、パフォーマンスが大幅に低下することがあります。これは、特に RAM が 64 GB 以上のシステムで、カーネルがメモリのデフラグメンテーションに過剰な時間を費やすためです。カーネル 5.9 では proactive compaction が導入され、THP をより適切に処理できるようになりましたが、THP が `always` に設定されている場合、ClickHouse は起動時に引き続き警告を表示します。そのため、カーネルのバージョンにかかわらず、推奨される設定は `madvise` です。

```bash
$ echo 'madvise' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

transparent huge pages の設定を永続的に変更するには、`/etc/default/grub` を編集して、`GRUB_CMDLINE_LINUX_DEFAULT` オプションに `transparent_hugepage=madvise` を追加します:

```bash
$ GRUB_CMDLINE_LINUX_DEFAULT="transparent_hugepage=madvise ..."
```

その後、`sudo update-grub` コマンドを実行してから、反映のために再起動します。

<div id="hypervisor-configuration">
  ## ハイパーバイザーの設定
</div>

OpenStack を使用している場合は、次を設定します

```ini
cpu_mode=host-passthrough
```

`nova.conf` で設定します。

libvirt を使用している場合は、次を設定します

```xml
<cpu mode='host-passthrough'/>
```

XML 設定では。

これは、ClickHouse が `cpuid` 命令から正しい情報を取得できるようにするうえで重要です。
そうしないと、古い CPU モデル上でハイパーバイザーを実行している場合に、`Illegal instruction` によるクラッシュが発生する可能性があります。

<div id="zookeeper">
  ## ClickHouse Keeper と ZooKeeper
</div>

ClickHouse クラスターでは、ZooKeeper の代わりに ClickHouse Keeper を使用することを推奨します。[ClickHouse Keeper](../guides/sre/keeper/index.md) のドキュメントを参照してください。

ZooKeeper を引き続き使用する場合は、ZooKeeper 3.4.9 以降の新しいバージョンを使うのが最適です。安定版の Linux ディストリビューションに含まれるバージョンは古い可能性があります。

異なる ZooKeeper クラスター間でデータを転送するために、手作業で作成したスクリプトを使ってはいけません。sequential ノードでは結果が正しくならないためです。同じ理由で、&quot;zkcopy&quot; ユーティリティも絶対に使用しないでください: https://github.com/ksprojects/zkcopy/issues/15

既存の ZooKeeper クラスターを 2 つに分割したい場合、正しい方法は、まずレプリカ数を増やしてから、2 つの独立したクラスターとして再構成することです。

テスト環境や、インジェスト率が低い環境では、ClickHouse Keeper を ClickHouse と同じサーバー上で実行できます。
production 環境では、ClickHouse と ZooKeeper/Keeper には別々のサーバーを使用するか、ClickHouse のファイルと Keeper のファイルを別々のディスクに配置することを推奨します。ZooKeeper/Keeper はディスクレイテンシに非常に敏感であり、ClickHouse は利用可能なシステムリソースをすべて使い切る可能性があるためです。

アンサンブル内に ZooKeeper オブザーバーを含めることはできますが、ClickHouse サーバーがオブザーバーとやり取りすべきではありません。

`minSessionTimeout` 設定は変更しないでください。値を大きくすると、ClickHouse の再起動の安定性に影響する可能性があります。

デフォルト設定のままでは、ZooKeeper は時限爆弾です:

> デフォルト設定を使用している場合、ZooKeeper サーバーは古い snapshots と logs のファイルを削除しません (`autopurge` を参照) 。これは operator の責任です。

この爆弾は解除しなければなりません。

以下の ZooKeeper (3.5.1) の設定は、大規模な production 環境で使用されています:

zoo.cfg:

```bash
# http://hadoop.apache.org/zookeeper/docs/current/zookeeperAdmin.html

# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
# This value is not quite motivated
initLimit=300
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=10

maxClientCnxns=2000

# It is the maximum value that client may request and the server will accept.
# It is Ok to have high maxSessionTimeout on server to allow clients to work with high session timeout if they want.
# But we request session timeout of 30 seconds by default (you can change it with session_timeout_ms in ClickHouse config).
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
# system. The default limit is 1000.
# globalOutstandingLimit=1000

# ZooKeeper logs transactions to a transaction log. After snapCount transactions
# are written to a log file a snapshot is started and a new transaction log file
# is started. The default snapCount is 100000.
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

Javaのバージョン：

```text
openjdk 11.0.5-shenandoah 2019-10-15
OpenJDK Runtime Environment (build 11.0.5-shenandoah+10-adhoc.heretic.src)
OpenJDK 64-Bit Server VM (build 11.0.5-shenandoah+10-adhoc.heretic.src, mixed mode)
```

JVM パラメータ:

```bash
NAME=zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }}
ZOOCFGDIR=/etc/$NAME/conf

# TODO this is really ugly
# How to find out, which jars are needed?
# seems, that log4j requires the log4j.properties file to be in the classpath
CLASSPATH="$ZOOCFGDIR:/usr/build/classes:/usr/build/lib/*.jar:/usr/share/zookeeper-3.6.2/lib/audience-annotations-0.5.0.jar:/usr/share/zookeeper-3.6.2/lib/commons-cli-1.2.jar:/usr/share/zookeeper-3.6.2/lib/commons-lang-2.6.jar:/usr/share/zookeeper-3.6.2/lib/jackson-annotations-2.10.3.jar:/usr/share/zookeeper-3.6.2/lib/jackson-core-2.10.3.jar:/usr/share/zookeeper-3.6.2/lib/jackson-databind-2.10.3.jar:/usr/share/zookeeper-3.6.2/lib/javax.servlet-api-3.1.0.jar:/usr/share/zookeeper-3.6.2/lib/jetty-http-9.4.24.v20191120.jar:/usr/share/zookeeper-3.6.2/lib/jetty-io-9.4.24.v20191120.jar:/usr/share/zookeeper-3.6.2/lib/jetty-security-9.4.24.v20191120.jar:/usr/share/zookeeper-3.6.2/lib/jetty-server-9.4.24.v20191120.jar:/usr/share/zookeeper-3.6.2/lib/jetty-servlet-9.4.24.v20191120.jar:/usr/share/zookeeper-3.6.2/lib/jetty-util-9.4.24.v20191120.jar:/usr/share/zookeeper-3.6.2/lib/jline-2.14.6.jar:/usr/share/zookeeper-3.6.2/lib/json-simple-1.1.1.jar:/usr/share/zookeeper-3.6.2/lib/log4j-1.2.17.jar:/usr/share/zookeeper-3.6.2/lib/metrics-core-3.2.5.jar:/usr/share/zookeeper-3.6.2/lib/netty-buffer-4.1.50.Final.jar:/usr/share/zookeeper-3.6.2/lib/netty-codec-4.1.50.Final.jar:/usr/share/zookeeper-3.6.2/lib/netty-common-4.1.50.Final.jar:/usr/share/zookeeper-3.6.2/lib/netty-handler-4.1.50.Final.jar:/usr/share/zookeeper-3.6.2/lib/netty-resolver-4.1.50.Final.jar:/usr/share/zookeeper-3.6.2/lib/netty-transport-4.1.50.Final.jar:/usr/share/zookeeper-3.6.2/lib/netty-transport-native-epoll-4.1.50.Final.jar:/usr/share/zookeeper-3.6.2/lib/netty-transport-native-unix-common-4.1.50.Final.jar:/usr/share/zookeeper-3.6.2/lib/simpleclient-0.6.0.jar:/usr/share/zookeeper-3.6.2/lib/simpleclient_common-0.6.0.jar:/usr/share/zookeeper-3.6.2/lib/simpleclient_hotspot-0.6.0.jar:/usr/share/zookeeper-3.6.2/lib/simpleclient_servlet-0.6.0.jar:/usr/share/zookeeper-3.6.2/lib/slf4j-api-1.7.25.jar:/usr/share/zookeeper-3.6.2/lib/slf4j-log4j12-1.7.25.jar:/usr/share/zookeeper-3.6.2/lib/snappy-java-1.1.7.jar:/usr/share/zookeeper-3.6.2/lib/zookeeper-3.6.2.jar:/usr/share/zookeeper-3.6.2/lib/zookeeper-jute-3.6.2.jar:/usr/share/zookeeper-3.6.2/lib/zookeeper-prometheus-metrics-3.6.2.jar:/usr/share/zookeeper-3.6.2/etc"

ZOOCFG="$ZOOCFGDIR/zoo.cfg"
ZOO_LOG_DIR=/var/log/$NAME
USER=zookeeper
GROUP=zookeeper
PIDDIR=/var/run/$NAME
PIDFILE=$PIDDIR/$NAME.pid
SCRIPTNAME=/etc/init.d/$NAME
JAVA=/usr/local/jdk-11/bin/java
ZOOMAIN="org.apache.zookeeper.server.quorum.QuorumPeerMain"
ZOO_LOG4J_PROP="INFO,ROLLINGFILE"
JMXLOCALONLY=false
JAVA_OPTS="-Xms{{ '{{' }} cluster.get('xms','128M') {{ '}}' }} \
    -Xmx{{ '{{' }} cluster.get('xmx','1G') {{ '}}' }} \
    -Xlog:safepoint,gc*=info,age*=debug:file=/var/log/$NAME/zookeeper-gc.log:time,level,tags:filecount=16,filesize=16M
    -verbose:gc \
    -XX:+UseG1GC \
    -Djute.maxbuffer=8388608 \
    -XX:MaxGCPauseMillis=50"
```

ソルトの初期化:

```text
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

<div id="antivirus-software">
  ## ウイルス対策ソフト
</div>

ウイルス対策ソフトを使用している場合は、ClickHouse のデータファイルを含むフォルダー (`/var/lib/clickhouse`) をスキャン対象から除外するよう設定してください。そうしないと、パフォーマンスが低下したり、データのインジェストやバックグラウンドマージ中に予期しないエラーが発生したりすることがあります。

<div id="related-content">
  ## 関連コンテンツ
</div>

* [ClickHouse を使い始めるなら？ よくある13の「致命的な落とし穴」と、その回避方法をご紹介します](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse)