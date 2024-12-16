---
slug: /ja/operations/tips
sidebar_position: 58
sidebar_label: 使用推奨事項
title: "使用推奨事項"
---
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_automated.md';

<SelfManaged />

## CPU スケーリングガバナー

常に `performance` スケーリングガバナーを使用してください。`on-demand` スケーリングガバナーは、常に高い需要に対してはるかに劣っています。

``` bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## CPU 制限 {#cpu-limitations}

プロセッサは過熱する可能性があります。`dmesg` を使用して、CPUのクロックレートが過熱により制限されたかどうかを確認します。この制限はデータセンターレベルで外部から設定されることもあります。負荷下で `turbostat` を使用して監視できます。

## RAM {#ram}

小規模なデータ（圧縮で約200 GBまで）の場合、データの容量と同じくらいのメモリを使用するのが最適です。大規模なデータとインタラクティブ（オンライン）クエリを処理する場合は、合理的な量のRAM（128 GB以上）を使用し、キャッシュにホットデータのサブセットを収めるべきです。サーバーごとに約50 TBのデータボリュームの場合でも、128 GBのRAMを使用すると、64 GBと比較してクエリのパフォーマンスが大幅に向上します。

オーバーコミットを無効にしないでください。`cat /proc/sys/vm/overcommit_memory` の値は0か1であるべきです。以下を実行します。

``` bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

メモリ管理におけるカーネルでの時間を監視するには、 `perf top` を使用します。永続的な大きなページを割り当てる必要はありません。

### 16GB未満のRAMを使用する場合

推奨されるRAMの量は32GB以上です。

システムが16GB未満のRAMを持っている場合、デフォルト設定がこのメモリ量に一致しないため、様々なメモリエラーが発生する可能性があります。最小で2GBほどのRAMを持つシステムでもClickHouseを使用できますが、このようなセットアップでは追加の調整が必要であり、低速のデータ取り込みしか行えません。

16GB未満のRAMでClickHouseを使用する場合、次を推奨します：

- `config.xml` 内のマークキャッシュのサイズを下げます。それは500 MBまで下げることができますが、ゼロに設定することはできません。
- クエリ処理スレッドの数を `1` まで下げます。
- `max_block_size` を `8192` に下げます。 `1024` まで下げても実用的です。
- `max_download_threads` を `1` に下げます。
- `input_format_parallel_parsing` と `output_format_parallel_formatting` を `0` に設定します。

追加の注意事項：
- メモリアロケーターによってキャッシュされたメモリをフラッシュするには、 `SYSTEM JEMALLOC PURGE` コマンドを実行できます。
- メモリが少ないマシンでS3やKafkaの統合を使用することは推奨されません。これらはバッファに多くのメモリを必要とするためです。

## ストレージサブシステム {#storage-subsystem}

予算が許すなら、SSDを使ってください。そうでない場合はHDDを使用します。SATA HDDs 7200 RPMで十分です。

ローカルハードドライブを持つ多数のサーバーを、ディスクシェルフが付いている少数のサーバーより優先してください。ただし、まれにクエリが実行されるアーカイブを保存する場合は、シェルフでも問題ありません。

## RAID {#raid}

HDDを使用する場合、RAID-10、RAID-5、RAID-6、RAID-50で結合することができます。Linuxのためには、ソフトウェアRAID（`mdadm`）がより良いです。RAID-10を作成する場合、`far` レイアウトを選択してください。予算が許すならば、RAID-10を選んでください。

LVM自体（RAIDや`mdadm`なし）は問題ありませんが、それと組み合わせてRAIDを作成することや、`mdadm`と組み合わせることは、あまり探られていない選択肢であり、間違ったチャンクサイズの選択、チャンクの整列のミス、間違ったRAIDタイプの選択、ディスクのクリーンアップを忘れるなどの間違いの可能性が増します。LVMの使用に自信があるなら、それを使うことに反対する理由はありません。

もし4つ以上のディスクを持っている場合、RAID-6（推奨）またはRAID-50を使用してください。RAID-5を使用する代わりにRAID-6やRAID-50を使用する場合、ストライプキャッシュサイズを必ず増やしてください。デフォルトの値は通常最適な選択ではありません。

``` bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

正確な数値は、デバイスの数とブロックサイズから次の式を使って計算します：`2 * num_devices * chunk_size_in_bytes / 4096`。

たいていのRAID設定に対して64KBのブロックサイズで十分です。平均的なClickHouseサーバの書き込みサイズは約1MB（1024KB）なので、推奨されるストライプサイズも1MBです。ブロックサイズは必要に応じて最適化でき、非パリティディスクの数で1MBを割った値にセットし、各書き込みが全ての利用可能な非パリティディスクに対して並列化されるようにします。ブロックサイズを小さすぎたり大きすぎたりしないようにしてください。

SSDでRAID-0を使用できます。RAIDの使用にかかわらず、データのセキュリティのために常にレプリケーションを使用してください。

HDDではmq-deadlineまたはCFQスケジューラを、SSDではnoopを選択し、‘readahead’設定を減らさないでください。HDDでは書き込みキャッシュを有効にしてください。

NVMEとSSDディスクに対して、OSで[`fstrim`](https://en.wikipedia.org/wiki/Trim_(computing))が有効であることを確認してください（通常、cronjobまたはsystemdサービスを使用して実装されています）。

## ファイルシステム {#file-system}

Ext4が最も信頼性のあるオプションです。マウントオプションで`noatime`を設定します。XFSも良好に動作します。他のほとんどのファイルシステムも問題なく動作するはずです。

ハードリンクが不足しているため、FAT-32とexFATはサポートされていません。

ClickHouseは独自に圧縮を行い、それがより良いため、圧縮されたファイルシステムを使用しないでください。暗号化されたファイルシステムを使用することは推奨されませんが、ClickHouseの組み込みの暗号化を使用することがより望ましいです。

ClickHouseはNFS上でも動作しますが、それは最良のアイデアではありません。

## Linuxカーネル {#linux-kernel}

古いLinuxカーネルを使用しないでください。

## ネットワーク {#network}

IPv6を使用している場合は、ルートキャッシュのサイズを増やしてください。Linuxカーネル3.2以前には、IPv6実装に多数の問題がありました。

可能であれば少なくとも10 GBのネットワークを使用してください。1 Gbも動作しますが、テラバイト単位のデータを持つレプリカのパッチ適用や、大量の中間データを持つ分散クエリの処理では大幅に劣ります。

## Huge Pages {#huge-pages}

古いLinuxカーネルを使用している場合は、透過的な大きなページを無効にしてください。これは、メモリアロケーターに干渉し、パフォーマンスの大幅な低下を引き起こします。新しいLinuxカーネルでは、透過的な大きなページは問題ありません。

``` bash
$ echo 'madvise' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

透過的な大きなページ設定を恒久的に変更したい場合は、`/etc/default/grub`を編集し、`GRUB_CMDLINE_LINUX_DEFAULT`オプションに`transparent_hugepage=madvise`を追加します：

```bash
$ GRUB_CMDLINE_LINUX_DEFAULT="transparent_hugepage=madvise ..."
```

その後、`sudo update-grub`コマンドを実行し、再起動して有効にします。

## ハイパーバイザー設定

OpenStackを使用している場合、`nova.conf`に次を設定します：

```
cpu_mode=host-passthrough
```

libvirtを使用している場合、XML設定に次を設定します：

```
<cpu mode='host-passthrough'/>
```

これは、ClickHouseが`cpuid`命令で正しい情報を取得できるようにするために重要です。そうしないと、ハイパーバイザーが古いCPUモデルで動作していると`Illegal instruction`クラッシュが発生する可能性があります。

## ClickHouse KeeperとZooKeeper {#zookeeper}

ClickHouseクラスタのZooKeeperの代わりにClickHouse Keeperを使用することを推奨します。 [ClickHouse Keeper](../guides/sre/keeper/index.md) のドキュメントを参照してください。

ZooKeeperを使い続けたい場合は、ZooKeeperの新しいバージョン（3.4.9以降）を使用するのが最適です。安定したLinuxディストリビューションのバージョンは古い可能性があります。

異なるZooKeeperクラスター間でデータを転送するための手動で書かれたスクリプトは決して使用しないでください。結果が逐次ノードに対して正しくありません。同じ理由で“zkcopy”ユーティリティも使用しないでください：https://github.com/ksprojects/zkcopy/issues/15

既存のZooKeeperクラスターを二つに分割したい場合は、その数を増やしてから二つの独立したクラスターとして再構成するのが正しい方法です。

テスト環境またはインジェストレートが低い環境では、ClickHouseと同じサーバーでClickHouse Keeperを実行できます。生産環境ではClickHouseとZooKeeper/Keeperのために別々のサーバーを使用するか、ClickHouseファイルとKeeperファイルを別々のディスクに配置することをお勧めします。ZooKeeper/Keeperはディスクの待ち時間に非常に敏感であり、ClickHouseが使用可能なシステムリソースの全てを活用する場合があるためです。

ZooKeeperのアンサンブルにオブザーバーを持つことができますが、ClickHouseサーバーはオブザーバーと対話しないでください。

`minSessionTimeout`設定を変更しないでください。大きな値はClickHouseの再起動の安定性に影響を与える可能性があります。

デフォルト設定では、ZooKeeperは時限爆弾です：

> ZooKeeperサーバーはデフォルト設定のままだと古いスナップショットやログのファイルを削除しないため（`autopurge`参照）、この責任は運用者にあります。

この爆弾を解除する必要があります。

以下のZooKeeper（3.5.1）設定は大規模な生産環境で使用されています：

zoo.cfg:

``` bash
# http://hadoop.apache.org/zookeeper/docs/current/zookeeperAdmin.html

# 各ティックのミリ秒数
tickTime=2000
# 初期同期フェーズにかかることができるティック数
# この値はそれほど動機付けられていない
initLimit=300
# 要求を送信して応答を受け取るまでにかかることができるティックの数
syncLimit=10

maxClientCnxns=2000

# クライアントが要求することができ、サーバーが受け入れる可能性がある最大値です。
# サーバーで高いmaxSessionTimeoutを持つことは、高いセッションタイムアウトで動作したい場合にクライアントが使用できるようにするために問題ありません。
# ただし、デフォルトではセッションタイムアウトを30秒（ClickHouseの設定でsession_timeout_msで変更可能）に要求します。
maxSessionTimeout=60000000
# スナップショットが保存されるディレクトリ。
dataDir=/opt/zookeeper/{{ '{{' }} cluster['name'] {{ '}}' }}/data
# パフォーマンス向上のためにdataLogDirを別の物理ディスクに配置します
dataLogDir=/opt/zookeeper/{{ '{{' }} cluster['name'] {{ '}}' }}/logs

autopurge.snapRetainCount=10
autopurge.purgeInterval=1

# シークを避けるために、ZooKeeperはトランザクションログファイルにプレアロケーションサイズキロバイト単位でスペースを確保します。
# デフォルトのブロックサイズは64Mです。ブロックサイズを変更する一つの理由は、スナップショットがより頻繁に行われる場合にブロックサイズを減らすことです。（snapCountも参照）
preAllocSize=131072

# クライアントは、特に多くのクライアントがいる場合、ZooKeeperが処理できる速度よりも速くリクエストを提出できます。
# メモリが足りなくなるのを防ぐために、システムのグローバル出願制限に応じて、システムでの未処理リクエストがグローバル出願制限を超えないようにクライアントを制限します。
# デフォルトの制限は1000です。
# globalOutstandingLimit=1000

# ZooKeeperはトランザクションをトランザクションログに記録します。
# snapCountトランザクションがログファイルに書き込まれるとスナップショットが始まり、新しいトランザクションログファイルが始まります。デフォルトのsnapCountは100000です。
snapCount=3000000

# このオプションが定義されている場合、リクエストはtraceFile.year.month.dayという名前のトレースファイルにログされます。
#traceFile=

# リーダーはクライアント接続を受け入れます。デフォルト値は「yes」です。リーダーマシンは更新を調整します。
# 少しの読み取りスループットの犠牲で高い更新スループットを取得するために、リーダーをクライアントを受け入れず、調整に集中するように構成できます。
leaderServes=yes

standaloneEnabled=false
dynamicConfigFile=/etc/zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }}/conf/zoo.cfg.dynamic
```

Java バージョン:

``` text
openjdk 11.0.5-shenandoah 2019-10-15
OpenJDK Runtime Environment (build 11.0.5-shenandoah+10-adhoc.heretic.src)
OpenJDK 64-Bit Server VM (build 11.0.5-shenandoah+10-adhoc.heretic.src, mixed mode)
```

JVM パラメータ:

``` bash
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

Saltの初期化:

``` text
description "zookeeper-{{ '{{' }} cluster['name'] {{ '}}' }} 中央集権的コーディネーションサービス"

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

## アンチウイルスソフトウェア {#antivirus-software}

アンチウイルスソフトウェアを使用している場合は、ClickHouseデータファイル（`/var/lib/clickhouse`）があるフォルダをスキップするように設定してください。そうしないとパフォーマンスが低下し、データインジェストやバックグラウンドマージ中に予期しないエラーが発生する可能性があります。

## 関連コンテンツ

- [ClickHouseの始め方？13の「致命的な過ち」とその回避方法](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse)
