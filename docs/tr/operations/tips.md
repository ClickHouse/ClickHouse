---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "Kullan\u0131m \xD6nerileri"
---

# Kullanım Önerileri {#usage-recommendations}

## CPU Ölçekleme Vali {#cpu-scaling-governor}

Her zaman kullanın `performance` Ölçekleme Valisi. Bu `on-demand` ölçeklendirme Valisi sürekli yüksek talep ile çok daha kötü çalışır.

``` bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## CPU sınırlamaları {#cpu-limitations}

İşlemciler aşırı ısınabilir. Kullanmak `dmesg` aşırı ısınma nedeniyle CPU'nun saat hızının sınırlı olup olmadığını görmek için.
Kısıtlama, veri merkezi düzeyinde harici olarak da ayarlanabilir. Kullanabilirsiniz `turbostat` bir yük altında izlemek için.

## RAM {#ram}

Küçük miktarlarda veri için (~200 GB'a kadar sıkıştırılmış), veri hacmi kadar bellek kullanmak en iyisidir.
Büyük miktarda veri için ve etkileşimli (çevrimiçi) sorguları işlerken, sıcak veri alt kümesi sayfaların önbelleğine sığacak şekilde makul miktarda RAM (128 GB veya daha fazla) kullanmalısınız.
Sunucu başına ~50 TB veri hacimleri için bile, 128 GB RAM kullanmak, 64 GB'ye kıyasla sorgu performansını önemli ölçüde artırır.

Overcommit devre dışı bırakmayın. Değer `cat /proc/sys/vm/overcommit_memory` 0 veya 1 olmalıdır. Koşmak

``` bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

## Büyük Sayfalar {#huge-pages}

Her zaman şeffaf büyük sayfaları devre dışı bırakın. Önemli performans düşmesine neden olan bellek yöneticileri, engel oluyor.

``` bash
$ echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

Kullanmak `perf top` bellek yönetimi için çekirdekte harcanan zamanı izlemek için.
Kalıcı büyük sayfaların da tahsis edilmesine gerek yoktur.

## Depolama Alt Sistemi {#storage-subsystem}

Bütçeniz SSD kullanmanıza izin veriyorsa, SSD kullanın.
Eğer değilse, sabit disk kullanın. SATA HDD'ler 7200 RPM yapacak.

Bağlı disk raflarına sahip daha az sayıda sunucu üzerinde yerel sabit disklere sahip birçok sunucuyu tercih edin.
Ancak nadir sorguları olan arşivleri saklamak için raflar çalışacaktır.

## RAID {#raid}

HDD kullanırken, RAID-10, RAID-5, RAID-6 veya RAID-50'yi birleştirebilirsiniz.
Linux için, yazılım RAID daha iyidir (ile `mdadm`). LVM'Yİ kullanmanızı önermiyoruz.
RAID-10 oluştururken, `far` düzen.
Bütçeniz izin veriyorsa, RAID-10'u seçin.

4'ten fazla diskiniz varsa, RAID-5 yerine RAID-6 (tercih edilen) veya RAID-50 kullanın.
RAID-5, RAID-6 veya RAID-50 kullanırken, varsayılan değer genellikle en iyi seçenek olmadığından daima stripe\_cache\_size değerini artırın.

``` bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

Formülü kullanarak cihaz sayısından ve blok boyutundan tam sayıyı hesaplayın: `2 * num_devices * chunk_size_in_bytes / 4096`.

Tüm RAID yapılandırmaları için 1024 KB blok boyutu yeterlidir.
Blok boyutunu asla çok küçük veya çok büyük ayarlamayın.

SSD'DE RAID-0 kullanabilirsiniz.
RAID kullanımı ne olursa olsun, her zaman veri güvenliği için çoğaltma kullanın.

Uzun bir kuyruk ile NCQ etkinleştirin. HDD için CFQ zamanlayıcısını seçin ve SSD için noop'u seçin. Azalt themayın ‘readahead’ ayar.
HDD için yazma önbelleğini etkinleştirin.

## Dosya Sistemi {#file-system}

Ext4 en güvenilir seçenektir. Bağlama seçeneklerini ayarlama `noatime, nobarrier`.
XFS de uygundur, ancak ClickHouse ile iyice test edilmemiştir.
Diğer çoğu dosya sistemi de iyi çalışmalıdır. Gecikmeli tahsisli dosya sistemleri daha iyi çalışır.

## Linux Çekirdeği {#linux-kernel}

Eski bir Linux çekirdeği kullanmayın.

## Ağ {#network}

IPv6 kullanıyorsanız, rota önbelleğinin boyutunu artırın.
3.2 öncesinde Linux çekirdeği IPv6 uygulaması ile ilgili sorunlar çok sayıda vardı.

Mümkünse en az 10 GB ağ kullanın. 1 Gb de çalışacak, ancak onlarca terabayt veri içeren kopyaları yamalamak veya büyük miktarda Ara veriyle dağıtılmış sorguları işlemek için çok daha kötü olacaktır.

## ZooKeeper {#zookeeper}

Muhtemelen zaten başka amaçlar için ZooKeeper kullanıyor. Zaten aşırı değilse, aynı ZooKeeper kurulumunu kullanabilirsiniz.

It's best to use a fresh version of ZooKeeper – 3.4.9 or later. The version in stable Linux distributions may be outdated.

Sonuç sıralı düğümler için yanlış olacağından, farklı ZooKeeper kümeleri arasında veri aktarmak için el ile yazılmış komut dosyalarını asla kullanmamalısınız. Asla kullanmayın “zkcopy” aynı nedenle yardımcı program: https://github.com/ksprojects/zkcopy/issues/15

Varolan bir ZooKeeper kümesini ikiye bölmek istiyorsanız, doğru yol, yinelemelerinin sayısını artırmak ve sonra iki bağımsız küme olarak yeniden yapılandırmaktır.

Zookeeper ClickHouse aynı sunucularda çalıştırmayın. ZooKeeper gecikme için çok hassas olduğundan ve ClickHouse mevcut tüm sistem kaynaklarını kullanabilir.

Varsayılan ayarlarla, ZooKeeper bir saatli bomba:

> ZooKeeper sunucusu, varsayılan yapılandırmayı kullanırken eski anlık görüntülerden ve günlüklerden dosyaları silmez (bkz.autopurge) ve bu operatörün sorumluluğundadır.

Bu bomba etkisiz hale getirilmeli.

Aşağıdaki ZooKeeper (3.5.1) yapılandırması Yandex'te kullanılmaktadır.20 Mayıs 2017 tarihi itibariyle Metrica üretim ortamı:

hayvanat bahçesi.cfg:

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

Java sürümü:

``` text
Java(TM) SE Runtime Environment (build 1.8.0_25-b17)
Java HotSpot(TM) 64-Bit Server VM (build 25.25-b02, mixed mode)
```

JVM parametreleri:

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

Tuz ve Karab saltiber:

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

{## [Orijinal makale](https://clickhouse.tech/docs/en/operations/tips/) ##}
