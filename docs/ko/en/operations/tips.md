---
description: '오픈소스 ClickHouse 사용 권장사항을 안내하는 페이지'
sidebar_label: 'OSS 사용 권장사항'
sidebar_position: 58
slug: /operations/tips
title: 'OSS 사용 권장사항'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

<div id="cpu-scaling-governor">
  ## CPU 스케일링 거버너
</div>

항상 `performance` 스케일링 거버너를 사용하세요. `on-demand` 스케일링 거버너는 부하가 지속적으로 높은 환경에서는 성능이 훨씬 떨어집니다.

```bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

<div id="cpu-limitations">
  ## CPU 제한 사항
</div>

프로세서는 과열될 수 있습니다. 과열로 인해 CPU의 클록 속도가 제한되었는지 확인하려면 `dmesg`를 사용하세요.
이 제한은 데이터센터 수준에서 별도로 설정될 수도 있습니다. 부하를 가한 상태에서 이를 모니터링하려면 `turbostat`를 사용할 수 있습니다.

<div id="ram">
  ## RAM
</div>

소량의 데이터(압축 기준 약 200 GB 이하)의 경우, 데이터 용량만큼 메모리를 최대한 사용하는 것이 가장 좋습니다.
대량의 데이터를 다루면서 대화형(온라인) 쿼리를 처리하는 경우에는 핫 데이터 부분 집합이 페이지 캐시에 들어갈 수 있도록 충분한 RAM(128 GB 이상)을 사용하는 것이 좋습니다.
서버당 약 50 TB의 데이터가 있는 경우에도 128 GB의 RAM을 사용하면 64 GB를 사용할 때보다 쿼리 성능이 크게 향상됩니다.

오버커밋을 비활성화하지 마십시오. `cat /proc/sys/vm/overcommit_memory` 값은 0 또는 1이어야 합니다. 다음을 실행하십시오

```bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

`perf top`를 사용하여 메모리 관리와 관련해 커널에서 소요되는 시간을 확인하세요.
영구 huge pages도 할당할 필요가 없습니다.

<div id="using-less-than-16gb-of-ram">
  ### 16GB 미만의 RAM 사용
</div>

권장 RAM 용량은 32 GB 이상입니다.

시스템의 RAM이 16 GB 미만이면 기본 설정이 이 메모리 용량에 맞지 않아 다양한 메모리 예외가 발생할 수 있습니다. 적은 양의 RAM(최소 2 GB)으로도 ClickHouse를 사용할 수 있지만, 이러한 구성은 추가 튜닝이 필요하며 낮은 속도로만 데이터를 수집할 수 있습니다.

16GB 미만의 RAM으로 ClickHouse를 사용할 때는 다음을 권장합니다.

* `config.xml`에서 마크 캐시의 크기를 줄이십시오. 최소 500 MB까지 설정할 수 있지만 0으로 설정할 수는 없습니다.
* 쿼리 처리 스레드 수를 `1`로 줄이십시오.
* `max_block_size`를 `8192`로 줄이십시오. `1024`처럼 낮은 값도 충분히 실용적일 수 있습니다.
* `max_download_threads`를 `1`로 줄이십시오.
* `input_format_parallel_parsing` 및 `output_format_parallel_formatting`을 `0`으로 설정하십시오.
* 로그 테이블에 대한 쓰기를 비활성화하십시오. 그렇지 않으면 로그 테이블의 머지를 수행하기 위해 백그라운드 머지 작업이 계속 RAM을 예약하게 됩니다. `asynchronous_metric_log`, `metric_log`, `text_log`, `trace_log`를 비활성화하십시오.

추가 참고 사항:

* 메모리 할당자에 캐시된 메모리를 플러시하려면 `SYSTEM JEMALLOC PURGE`
  명령을 실행할 수 있습니다.
* 버퍼에 상당한 메모리가 필요하므로 메모리가 적은 시스템에서는 S3 또는 Kafka 통합 사용을 권장하지 않습니다.

<div id="storage-subsystem">
  ## 스토리지 서브시스템
</div>

예산이 허용된다면 SSD를 사용하십시오.
그렇지 않다면 HDD를 사용하십시오. 7200 RPM SATA HDD면 충분합니다.

디스크 셸프가 연결된 소수의 서버보다 로컬 하드 드라이브가 있는 다수의 서버를 우선적으로 선택하십시오.
하지만 쿼리가 드문 아카이브를 저장하는 용도라면 디스크 셸프도 적합합니다.

<div id="raid">
  ## RAID
</div>

HDD를 사용할 경우 RAID-10, RAID-5, RAID-6 또는 RAID-50으로 구성할 수 있습니다.
Linux에서는 소프트웨어 RAID(`mdadm` 사용)가 더 적합합니다.
RAID-10을 생성할 때는 `far` 레이아웃을 선택하십시오.
예산에 여유가 있다면 RAID-10을 선택하십시오.

LVM만 단독으로 사용하는 것(RAID 또는 `mdadm` 없이)은 괜찮지만, LVM으로 RAID를 구성하거나 `mdadm`과 조합해서 사용하는 방식은 충분히 검증된 선택지는 아니며, 실수할 가능성도 더 큽니다
(잘못된 청크 크기 선택, 청크 정렬 불일치, 잘못된 RAID 유형 선택, 디스크 정리 누락). LVM 사용에 익숙하다면
사용해도 문제는 없습니다.

디스크가 4개를 초과한다면 RAID-5 대신 RAID-6(권장) 또는 RAID-50을 사용하십시오.
RAID-5, RAID-6 또는 RAID-50을 사용할 때는 기본값이 대체로 최선이 아니므로 항상 stripe&#95;cache&#95;size를 늘리십시오.

```bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

장치 수와 블록 크기를 바탕으로 정확한 값을 계산하려면 다음 공식을 사용하십시오: `2 * num_devices * chunk_size_in_bytes / 4096`.

64 KB 블록 크기는 대부분의 RAID 구성에 충분합니다. clickhouse-server의 평균 쓰기 크기는 약 1 MB(1024 KB)이므로 권장되는 stripe 크기도 1 MB입니다. 필요한 경우 블록 크기를 1 MB를 RAID 배열의 비패리티 디스크 수로 나눈 값으로 설정하면 최적화할 수 있으며, 이렇게 하면 각 쓰기가 사용 가능한 모든 비패리티 디스크에 병렬로 분산됩니다.
블록 크기를 너무 작게 또는 너무 크게 설정하지 마십시오.

SSD에서는 RAID-0를 사용할 수 있습니다.
RAID 사용 여부와 관계없이 데이터 보안을 위해 항상 복제를 사용하십시오.

긴 큐와 함께 NCQ를 활성화하십시오. HDD의 경우 mq-deadline 또는 CFQ 스케줄러를 선택하고, SSD의 경우 noop를 선택하십시오. &#39;readahead&#39; 설정은 줄이지 마십시오.
HDD의 경우 쓰기 캐시를 활성화하십시오.

OS에서 NVME 및 SSD 디스크에 대해 [`fstrim`](https://en.wikipedia.org/wiki/Trim_\(computing\))이 활성화되어 있는지 확인하십시오(보통 cronjob 또는 systemd 서비스로 구현됩니다).

<div id="file-system">
  ## 파일 시스템
</div>

Ext4가 가장 신뢰할 수 있는 선택지입니다. 마운트 옵션 `noatime`을 설정하십시오. XFS도 잘 작동합니다.
그 밖의 대부분의 파일 시스템도 대체로 문제없이 사용할 수 있습니다.

FAT-32와 exFAT는 하드 링크를 지원하지 않으므로 지원되지 않습니다.

압축된 파일 시스템은 사용하지 마십시오. ClickHouse가 자체적으로 더 뛰어난 압축을 수행하기 때문입니다.
암호화된 파일 시스템도 권장되지 않습니다. ClickHouse에서 더 나은 내장 암호화를 사용할 수 있기 때문입니다.

ClickHouse는 NFS에서도 작동할 수 있지만, 최선의 선택은 아닙니다.

<div id="linux-kernel">
  ## Linux 커널
</div>

구형 Linux 커널은 사용하지 마십시오.

<div id="network">
  ## 네트워크
</div>

IPv6를 사용하는 경우 라우트 캐시 크기를 늘리십시오.
3.2 이전 Linux 커널에는 IPv6 구현에 여러 문제가 있었습니다.

가능하면 최소 10 Gb 네트워크를 사용하십시오. 1 Gb도 사용할 수는 있지만, 수십 테라바이트 규모의 데이터를 가진 레플리카에 patch를 적용하거나 많은 양의 중간 데이터를 사용하는 분산 쿼리를 처리할 때는 성능이 훨씬 떨어집니다.

<div id="huge-pages">
  ## Huge Pages
</div>

항상 Transparent Huge Pages를 `madvise`로 설정하십시오. 오래된 커널(5.9 이전)에서는 THP가 `always`로 설정된 경우 성능이 크게 저하될 수 있습니다. 특히 RAM이 64 GB 이상인 시스템에서는 커널이 메모리 단편화 해소에 과도한 시간을 소비합니다. 커널 5.9에서는 proactive compaction이 도입되어 THP를 훨씬 더 잘 처리하지만, THP가 `always`로 설정되어 있으면 ClickHouse는 시작 시 여전히 경고를 표시하므로 커널 버전과 관계없이 `madvise`를 권장 설정으로 사용해야 합니다.

```bash
$ echo 'madvise' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

transparent huge pages 설정을 영구적으로 변경하려면 `/etc/default/grub` 파일을 편집하여 `GRUB_CMDLINE_LINUX_DEFAULT` 옵션에 `transparent_hugepage=madvise`를 추가하십시오:

```bash
$ GRUB_CMDLINE_LINUX_DEFAULT="transparent_hugepage=madvise ..."
```

그런 다음 `sudo update-grub` 명령을 실행하고, 변경 사항을 적용하려면 시스템을 재부팅하십시오.

<div id="hypervisor-configuration">
  ## 하이퍼바이저 구성
</div>

OpenStack를 사용 중이면 다음을 설정하십시오.

```ini
cpu_mode=host-passthrough
```

`nova.conf`에서.

libvirt를 사용하는 경우 다음과 같이 설정합니다

```xml
<cpu mode='host-passthrough'/>
```

XML 구성에서.

이는 ClickHouse가 `cpuid` 명령어로 올바른 정보를 가져올 수 있도록 하는 데 중요합니다.
그렇지 않으면 하이퍼바이저가 오래된 CPU 모델에서 실행될 때 `Illegal instruction` 크래시가 발생할 수 있습니다.

<div id="zookeeper">
  ## ClickHouse Keeper와 ZooKeeper
</div>

ClickHouse 클러스터에서는 ZooKeeper를 대체하기 위해 ClickHouse Keeper를 사용하는 것을 권장합니다. [ClickHouse Keeper](../guides/sre/keeper/index.md) 문서를 참조하십시오.

계속 ZooKeeper를 사용하려면 최신 버전의 ZooKeeper(3.4.9 이상)를 사용하는 것이 가장 좋습니다. 안정적인 Linux 배포판에 포함된 버전은 오래되었을 수 있습니다.

서로 다른 ZooKeeper 클러스터 간에 데이터를 전송할 때 수동으로 작성한 스크립트를 사용해서는 안 됩니다. sequential 노드에서는 결과가 올바르지 않기 때문입니다. 같은 이유로 &quot;zkcopy&quot; 유틸리티도 절대 사용하지 마십시오: https://github.com/ksprojects/zkcopy/issues/15

기존 ZooKeeper 클러스터를 둘로 나누려면, 먼저 레플리카 수를 늘린 다음 두 개의 독립적인 클러스터로 재구성하는 것이 올바른 방법입니다.

테스트 환경이나 수집 속도가 낮은 환경에서는 ClickHouse와 동일한 서버에서 ClickHouse Keeper를 실행할 수 있습니다.
프로덕션 환경에서는 ClickHouse와 ZooKeeper/Keeper에 각각 별도의 서버를 사용하거나, ClickHouse 파일과 Keeper 파일을 서로 다른 디스크에 두는 것을 권장합니다. ZooKeeper/Keeper는 디스크 지연 시간에 매우 민감하고, ClickHouse는 사용 가능한 시스템 리소스를 모두 사용할 수 있기 때문입니다.

앙상블에 ZooKeeper observer를 둘 수는 있지만, ClickHouse 서버는 observer와 상호작용해서는 안 됩니다.

`minSessionTimeout` 설정은 변경하지 마십시오. 값이 너무 크면 ClickHouse 재시작 안정성에 영향을 줄 수 있습니다.

기본 설정에서는 ZooKeeper가 시한폭탄과 같습니다:

> 기본 구성에서는 ZooKeeper 서버가 오래된 snapshots 및 logs 파일을 삭제하지 않으며(`autopurge` 참조), 이는 운영자의 책임입니다.

이 폭탄은 반드시 해체해야 합니다.

아래의 ZooKeeper (3.5.1) 구성은 대규모 프로덕션 환경에서 사용됩니다:

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

Java 버전:

```text
openjdk 11.0.5-shenandoah 2019-10-15
OpenJDK Runtime Environment (build 11.0.5-shenandoah+10-adhoc.heretic.src)
OpenJDK 64-Bit Server VM (build 11.0.5-shenandoah+10-adhoc.heretic.src, mixed mode)
```

JVM 매개변수:

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

Salt 초기화:

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
  ## 안티바이러스 소프트웨어
</div>

안티바이러스 소프트웨어를 사용하는 경우 ClickHouse 데이터 파일이 있는 폴더(`/var/lib/clickhouse`)를 검사 대상에서 제외하도록 구성하십시오. 그렇지 않으면 성능이 저하되거나 데이터 수집 및 백그라운드 머지 중 예기치 않은 오류가 발생할 수 있습니다.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* [ClickHouse를 시작하시나요? 피해야 할 13가지 &quot;치명적인 실수&quot;와 그 예방법](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse)