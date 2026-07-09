---
description: 'صفحة تعرض توصيات استخدام ClickHouse مفتوح المصدر'
sidebar_label: 'توصيات استخدام OSS'
sidebar_position: 58
slug: /operations/tips
title: 'توصيات استخدام OSS'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

<div id="cpu-scaling-governor">
  ## متحكّم تحجيم CPU
</div>

استخدم دائمًا متحكّم التحجيم `performance`. أمّا متحكّم التحجيم `on-demand` فيعمل بشكل أسوأ بكثير مع الأحمال المرتفعة المستمرة.

```bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

<div id="cpu-limitations">
  ## قيود CPU
</div>

قد ترتفع حرارة المعالجات بشكل مفرط. استخدم `dmesg` لمعرفة ما إذا كان تردد CPU قد جرى تقييده بسبب ارتفاع الحرارة.
يمكن أيضًا فرض هذا التقييد خارجيًا على مستوى مركز البيانات. يمكنك استخدام `turbostat` لمراقبته تحت حمل.

<div id="ram">
  ## RAM
</div>

بالنسبة إلى كميات البيانات الصغيرة (حتى نحو 200 جيجابايت مضغوطة)، فمن الأفضل استخدام ذاكرة بحجم مماثل لحجم البيانات.
أما بالنسبة إلى كميات البيانات الكبيرة وعند معالجة الاستعلامات التفاعلية (عبر الإنترنت)، فينبغي استخدام مقدار مناسب من RAM (128 جيجابايت أو أكثر) بحيث تتسع المجموعة الفرعية الأكثر نشاطًا من البيانات في ذاكرة التخزين المؤقت للصفحات.
وحتى مع أحجام بيانات تبلغ نحو 50 تيرابايت لكل خادم، فإن استخدام 128 جيجابايت من RAM يحسّن أداء الاستعلامات بشكل ملحوظ مقارنةً بـ 64 جيجابايت.

لا تعطّل overcommit. يجب أن تكون قيمة `cat /proc/sys/vm/overcommit_memory` هي 0 أو 1. شغّل

```bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

استخدم `perf top` لمراقبة الوقت الذي يُقضى داخل النواة في إدارة الذاكرة.
كما أن الصفحات الضخمة الدائمة لا تحتاج أيضًا إلى تخصيص.

<div id="using-less-than-16gb-of-ram">
  ### استخدام أقل من 16GB من RAM
</div>

الكمية الموصى بها من RAM هي 32 GB أو أكثر.

إذا كان نظامك يحتوي على أقل من 16 GB من RAM، فقد تواجه استثناءات مختلفة متعلقة بالذاكرة لأن الإعدادات الافتراضية لا تتناسب مع هذا الحجم من الذاكرة. يمكنك استخدام ClickHouse على نظام ذي RAM منخفضة (حتى 2 GB)، لكن هذه الإعدادات تتطلب ضبطًا إضافيًا ولا يمكنها سوى ingest البيانات بمعدل منخفض.

عند استخدام ClickHouse مع أقل من 16GB من RAM، نوصي بما يلي:

* خفّض حجم mark cache في `config.xml`. يمكن ضبطه حتى 500 MB، لكن لا يمكن ضبطه على صفر.
* خفّض عدد threads الخاصة بـ query processing إلى `1`.
* خفّض `max_block_size` إلى `8192`. وقد تظل القيم المنخفضة حتى `1024` عملية.
* خفّض `max_download_threads` إلى `1`.
* اضبط `input_format_parallel_parsing` و`output_format_parallel_formatting` على `0`.
* عطّل الكتابة في log tables، لأن ذلك يُبقي مهمة الدمج في الخلفية محتفظةً بـ RAM لتنفيذ عمليات دمج log tables. عطّل `asynchronous_metric_log` و`metric_log` و`text_log` و`trace_log`.

ملاحظات إضافية:

* لتفريغ الذاكرة المخزنة مؤقتًا بواسطة مخصّص الذاكرة، يمكنك تشغيل الأمر `SYSTEM JEMALLOC PURGE`.
* لا نوصي باستخدام تكاملات S3 أو Kafka على الأجهزة منخفضة الذاكرة لأنها تتطلب مقدارًا كبيرًا من الذاكرة للمخازن المؤقتة.

<div id="storage-subsystem">
  ## النظام الفرعي للتخزين
</div>

إذا كانت ميزانيتك تسمح باستخدام SSD، فاستخدم SSD.
وإلا، فاستخدم HDD. وتفي أقراص SATA HDD بسرعة 7200 دورة في الدقيقة بالغرض.

أعطِ الأفضلية لعدد كبير من الخوادم المزودة بأقراص محلية على عدد أقل من الخوادم المتصلة برفوف أقراص.
لكن لتخزين الأرشيفات التي نادرًا ما تُجرى عليها استعلامات، فإن رفوف الأقراص ستفي بالغرض.

<div id="raid">
  ## RAID
</div>

عند استخدام HDD، يمكنك دمجها في RAID-10 أو RAID-5 أو RAID-6 أو RAID-50.
في Linux، يكون RAID البرمجي أفضل (باستخدام `mdadm`).
عند إنشاء RAID-10، اختر تخطيط `far`.
إذا كانت ميزانيتك تسمح، فاختر RAID-10.

يُعد LVM بحد ذاته (من دون RAID أو `mdadm`) خيارًا مقبولًا، لكن إنشاء RAID باستخدامه أو دمجه مع `mdadm` خيار أقل شيوعًا، وتكون معه احتمالات حدوث الأخطاء أكبر
(مثل اختيار حجم chunk غير صحيح، أو عدم محاذاة الـ chunks، أو اختيار نوع RAID غير صحيح، أو نسيان تنظيف الأقراص). إذا كنت واثقًا
من استخدام LVM، فلا يوجد ما يمنعك من استخدامه.

إذا كان لديك أكثر من 4 أقراص، فاستخدم RAID-6 (وهو الخيار المفضل) أو RAID-50 بدلًا من RAID-5.
عند استخدام RAID-5 أو RAID-6 أو RAID-50، احرص دائمًا على زيادة stripe&#95;cache&#95;size، لأن القيمة الافتراضية غالبًا لا تكون الخيار الأفضل.

```bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

احسب العدد الدقيق استنادًا إلى عدد الأجهزة وحجم الكتلة، باستخدام الصيغة: `2 * num_devices * chunk_size_in_bytes / 4096`.

يُعد حجم كتلة قدره 64 KB كافيًا لمعظم إعدادات RAID. ويبلغ متوسط حجم الكتابة في clickhouse-server نحو 1 MB ‏(1024 KB)، ولذلك فإن حجم stripe الموصى به هو أيضًا 1 MB. ويمكن تحسين حجم الكتلة عند الحاجة بضبطه على 1 MB مقسومًا على عدد الأقراص غير المخصصة للتكافؤ في مصفوفة RAID، بحيث تُنفَّذ كل عملية كتابة بالتوازي عبر جميع الأقراص غير المخصصة للتكافؤ المتاحة.
لا تضبط حجم الكتلة أبدًا على قيمة صغيرة جدًا أو كبيرة جدًا.

يمكنك استخدام RAID-0 مع أقراص SSD.
وبغض النظر عن استخدام RAID، استخدم دائمًا النسخ المتماثل لضمان أمان البيانات.

فعّل NCQ مع قائمة انتظار طويلة. بالنسبة إلى HDD، اختر المجدول mq-deadline أو CFQ، وبالنسبة إلى SSD، اختر noop. لا تقلّل إعداد &#39;readahead&#39;.
بالنسبة إلى HDD، فعّل ذاكرة التخزين المؤقت للكتابة.

تأكد من أن [`fstrim`](https://en.wikipedia.org/wiki/Trim_\(computing\)) مفعّل لأقراص NVME وSSD في نظام التشغيل لديك (وعادةً ما يُنفَّذ ذلك باستخدام مهمة cron أو خدمة systemd).

<div id="file-system">
  ## نظام الملفات
</div>

يُعد Ext4 الخيار الأكثر موثوقية. اضبط خيارات الربط `noatime`. ويعمل XFS جيدًا أيضًا.
كما أن معظم أنظمة الملفات الأخرى تعمل جيدًا أيضًا.

FAT-32 وexFAT غير مدعومين بسبب عدم دعم الروابط الصلبة.

لا تستخدم أنظمة الملفات المضغوطة، لأن ClickHouse يتولى الضغط بنفسه وبكفاءة أفضل.
ولا يُنصح باستخدام أنظمة الملفات المشفّرة، لأنك تستطيع استخدام التشفير المدمج في ClickHouse، وهو أفضل.

مع أن ClickHouse يمكنه العمل عبر NFS، فليس هذا الخيار الأمثل.

<div id="linux-kernel">
  ## نواة Linux
</div>

لا تستخدم إصدارًا قديمًا من نواة Linux.

<div id="network">
  ## الشبكة
</div>

إذا كنت تستخدم IPv6، فزِد حجم ذاكرة التخزين المؤقت لمسارات التوجيه.
كانت نواة Linux قبل الإصدار 3.2 تعاني من مشكلات كثيرة في دعم IPv6.

استخدم شبكة بسرعة 10 جيجابت على الأقل إن أمكن. ستعمل سرعة 1 جيجابت أيضًا، لكنها ستكون أسوأ بكثير عند تصحيح النسخ المتماثلة التي تحتوي على عشرات التيرابايتات من البيانات، أو عند معالجة الاستعلامات الموزعة التي تتضمن كمية كبيرة من البيانات الوسيطة.

<div id="huge-pages">
  ## الصفحات الضخمة
</div>

اضبط transparent huge pages دائمًا على `madvise`. في إصدارات النواة الأقدم (قبل 5.9)، قد يؤدي ضبط THP على `always` إلى تدهور ملحوظ في الأداء، إذ تقضي النواة وقتًا طويلًا في إلغاء تجزئة الذاكرة، خاصةً على الأنظمة التي تحتوي على 64 جيجابايت أو أكثر من RAM. وقد قدّم الإصدار 5.9 من النواة ميزة compaction الاستباقي، التي تتعامل مع THP بكفاءة أفضل بكثير، لكن ClickHouse لا يزال يعرض تحذيرًا عند بدء التشغيل إذا كان THP مضبوطًا على `always`، لذا يظل `madvise` هو الإعداد الموصى به بغض النظر عن إصدار النواة.

```bash
$ echo 'madvise' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

إذا كنت تريد تعديل إعداد transparent huge pages بشكل دائم، فحرّر الملف `/etc/default/grub` لإضافة `transparent_hugepage=madvise` إلى الخيار `GRUB_CMDLINE_LINUX_DEFAULT`:

```bash
$ GRUB_CMDLINE_LINUX_DEFAULT="transparent_hugepage=madvise ..."
```

بعد ذلك، نفّذ الأمر `sudo update-grub` ثم أعد التشغيل حتى تسري التغييرات.

<div id="hypervisor-configuration">
  ## إعدادات الهايبرفايزر
</div>

إذا كنت تستخدم OpenStack، فاضبط

```ini
cpu_mode=host-passthrough
```

في `nova.conf`.

إذا كنت تستخدم libvirt، فعيّن

```xml
<cpu mode='host-passthrough'/>
```

في تكوين XML.

هذا مهم لكي يتمكن ClickHouse من الحصول على المعلومات الصحيحة عبر التعليمة `cpuid`.
وإلا فقد يحدث تعطل برسالة `Illegal instruction` عند تشغيل الـ hypervisor على طرازات CPU قديمة.

<div id="zookeeper">
  ## ClickHouse Keeper وZooKeeper
</div>

يُنصح باستخدام ClickHouse Keeper بدلاً من ZooKeeper في مجموعات ClickHouse. راجع وثائق [ClickHouse Keeper](../guides/sre/keeper/index.md)

إذا كنت ترغب في الاستمرار في استخدام ZooKeeper، فمن الأفضل استخدام إصدار جديد منه — 3.4.9 أو أحدث. وقد يكون الإصدار المتوفر في توزيعات Linux المستقرة قديماً.

يجب ألا تستخدم أبداً برامج نصية مكتوبة يدوياً لنقل البيانات بين مجموعات ZooKeeper المختلفة، لأن النتيجة ستكون غير صحيحة بالنسبة إلى العُقد التسلسلية. ولا تستخدم أبداً الأداة المساعدة &quot;zkcopy&quot; للسبب نفسه: https://github.com/ksprojects/zkcopy/issues/15

إذا كنت تريد تقسيم مجموعة ZooKeeper حالية إلى مجموعتين، فالطريقة الصحيحة هي زيادة عدد نُسخها المتماثلة ثم إعادة تهيئتها كمجموعتين مستقلتين.

يمكنك تشغيل ClickHouse Keeper على الخادم نفسه الذي يعمل عليه ClickHouse في بيئات الاختبار، أو في البيئات ذات معدل الاستيعاب المنخفض.
أما في بيئات الإنتاج، فنقترح استخدام خوادم منفصلة لكل من ClickHouse وZooKeeper/Keeper، أو وضع ملفات ClickHouse وملفات Keeper على أقراص منفصلة. وذلك لأن ZooKeeper/Keeper شديدا الحساسية لكمون القرص، وقد يستهلك ClickHouse جميع موارد النظام المتاحة.

يمكن أن يكون لديك مراقبون لـ ZooKeeper ضمن المجموعة، لكن يجب ألا تتفاعل خوادم ClickHouse مع هؤلاء المراقبين.

لا تغيّر إعداد `minSessionTimeout`، فقد تؤثر القيم الكبيرة في استقرار إعادة تشغيل ClickHouse.

مع الإعدادات الافتراضية، يُعد ZooKeeper قنبلة موقوتة:

> لن يحذف خادم ZooKeeper الملفات من اللقطات والسجلات القديمة عند استخدام الإعدادات الافتراضية (راجع `autopurge`)، وتقع هذه المسؤولية على عاتق مسؤول التشغيل.

يجب إبطال مفعول هذه القنبلة.

يُستخدم إعداد ZooKeeper (3.5.1) أدناه في بيئة إنتاج كبيرة:

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

إصدار Java:

```text
openjdk 11.0.5-shenandoah 2019-10-15
OpenJDK Runtime Environment (build 11.0.5-shenandoah+10-adhoc.heretic.src)
OpenJDK 64-Bit Server VM (build 11.0.5-shenandoah+10-adhoc.heretic.src, mixed mode)
```

معلمات JVM:

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

تهيئة Salt:

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
  ## برامج مكافحة الفيروسات
</div>

إذا كنت تستخدم برنامج مكافحة فيروسات، فاضبطه لتجاهل المجلدات التي تحتوي على ملفات بيانات ClickHouse (`/var/lib/clickhouse`)، وإلا فقد يتراجع الأداء وقد تواجه أخطاء غير متوقعة أثناء استيعاب البيانات وعمليات الدمج التي تُجرى في الخلفية.

<div id="related-content">
  ## محتوى ذو صلة
</div>

* [هل بدأت مع ClickHouse؟ إليك 13 «خطأً قاتلًا» وكيف تتجنبها](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse)