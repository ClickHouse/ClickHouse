---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "\u062A\u0648\u0635\u06CC\u0647 \u0647\u0627\u06CC \u0627\u0633\u062A\u0641\
  \u0627\u062F\u0647"
---

# توصیه های استفاده {#usage-recommendations}

## فرماندار پوسته پوسته شدن پردازنده {#cpu-scaling-governor}

همیشه استفاده از `performance` پوسته پوسته شدن فرماندار. این `on-demand` پوسته پوسته شدن فرماندار کار می کند بسیار بدتر با تقاضای به طور مداوم بالا.

``` bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## محدودیت های پردازنده {#cpu-limitations}

پردازنده می تواند بیش از حد گرم. استفاده `dmesg` برای دیدن اگر نرخ ساعت پردازنده به دلیل گرمای بیش از حد محدود بود.
محدودیت همچنین می توانید خارجی در سطح مرکز داده تنظیم شود. شما می توانید استفاده کنید `turbostat` تحت نظر داشته باشمش

## RAM {#ram}

برای مقدار کمی از داده ها (تا ~ 200 گیگابایت فشرده), بهتر است به استفاده از حافظه به همان اندازه که حجم داده ها.
برای مقادیر زیادی از داده ها و در هنگام پردازش تعاملی (اینترنتی) نمایش داده شد, شما باید یک مقدار مناسب از رم استفاده (128 گیگابایت یا بیشتر) بنابراین زیر مجموعه داده های داغ در کش صفحات مناسب خواهد شد.
حتی برای حجم داده ها از ~50 سل در هر سرور, با استفاده از 128 گیگابایت رم به طور قابل توجهی بهبود می بخشد عملکرد پرس و جو در مقایسه با 64 گیگابایت.

هنوز بیش از حد غیر فعال کردن نیست. مقدار `cat /proc/sys/vm/overcommit_memory` باید 0 یا 1. بدو

``` bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

## صفحات بزرگ {#huge-pages}

همیشه صفحات بزرگ شفاف غیر فعال کنید. این با تخصیص حافظه تداخل, که منجر به تخریب عملکرد قابل توجهی.

``` bash
$ echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

استفاده `perf top` برای تماشای زمان صرف شده در هسته برای مدیریت حافظه.
صفحات بزرگ ثابت نیز لازم نیست اختصاص داده شود.

## زیرسیستم ذخیره سازی {#storage-subsystem}

اگر بودجه شما اجازه می دهد تا شما را به استفاده از اس اس دی, استفاده از اس اس دی.
اگر نه, استفاده از هارد. ساعت 7200 دور در دقیقه انجام خواهد شد.

دادن اولویت به بسیاری از سرور با دیسک های سخت محلی بیش از تعداد کمتری از سرور با قفسه های دیسک متصل.
اما برای ذخیره سازی بایگانی با نمایش داده شد نادر, قفسه کار خواهد کرد.

## RAID {#raid}

هنگام استفاده از هارد, شما می توانید حمله خود را ترکیب-10, حمله-5, حمله-6 و یا حمله-50.
برای لینوکس, حمله نرم افزار بهتر است (با `mdadm`). ما توصیه نمی کنیم با استفاده از سطح.
هنگام ایجاد حمله-10, را انتخاب کنید `far` طرح بندی.
اگر بودجه شما اجازه می دهد تا, را انتخاب کنید حمله-10.

اگر شما بیش از 4 دیسک, استفاده از حمله-6 (ترجیحا) و یا حمله-50, به جای حمله-5.
هنگام استفاده از حمله-5, حمله-6 و یا حمله-50, همیشه افزایش نزاع, از مقدار پیش فرض است که معمولا بهترین انتخاب نیست.

``` bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

محاسبه تعداد دقیق از تعداد دستگاه ها و اندازه بلوک با استفاده از فرمول: `2 * num_devices * chunk_size_in_bytes / 4096`.

اندازه بلوک 1024 کیلوبایت برای تمام تنظیمات حمله کافی است.
هرگز اندازه بلوک بیش از حد کوچک یا بیش از حد بزرگ تنظیم شده است.

شما می توانید حمله استفاده-0 در اس اس دی.
صرف نظر از استفاده از حمله, همیشه تکرار برای امنیت داده ها استفاده.

فعال کردن دفتر مرکزی اروپا با یک صف طولانی. برای HDD را انتخاب کنید CFQ زمانبندی و برای SSD را انتخاب کنید noop. کاهش نمی دهد ‘readahead’ تنظیمات.
برای هارد, فعال کردن کش نوشتن.

## سیستم پرونده {#file-system}

موجود 4 قابل اطمینان ترین گزینه است. تنظیم گزینههای سوارکردن `noatime, nobarrier`.
XFS نیز مناسب است اما از آن شده است به طور کامل تست شده با ClickHouse.
اکثر سیستم های فایل های دیگر نیز باید خوب کار می کنند. سیستم های فایل با تاخیر تخصیص کار بهتر است.

## هسته لینوکس {#linux-kernel}

هنوز یک هسته لینوکس منسوخ شده استفاده کنید.

## شبکه {#network}

اگر شما با استفاده از ایپو6, افزایش اندازه کش مسیر.
هسته لینوکس قبل از 3.2 بسیاری از مشکلات با اجرای قانون مجازات اسلامی بود.

استفاده از حداقل یک 10 شبکه گیگابایت, در صورت امکان. 1 گیگابایت نیز کار خواهد کرد, اما برای وصله کپی با ده ها ترابایت داده بسیار بدتر خواهد بود, و یا برای پردازش نمایش داده شد توزیع با مقدار زیادی از داده های متوسط.

## باغ وحش {#zookeeper}

شما احتمالا در حال حاضر با استفاده از باغ وحش برای مقاصد دیگر. شما می توانید نصب و راه اندازی همان باغ وحش استفاده, اگر در حال حاضر بیش از حد نیست.

It's best to use a fresh version of ZooKeeper – 3.4.9 or later. The version in stable Linux distributions may be outdated.

شما هرگز نباید از اسکریپت های دستی نوشته شده برای انتقال داده ها بین خوشه های مختلف باغ وحش استفاده کنید زیرا نتیجه برای گره های متوالی نادرست خواهد بود. هرگز استفاده از “zkcopy” ابزار به همین دلیل: https://github.com/ksprojects/zkcopy/issues/15

اگر میخواهید یک خوشه باغ وحش موجود را به دو قسمت تقسیم کنید راه درست این است که تعداد تکرار های خود را افزایش دهید و سپس به عنوان دو خوشه مستقل پیکربندی کنید.

باغ وحش را بر روی سرورهای مشابه کلیک کنید. چرا که باغ وحش برای تاخیر بسیار حساس است و خانه رعیتی ممکن است تمام منابع سیستم در دسترس استفاده کنند.

با تنظیمات پیش فرض, باغ وحش یک بمب زمان است:

> سرور باغ وحش فایل ها را از عکس های فوری و سیاهههای مربوط قدیمی هنگام استفاده از پیکربندی پیش فرض حذف نمی کند (نگاه کنید به کالبد شکافی), و این به عهده اپراتور است.

این بمب باید خنثی شود.

باغ وحش (3.5.1) پیکربندی زیر در یاندکس استفاده می شود.محیط تولید متریکا تا 20 مه 2017:

باغ وحش.cfg:

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

نسخه جاوا:

``` text
Java(TM) SE Runtime Environment (build 1.8.0_25-b17)
Java HotSpot(TM) 64-Bit Server VM (build 25.25-b02, mixed mode)
```

پارامترهای جی ام:

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

نمک درون:

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

{## [مقاله اصلی](https://clickhouse.tech/docs/en/operations/tips/) ##}
