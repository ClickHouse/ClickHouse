---
toc_priority: 58
toc_title: "\u0421\u043e\u0432\u0435\u0442\u044b\u0020\u043f\u043e\u0020\u044d\u043a\u0441\u043f\u043b\u0443\u0430\u0442\u0430\u0446\u0438\u0438"
---

# Советы по эксплуатации {#sovety-po-ekspluatatsii}

## CPU Scaling Governor {#cpu-scaling-governor}

Всегда используйте `performance` scaling governor. `ondemand` scaling governor работает намного хуже при постоянно высоком спросе.

``` bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## Ограничение CPU {#ogranichenie-cpu}

Процессоры могут перегреваться. С помощью `dmesg` можно увидеть, если тактовая частота процессора была ограничена из-за перегрева.
Также ограничение может устанавливаться снаружи на уровне дата-центра. С помощью `turbostat` можно за этим наблюдать под нагрузкой.

## Оперативная память {#operativnaia-pamiat}

Для небольших объёмов данных (до ~200 Гб в сжатом виде) лучше всего использовать столько памяти не меньше, чем объём данных.
Для больших объёмов данных, при выполнении интерактивных (онлайн) запросов, стоит использовать разумный объём оперативной памяти (128 Гб или более) для того, чтобы горячее подмножество данных поместилось в кеше страниц.
Даже для объёмов данных в ~50 Тб на сервер, использование 128 Гб оперативной памяти намного лучше для производительности выполнения запросов, чем 64 Гб.

Не выключайте overcommit. Значение `cat /proc/sys/vm/overcommit_memory` должно быть 0 or 1. Выполните:

``` bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

## Huge Pages {#huge-pages}

Механизм прозрачных huge pages нужно отключить. Он мешает работе аллокаторов памяти, что приводит к значительной деградации производительности.

``` bash
$ echo 'madvise' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

С помощью `perf top` можно наблюдать за временем, проведенном в ядре операционной системы для управления памятью.
Постоянные huge pages так же не нужно аллоцировать.

## Подсистема хранения {#podsistema-khraneniia}

Если ваш бюджет позволяет использовать SSD, используйте SSD.
В противном случае используйте HDD. SATA HDDs 7200 RPM подойдут.

Предпочитайте много серверов с локальными жесткими дисками вместо меньшего числа серверов с подключенными дисковыми полками.
Но для хранения архивов с редкими запросами полки всё же подходят.

## RAID {#raid}

При использовании HDD можно объединить их RAID-10, RAID-5, RAID-6 или RAID-50.
Лучше использовать программный RAID в Linux (`mdadm`). Лучше не использовать LVM.
При создании RAID-10, нужно выбрать `far` расположение.
Если бюджет позволяет, лучше выбрать RAID-10.

На более чем 4 дисках вместо RAID-5 нужно использовать RAID-6 (предпочтительнее) или RAID-50.
При использовании RAID-5, RAID-6 или RAID-50, нужно всегда увеличивать stripe_cache_size, так как значение по умолчанию выбрано не самым удачным образом.

``` bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

Точное число стоит вычислять из числа устройств и размер блока по формуле: `2 * num_devices * chunk_size_in_bytes / 4096`.

Размер блока в 1024 Кб подходит для всех конфигураций RAID.
Никогда не указывайте слишком маленький или слишком большой размер блока.

На SSD можно использовать RAID-0.
Вне зависимости от использования RAID, всегда используйте репликацию для безопасности данных.

Включите NCQ с длинной очередью. Для HDD стоит выбрать планировщик CFQ, а для SSD — noop. Не стоит уменьшать настройку readahead.
На HDD стоит включать кеш записи.

## Файловая система {#failovaia-sistema}

Ext4 самый проверенный вариант. Укажите опции монтирования `noatime,nobarrier`.
XFS также подходит, но не так тщательно протестирована в сочетании с ClickHouse.
Большинство других файловых систем также должны нормально работать. Файловые системы с отложенной аллокацией работают лучше.

## Ядро Linux {#iadro-linux}

Не используйте слишком старое ядро Linux.

## Сеть {#set}

При использовании IPv6, стоит увеличить размер кеша маршрутов.
Ядра Linux до 3.2 имели массу проблем в реализации IPv6.

Предпочитайте как минимум 10 Гбит сеть. 1 Гбит также будет работать, но намного хуже для починки реплик с десятками терабайт данных или для обработки распределенных запросов с большим объёмом промежуточных данных.

## ZooKeeper {#zookeeper}

Вероятно вы уже используете ZooKeeper для других целей. Можно использовать ту же инсталляцию ZooKeeper, если она не сильно перегружена.

Лучше использовать свежую версию ZooKeeper, как минимум 3.4.9. Версия в стабильных дистрибутивах Linux может быть устаревшей.

Никогда не используете написанные вручную скрипты для переноса данных между разными ZooKeeper кластерами, потому что результат будет некорректный для sequential нод. Никогда не используйте утилиту «zkcopy», по той же причине: https://github.com/ksprojects/zkcopy/issues/15

Если вы хотите разделить существующий ZooKeeper кластер на два, правильный способ - увеличить количество его реплик, а затем переконфигурировать его как два независимых кластера.

Не запускайте ZooKeeper на тех же серверах, что и ClickHouse. Потому что ZooKeeper очень чувствителен к задержкам, а ClickHouse может использовать все доступные системные ресурсы.

С настройками по умолчанию, ZooKeeper является бомбой замедленного действия:

> Сервер ZooKeeper не будет удалять файлы со старыми снепшоты и логами при использовании конфигурации по умолчанию (см. autopurge), это является ответственностью оператора.

Эту бомбу нужно обезвредить.

Далее описана конфигурация ZooKeeper (3.5.1), используемая в боевом окружении Яндекс.Метрики на момент 20 мая 2017 года:

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
dataDir=/opt/zookeeper/{{ '{{' }} cluster['name'] {{ '{{' }} '}}' }}/data
# Place the dataLogDir to a separate physical disc for better performance
dataLogDir=/opt/zookeeper/{{ '{{' }} cluster['name'] {{ '{{' }} '}}' }}/logs

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
dynamicConfigFile=/etc/zookeeper-{{ '{{' }} cluster['name'] {{ '{{' }} '}}' }}/conf/zoo.cfg.dynamic
```

Версия Java:

``` text
Java(TM) SE Runtime Environment (build 1.8.0_25-b17)
Java HotSpot(TM) 64-Bit Server VM (build 25.25-b02, mixed mode)
```

Параметры JVM:

``` bash
NAME=zookeeper-{{ '{{' }} cluster['name'] {{ '{{' }} '}}' }}
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
JAVA_OPTS="-Xms{{ '{{' }} cluster.get('xms','128M') {{ '{{' }} '}}' }} \
    -Xmx{{ '{{' }} cluster.get('xmx','1G') {{ '{{' }} '}}' }} \
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

Salt init:

``` text
description "zookeeper-{{ '{{' }} cluster['name'] {{ '{{' }} '}}' }} centralized coordination service"

start on runlevel [2345]
stop on runlevel [!2345]

respawn

limit nofile 8192 8192

pre-start script
    [ -r "/etc/zookeeper-{{ '{{' }} cluster['name'] {{ '{{' }} '}}' }}/conf/environment" ] || exit 0
    . /etc/zookeeper-{{ '{{' }} cluster['name'] {{ '{{' }} '}}' }}/conf/environment
    [ -d $ZOO_LOG_DIR ] || mkdir -p $ZOO_LOG_DIR
    chown $USER:$GROUP $ZOO_LOG_DIR
end script

script
    . /etc/zookeeper-{{ '{{' }} cluster['name'] {{ '{{' }} '}}' }}/conf/environment
    [ -r /etc/default/zookeeper ] && . /etc/default/zookeeper
    if [ -z "$JMXDISABLE" ]; then
        JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=$JMXLOCALONLY"
    fi
    exec start-stop-daemon --start -c $USER --exec $JAVA --name zookeeper-{{ '{{' }} cluster['name'] {{ '{{' }} '}}' }} \
        -- -cp $CLASSPATH $JAVA_OPTS -Dzookeeper.log.dir=${ZOO_LOG_DIR} \
        -Dzookeeper.root.logger=${ZOO_LOG4J_PROP} $ZOOMAIN $ZOOCFG
end script
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/tips/) <!--hide-->
