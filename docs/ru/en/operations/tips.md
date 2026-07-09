---
description: 'Страница с рекомендациями по использованию ClickHouse с открытым исходным кодом'
sidebar_label: 'Рекомендации по использованию OSS'
sidebar_position: 58
slug: /operations/tips
title: 'Рекомендации по использованию OSS'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

<div id="cpu-scaling-governor">
  ## Регулятор частоты CPU
</div>

Всегда используйте регулятор частоты `performance`. Регулятор `on-demand` работает заметно хуже при постоянно высокой нагрузке.

```bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

<div id="cpu-limitations">
  ## Ограничения CPU
</div>

Процессоры могут перегреваться. Используйте `dmesg`, чтобы проверить, не была ли тактовая частота CPU снижена из-за перегрева.
Ограничение также может задаваться извне — на уровне датацентра. Для мониторинга под нагрузкой можно использовать `turbostat`.

<div id="ram">
  ## Оперативная память
</div>

Для небольших объёмов данных (до ~200 ГБ в сжатом виде) лучше использовать объём памяти, сопоставимый с объёмом данных.
Для больших объёмов данных и при обработке интерактивных (онлайн-)запросов следует использовать достаточный объём оперативной памяти (128 ГБ или больше), чтобы горячее подмножество данных помещалось в кэше страниц.
Даже при объёмах данных ~50 ТБ на сервер использование 128 ГБ оперативной памяти значительно повышает производительность запросов по сравнению с 64 ГБ.

Не отключайте overcommit. Значение `cat /proc/sys/vm/overcommit_memory` должно быть 0 или 1. Выполните

```bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

Используйте `perf top`, чтобы наблюдать, сколько времени ядро тратит на управление памятью.
Также не нужно выделять постоянные большие страницы памяти.

<div id="using-less-than-16gb-of-ram">
  ### Использование менее 16 ГБ оперативной памяти
</div>

Рекомендуемый объём оперативной памяти — 32 ГБ и более.

Если в вашей системе менее 16 ГБ оперативной памяти, вы можете столкнуться с различными исключениями из-за нехватки памяти, поскольку настройки по умолчанию не рассчитаны на такой объём памяти. ClickHouse можно использовать и в системе с небольшим объёмом оперативной памяти (вплоть до 2 ГБ), но такие конфигурации требуют дополнительной настройки и могут обеспечивать приём данных лишь с низкой скоростью.

При использовании ClickHouse с объёмом оперативной памяти менее 16 ГБ мы рекомендуем следующее:

* Уменьшите размер кэша меток в `config.xml`. Его можно установить вплоть до 500 МБ, но нельзя установить в ноль.
* Уменьшите число потоков обработки запросов до `1`.
* Уменьшите `max_block_size` до `8192`. Значения вплоть до `1024` тоже могут быть практичными.
* Уменьшите `max_download_threads` до `1`.
* Установите `input_format_parallel_parsing` и `output_format_parallel_formatting` в `0`.
* Отключите запись в таблицы логов, поскольку из-за этого фоновая задача слияния резервирует оперативную память для выполнения слияний таблиц логов. Отключите `asynchronous_metric_log`, `metric_log`, `text_log`, `trace_log`.

Дополнительные замечания:

* Чтобы освободить память, кэшированную аллокатором, можно выполнить команду `SYSTEM JEMALLOC PURGE`
  .
* Мы не рекомендуем использовать интеграции с S3 или Kafka на машинах с малым объёмом памяти, поскольку для буферов им требуется значительный объём памяти.

<div id="storage-subsystem">
  ## Подсистема хранения
</div>

Если бюджет позволяет использовать SSD, используйте SSD.
Если нет — используйте HDD. Подойдут SATA HDD со скоростью 7200 об/мин.

Лучше отдать предпочтение большому количеству серверов с локальными жёсткими дисками, чем меньшему количеству серверов с подключёнными дисковыми полками.
Но для хранения архивов, к которым обращаются редко, дисковые полки подойдут.

<div id="raid">
  ## RAID
</div>

При использовании HDD их можно объединить в RAID-10, RAID-5, RAID-6 или RAID-50.
Для Linux лучше использовать программный RAID (с `mdadm`).
При создании RAID-10 выбирайте структуру `far`.
Если бюджет позволяет, выбирайте RAID-10.

LVM сам по себе (без RAID или `mdadm`) — вполне нормальный вариант, но создание RAID с его помощью или его сочетание с `mdadm` изучено меньше, и вероятность ошибок выше
(неправильный выбор размера chunk; смещение фрагментов; выбор неподходящего типа RAID; забытая очистка дисков). Если вы уверенно
работаете с LVM, ничто не мешает его использовать.

Если у вас больше 4 дисков, используйте RAID-6 (предпочтительно) или RAID-50 вместо RAID-5.
При использовании RAID-5, RAID-6 или RAID-50 всегда увеличивайте stripe&#95;cache&#95;size, так как значение по умолчанию обычно не является оптимальным.

```bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

Вычислите точное число по количеству устройств и размеру блока, используя формулу: `2 * num_devices * chunk_size_in_bytes / 4096`.

Размер блока 64 КБ достаточен для большинства конфигураций RAID. Средний размер записи clickhouse-server составляет примерно 1 МБ (1024 КБ), поэтому рекомендуемый размер stripe также равен 1 МБ. При необходимости размер блока можно оптимизировать, задав его равным 1 МБ, деленному на количество дисков без четности в массиве RAID, чтобы каждая запись параллельно распределялась по всем доступным дискам без четности.
Никогда не задавайте слишком маленький или слишком большой размер блока.

На SSD можно использовать RAID-0.
Независимо от использования RAID, всегда применяйте репликацию для обеспечения безопасности данных.

Включите NCQ с длинной очередью. Для HDD выберите планировщик mq-deadline или CFQ, а для SSD — noop. Не уменьшайте значение настройки &#39;readahead&#39;.
Для HDD включите кэш записи.

Убедитесь, что [`fstrim`](https://en.wikipedia.org/wiki/Trim_\(computing\)) включен для дисков NVME и SSD в вашей ОС (обычно это реализовано с помощью задания cron или сервиса systemd).

<div id="file-system">
  ## Файловая система
</div>

Ext4 — самый надёжный вариант. Задайте опцию монтирования `noatime`. XFS тоже хорошо подходит.
Большинство других файловых систем также должны работать нормально.

FAT-32 и exFAT не поддерживаются из-за отсутствия жёстких ссылок.

Не используйте файловые системы со сжатием, потому что ClickHouse выполняет сжатие сам и делает это лучше.
Использовать зашифрованные файловые системы не рекомендуется, потому что в ClickHouse есть встроенное шифрование, и оно лучше.

Хотя ClickHouse может работать через NFS, это не лучший вариант.

<div id="linux-kernel">
  ## Ядро Linux
</div>

Не используйте устаревшее ядро Linux.

<div id="network">
  ## Сеть
</div>

Если вы используете IPv6, увеличьте размер кэша маршрутизации.
В ядрах Linux версий ниже 3.2 было множество проблем с реализацией IPv6.

По возможности используйте сеть со скоростью не менее 10 Гбит/с. 1 Гбит/с тоже будет работать, но это значительно ухудшит восстановление реплик с десятками терабайт данных и обработку распределённых запросов с большим объёмом промежуточных данных.

<div id="huge-pages">
  ## Большие страницы памяти
</div>

Всегда устанавливайте transparent huge pages в значение `madvise`. На старых ядрах (до 5.9) значение THP `always` может приводить к существенному снижению производительности: ядро тратит слишком много времени на дефрагментацию памяти, особенно в системах с 64 ГБ+ оперативной памяти. В ядре 5.9 появилась проактивная compaction, которая гораздо лучше справляется с THP, но ClickHouse по-прежнему выводит предупреждение при запуске, если для THP установлено значение `always`, поэтому `madvise` остается рекомендуемым значением независимо от версии ядра.

```bash
$ echo 'madvise' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

Если вы хотите навсегда изменить параметр transparent huge pages, отредактируйте `/etc/default/grub`, добавив `transparent_hugepage=madvise` в параметр `GRUB_CMDLINE_LINUX_DEFAULT`:

```bash
$ GRUB_CMDLINE_LINUX_DEFAULT="transparent_hugepage=madvise ..."
```

После этого выполните команду `sudo update-grub`, а затем перезагрузите систему, чтобы изменения вступили в силу.

<div id="hypervisor-configuration">
  ## Конфигурация гипервизора
</div>

Если вы используете OpenStack, установите

```ini
cpu_mode=host-passthrough
```

в `nova.conf`.

Если вы используете libvirt, установите

```xml
<cpu mode='host-passthrough'/>
```

в XML-конфигурации.

Это важно, чтобы ClickHouse мог получать корректную информацию с помощью инструкции `cpuid`.
Иначе при работе гипервизора на старых моделях CPU возможны сбои с ошибкой `Illegal instruction`.

<div id="zookeeper">
  ## ClickHouse Keeper and ZooKeeper
</div>

Для кластеров ClickHouse рекомендуется использовать ClickHouse Keeper вместо ZooKeeper. См. документацию по [ClickHouse Keeper](../guides/sre/keeper/index.md)

Если вы хотите продолжить использовать ZooKeeper, лучше выбрать актуальную версию ZooKeeper — 3.4.9 или новее. Версия в стабильных дистрибутивах Linux может быть устаревшей.

Никогда не используйте написанные вручную скрипты для передачи данных между разными кластерами ZooKeeper, потому что для последовательных узлов результат будет некорректным. По той же причине никогда не используйте утилиту &quot;zkcopy&quot;: https://github.com/ksprojects/zkcopy/issues/15

Если вы хотите разделить существующий кластер ZooKeeper на два, правильный способ — увеличить число его реплик, а затем перенастроить его как два независимых кластера.

Вы можете запускать ClickHouse Keeper на том же сервере, что и ClickHouse, в тестовых средах или в средах с низкой интенсивностью ингестии.
Для сред продакшн мы рекомендуем использовать отдельные серверы для ClickHouse и ZooKeeper/Keeper либо размещать файлы ClickHouse и файлы Keeper на разных дисках. Это связано с тем, что ZooKeeper/Keeper очень чувствительны к задержкам диска, а ClickHouse может задействовать все доступные системные ресурсы.

В ансамбле ZooKeeper могут быть observers, но серверы ClickHouse не должны с ними взаимодействовать.

Не изменяйте настройку `minSessionTimeout`, большие значения могут повлиять на стабильность перезапуска ClickHouse.

При настройках по умолчанию ZooKeeper — это бомба замедленного действия:

> Сервер ZooKeeper не будет удалять файлы старых снимков и журналов при конфигурации по умолчанию (см. `autopurge`), и это обязанность оператора.

Эту бомбу необходимо обезвредить.

Приведённая ниже конфигурация ZooKeeper (3.5.1) используется в крупной среде продакшн:

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

Версия Java:

```text
openjdk 11.0.5-shenandoah 2019-10-15
OpenJDK Runtime Environment (build 11.0.5-shenandoah+10-adhoc.heretic.src)
OpenJDK 64-Bit Server VM (build 11.0.5-shenandoah+10-adhoc.heretic.src, mixed mode)
```

Параметры JVM:

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

Инициализация Salt:

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
  ## Антивирусное программное обеспечение
</div>

Если вы используете антивирусное программное обеспечение, настройте его так, чтобы оно исключало из проверки каталоги с файлами данных ClickHouse (`/var/lib/clickhouse`), иначе производительность может снизиться, а во время ингестии данных и фоновых слияний могут возникать непредвиденные ошибки.

<div id="related-content">
  ## Связанные материалы
</div>

* [Начинаете работать с ClickHouse? Вот 13 «смертных грехов» и способы их избежать](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse)