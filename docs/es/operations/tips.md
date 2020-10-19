---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: Recomendaciones de uso
---

# Recomendaciones de uso {#usage-recommendations}

## CPU Scaling Governor {#cpu-scaling-governor}

Utilice siempre el `performance` gobernador de escala. El `on-demand` regulador de escala funciona mucho peor con una demanda constante.

``` bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## Limitaciones de la CPU {#cpu-limitations}

Los procesadores pueden sobrecalentarse. Utilizar `dmesg` para ver si la velocidad de reloj de la CPU era limitada debido al sobrecalentamiento.
La restricción también se puede establecer externamente en el nivel del centro de datos. Usted puede utilizar `turbostat` para controlarlo bajo una carga.

## RAM {#ram}

Para pequeñas cantidades de datos (hasta ~200 GB comprimidos), es mejor usar tanta memoria como el volumen de datos.
Para grandes cantidades de datos y al procesar consultas interactivas (en línea), debe usar una cantidad razonable de RAM (128 GB o más) para que el subconjunto de datos en caliente quepa en la memoria caché de páginas.
Incluso para volúmenes de datos de ~ 50 TB por servidor, el uso de 128 GB de RAM mejora significativamente el rendimiento de las consultas en comparación con 64 GB.

No deshabilite el sobrecompromiso. Valor `cat /proc/sys/vm/overcommit_memory` debe ser 0 o 1. Ejecutar

``` bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

## Páginas enormes {#huge-pages}

Siempre deshabilite las páginas enormes transparentes. Interfiere con los asignadores de memoria, lo que conduce a una degradación significativa del rendimiento.

``` bash
$ echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

Utilizar `perf top` para ver el tiempo pasado en el kernel para la administración de memoria.
Las páginas enormes permanentes tampoco necesitan ser asignadas.

## Subsistema de almacenamiento {#storage-subsystem}

Si su presupuesto le permite usar SSD, use SSD.
Si no, use HDD. Los discos duros SATA 7200 RPM servirán.

Dar preferencia a una gran cantidad de servidores con discos duros locales sobre un número menor de servidores con estantes de discos conectados.
Pero para almacenar archivos con consultas raras, los estantes funcionarán.

## RAID {#raid}

Al usar HDD, puede combinar su RAID-10, RAID-5, RAID-6 o RAID-50.
Para Linux, el software RAID es mejor (con `mdadm`). No recomendamos usar LVM.
Al crear RAID-10, seleccione el `far` diseño.
Si su presupuesto lo permite, elija RAID-10.

Si tiene más de 4 discos, utilice RAID-6 (preferido) o RAID-50, en lugar de RAID-5.
Cuando use RAID-5, RAID-6 o RAID-50, siempre aumente stripe_cache_size, ya que el valor predeterminado generalmente no es la mejor opción.

``` bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

Calcule el número exacto a partir del número de dispositivos y el tamaño del bloque, utilizando la fórmula: `2 * num_devices * chunk_size_in_bytes / 4096`.

Un tamaño de bloque de 1024 KB es suficiente para todas las configuraciones RAID.
Nunca ajuste el tamaño del bloque demasiado pequeño o demasiado grande.

Puede usar RAID-0 en SSD.
Independientemente del uso de RAID, utilice siempre la replicación para la seguridad de los datos.

Habilite NCQ con una cola larga. Para HDD, elija el programador CFQ, y para SSD, elija noop. No reduzca el ‘readahead’ configuración.
Para HDD, habilite la memoria caché de escritura.

## Sistema de archivos {#file-system}

Ext4 es la opción más confiable. Establecer las opciones de montaje `noatime, nobarrier`.
XFS también es adecuado, pero no ha sido probado tan a fondo con ClickHouse.
La mayoría de los otros sistemas de archivos también deberían funcionar bien. Los sistemas de archivos con asignación retrasada funcionan mejor.

## Núcleo de Linux {#linux-kernel}

No use un kernel de Linux obsoleto.

## Red {#network}

Si está utilizando IPv6, aumente el tamaño de la caché de ruta.
El kernel de Linux anterior a 3.2 tenía una multitud de problemas con la implementación de IPv6.

Utilice al menos una red de 10 GB, si es posible. 1 Gb también funcionará, pero será mucho peor para parchear réplicas con decenas de terabytes de datos, o para procesar consultas distribuidas con una gran cantidad de datos intermedios.

## ZooKeeper {#zookeeper}

Probablemente ya esté utilizando ZooKeeper para otros fines. Puede usar la misma instalación de ZooKeeper, si aún no está sobrecargada.

It's best to use a fresh version of ZooKeeper – 3.4.9 or later. The version in stable Linux distributions may be outdated.

Nunca debe usar scripts escritos manualmente para transferir datos entre diferentes clústeres de ZooKeeper, ya que el resultado será incorrecto para los nodos secuenciales. Nunca utilice el “zkcopy” utilidad por la misma razón: https://github.com/ksprojects/zkcopy/issues/15

Si desea dividir un clúster ZooKeeper existente en dos, la forma correcta es aumentar el número de sus réplicas y, a continuación, volver a configurarlo como dos clústeres independientes.

No ejecute ZooKeeper en los mismos servidores que ClickHouse. Porque ZooKeeper es muy sensible a la latencia y ClickHouse puede utilizar todos los recursos del sistema disponibles.

Con la configuración predeterminada, ZooKeeper es una bomba de tiempo:

> El servidor ZooKeeper no eliminará archivos de instantáneas y registros antiguos cuando utilice la configuración predeterminada (consulte autopurge), y esto es responsabilidad del operador.

Esta bomba debe ser desactivada.

La configuración ZooKeeper (3.5.1) a continuación se usa en Yandex.Entorno de producción de Métrica al 20 de mayo de 2017:

zoológico.Cómo:

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

Versión Java:

``` text
Java(TM) SE Runtime Environment (build 1.8.0_25-b17)
Java HotSpot(TM) 64-Bit Server VM (build 25.25-b02, mixed mode)
```

Parámetros de JVM:

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

Sal init:

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

{## [Artículo Original](https://clickhouse.tech/docs/en/operations/tips/) ##}
