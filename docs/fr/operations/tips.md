---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: Recommandations D'Utilisation
---

# Recommandations D'Utilisation {#usage-recommendations}

## Gouverneur de mise à L'échelle du processeur {#cpu-scaling-governor}

Utilisez toujours la `performance` mise à l'échelle gouverneur. Le `on-demand` gouverneur de mise à l'échelle fonctionne bien pire avec une demande constamment élevée.

``` bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## CPU Limitations {#cpu-limitations}

Les processeurs peuvent surchauffer. Utiliser `dmesg` pour voir si la fréquence D'horloge du processeur était limitée en raison de la surchauffe.
La restriction peut également être définie en externe au niveau du centre de données. Vous pouvez utiliser `turbostat` à surveiller sous une charge.

## RAM {#ram}

Pour de petites quantités de données (jusqu'à ~200 GO en mode compressé), il est préférable d'utiliser autant de mémoire que le volume de données.
Pour de grandes quantités de données et lors du traitement de requêtes interactives (en ligne), vous devez utiliser une quantité raisonnable de RAM (128 Go ou plus) afin que le sous-ensemble de données chaudes s'intègre dans le cache des pages.
Même pour des volumes de données d'environ 50 To par serveur, l'utilisation de 128 Go de RAM améliore considérablement les performances des requêtes par rapport à 64 Go.

Ne désactivez pas de surcharge. Valeur `cat /proc/sys/vm/overcommit_memory` devrait être 0 ou 1. Exécuter

``` bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

## Huge Pages {#huge-pages}

Toujours désactiver les pages énormes transparentes. Il interfère avec les allocateurs de mémoire, ce qui entraîne une dégradation significative des performances.

``` bash
$ echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

Utiliser `perf top` pour regarder le temps passé dans le noyau pour la gestion de la mémoire.
Les pages énormes permanentes n'ont pas non plus besoin d'être allouées.

## Sous-Système De Stockage {#storage-subsystem}

Si votre budget vous permet D'utiliser SSD, utilisez SSD.
Sinon, utilisez un disque dur. Disques durs SATA 7200 RPM fera l'affaire.

Donner la préférence à un grand nombre de serveurs avec des disques durs locaux sur un plus petit nombre de serveurs avec un disque attaché étagères.
Mais pour stocker des archives avec des requêtes rares, les étagères fonctionneront.

## RAID {#raid}

Lorsque vous utilisez le disque dur, vous pouvez combiner leur RAID-10, RAID-5, RAID-6 ou RAID-50.
Pour Linux, le RAID logiciel est meilleur (avec `mdadm`). Nous ne recommandons pas d'utiliser LVM.
Lors de la création de RAID-10, sélectionnez `far` disposition.
Si votre budget le permet, choisissez RAID-10.

Si vous avez plus de 4 disques, Utilisez RAID-6 (préféré) ou RAID-50, au lieu de RAID-5.
Lorsque vous utilisez RAID-5, RAID-6 ou RAID-50, augmentez toujours stripe_cache_size, car la valeur par défaut n'est généralement pas le meilleur choix.

``` bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

Calculez le nombre exact à partir du nombre de périphériques et de la taille du bloc, en utilisant la formule: `2 * num_devices * chunk_size_in_bytes / 4096`.

Une taille de bloc de 1024 KO est suffisante pour toutes les configurations RAID.
Ne définissez jamais la taille du bloc trop petite ou trop grande.

Vous pouvez utiliser RAID-0 sur SSD.
Quelle que soit L'utilisation du RAID, utilisez toujours la réplication pour la sécurité des données.

Activer NCQ avec une longue file d'attente. Pour HDD, choisissez le planificateur CFQ, et pour SSD, choisissez noop. Ne pas réduire le ‘readahead’ paramètre.
Pour le disque dur, activez le cache d'écriture.

## Système De Fichiers {#file-system}

Ext4 est l'option la plus fiable. Définir les options de montage `noatime, nobarrier`.
XFS est également adapté, mais il n'a pas été aussi soigneusement testé avec ClickHouse.
La plupart des autres systèmes de fichiers devraient également fonctionner correctement. Les systèmes de fichiers avec allocation retardée fonctionnent mieux.

## Le Noyau Linux {#linux-kernel}

N'utilisez pas un noyau Linux obsolète.

## Réseau {#network}

Si vous utilisez IPv6, augmenter la taille du cache.
Le noyau Linux avant 3.2 avait une multitude de problèmes avec l'implémentation D'IPv6.

Utilisez au moins un réseau de 10 Go, si possible. 1 Go fonctionnera également, mais ce sera bien pire pour patcher des répliques avec des dizaines de téraoctets de données, ou pour traiter des requêtes distribuées avec une grande quantité de données intermédiaires.

## ZooKeeper {#zookeeper}

Vous utilisez probablement déjà ZooKeeper à d'autres fins. Vous pouvez utiliser la même installation de ZooKeeper, si elle n'est pas déjà surchargée.

It's best to use a fresh version of ZooKeeper – 3.4.9 or later. The version in stable Linux distributions may be outdated.

Vous ne devez jamais utiliser de scripts écrits manuellement pour transférer des données entre différents clusters ZooKeeper, car le résultat sera incorrect pour les nœuds séquentiels. Ne jamais utiliser de l' “zkcopy” utilitaire pour la même raison: https://github.com/ksprojects/zkcopy/issues/15

Si vous souhaitez diviser un cluster Zookeeper existant en deux, le bon moyen est d'augmenter le nombre de ses répliques, puis de le reconfigurer en deux clusters indépendants.

N'exécutez pas ZooKeeper sur les mêmes serveurs que ClickHouse. Parce que ZooKeeper est très sensible à la latence et ClickHouse peut utiliser toutes les ressources système disponibles.

Avec les paramètres par défaut, ZooKeeper est une bombe à retardement:

> Le serveur ZooKeeper ne supprime pas les fichiers des anciens snapshots et journaux lors de l'utilisation de la configuration par défaut (voir autopurge), et c'est la responsabilité de l'opérateur.

Cette bombe doit être désamorcée.

La configuration ZooKeeper (3.5.1) ci-dessous est utilisée dans le Yandex.Environnement de production Metrica au 20 mai 2017:

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

Version de Java:

``` text
Java(TM) SE Runtime Environment (build 1.8.0_25-b17)
Java HotSpot(TM) 64-Bit Server VM (build 25.25-b02, mixed mode)
```

Les paramètres de la JVM:

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

Sel init:

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

{## [Article Original](https://clickhouse.tech/docs/en/operations/tips/) ##}
