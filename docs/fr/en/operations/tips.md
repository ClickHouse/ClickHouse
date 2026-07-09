---
description: "Page décrivant les recommandations d’utilisation de ClickHouse open-source"
sidebar_label: "Recommandations d’utilisation de ClickHouse OSS"
sidebar_position: 58
slug: /operations/tips
title: "Recommandations d’utilisation de ClickHouse OSS"
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

<div id="cpu-scaling-governor">
  ## Gouverneur de mise à l’échelle du CPU
</div>

Utilisez toujours le gouverneur `performance`. Le gouverneur `on-demand` fonctionne nettement moins bien lorsque la charge reste élevée en permanence.

```bash
$ echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

<div id="cpu-limitations">
  ## Limites du CPU
</div>

Les processeurs peuvent surchauffer. Utilisez `dmesg` pour vérifier si la fréquence d’horloge du CPU a été réduite en raison d’une surchauffe.
Cette limitation peut également être imposée en externe, au niveau du datacenter. Vous pouvez utiliser `turbostat` pour la surveiller en charge.

<div id="ram">
  ## RAM
</div>

Pour de petits volumes de données (jusqu’à ~200 Go compressés), il est préférable d’utiliser autant de mémoire que de données.
Pour de grands volumes de données et pour le traitement de requêtes interactives (en ligne), vous devez utiliser une quantité raisonnable de RAM (128 Go ou plus) afin que le sous-ensemble de données les plus sollicitées tienne dans le cache de pages.
Même pour des volumes de données d’environ 50 To par serveur, l’utilisation de 128 Go de RAM améliore considérablement les performances des requêtes par rapport à 64 Go.

Ne désactivez pas l’overcommit. La valeur de `cat /proc/sys/vm/overcommit_memory` doit être 0 ou 1. Exécutez

```bash
$ echo 0 | sudo tee /proc/sys/vm/overcommit_memory
```

Utilisez `perf top` pour surveiller le temps passé dans le noyau à gérer la mémoire.
Les huge pages permanentes n&#39;ont pas non plus besoin d&#39;être allouées.

<div id="using-less-than-16gb-of-ram">
  ### Utiliser moins de 16GB de RAM
</div>

La quantité de RAM recommandée est de 32 GB ou plus.

Si votre système dispose de moins de 16 GB de RAM, vous risquez de rencontrer diverses exceptions liées à la mémoire, car les paramètres par défaut ne sont pas adaptés à cette quantité de mémoire. Vous pouvez utiliser ClickHouse sur un système avec peu de RAM (jusqu&#39;à 2 GB), mais ces configurations nécessitent des ajustements supplémentaires et ne peuvent ingérer des données qu&#39;à faible débit.

Lorsque vous utilisez ClickHouse avec moins de 16GB de RAM, nous recommandons ce qui suit :

* Réduisez la taille du mark cache dans le `config.xml`. Il peut être défini à seulement 500 MB, mais pas à zéro.
* Réduisez le nombre de threads de traitement des requêtes à `1`.
* Réduisez `max_block_size` à `8192`. Des valeurs aussi basses que `1024` peuvent rester pratiques.
* Réduisez `max_download_threads` à `1`.
* Définissez `input_format_parallel_parsing` et `output_format_parallel_formatting` à `0`.
* Désactivez l&#39;écriture dans les log tables, car cela conduit la tâche de fusion en arrière-plan à réserver de la RAM pour effectuer des fusions des log tables. Désactivez `asynchronous_metric_log`, `metric_log`, `text_log`, `trace_log`.

Remarques supplémentaires :

* Pour purger la mémoire mise en cache par l&#39;allocateur mémoire, vous pouvez exécuter la commande `SYSTEM JEMALLOC PURGE`.
* Nous ne recommandons pas d&#39;utiliser les intégrations S3 ou Kafka sur des machines disposant de peu de mémoire, car elles nécessitent une quantité importante de mémoire pour les buffers.

<div id="storage-subsystem">
  ## Sous-système de stockage
</div>

Si votre budget vous permet d’utiliser des SSD, utilisez des SSD.
Sinon, utilisez des HDD. Des disques SATA à 7 200 tr/min feront l’affaire.

Privilégiez un grand nombre de serveurs équipés de disques durs locaux plutôt qu’un plus petit nombre de serveurs reliés à des baies de disques.
Mais pour stocker des archives rarement interrogées, les baies conviendront.

<div id="raid">
  ## RAID
</div>

Lorsque vous utilisez des HDD, vous pouvez les regrouper en RAID-10, RAID-5, RAID-6 ou RAID-50.
Sous Linux, le RAID logiciel est préférable (avec `mdadm`).
Lors de la création d’un RAID-10, sélectionnez l’agencement `far`.
Si votre budget le permet, choisissez le RAID-10.

LVM à lui seul (sans RAID ni `mdadm`) convient, mais créer un RAID avec LVM ou le combiner avec `mdadm` est une option moins éprouvée, avec davantage de risques d’erreur
(sélection d’une taille de chunk incorrecte ; mauvais alignement des chunks ; choix d’un type de RAID inadapté ; oubli du nettoyage des disques). Si vous maîtrisez
l’utilisation de LVM, rien ne s’oppose à son usage.

Si vous avez plus de 4 disques, utilisez le RAID-6 (de préférence) ou le RAID-50, plutôt que le RAID-5.
Lorsque vous utilisez le RAID-5, le RAID-6 ou le RAID-50, augmentez toujours stripe&#95;cache&#95;size, car la valeur par défaut n’est généralement pas le meilleur choix.

```bash
$ echo 4096 | sudo tee /sys/block/md2/md/stripe_cache_size
```

Calculez la valeur exacte à partir du nombre de périphériques et de la taille de bloc, à l’aide de la formule : `2 * num_devices * chunk_size_in_bytes / 4096`.

Une taille de bloc de 64 KB suffit pour la plupart des configurations RAID. La taille moyenne des écritures de clickhouse-server est d’environ 1 MB (1024 KB) ; la taille de stripe recommandée est donc elle aussi de 1 MB. Si nécessaire, la taille de bloc peut être optimisée en la définissant sur 1 MB divisé par le nombre de disques RAID sans parité, afin que chaque écriture soit parallélisée sur tous les disques sans parité disponibles.
Ne définissez jamais une taille de bloc trop petite ou trop grande.

Vous pouvez utiliser RAID-0 sur SSD.
Que vous utilisiez RAID ou non, utilisez toujours la réplication pour sécuriser les données.

Activez NCQ avec une file d’attente longue. Pour les HDD, choisissez l’ordonnanceur mq-deadline ou CFQ, et pour les SSD, choisissez noop. Ne réduisez pas le paramètre &#39;readahead&#39;.
Pour les HDD, activez le cache d’écriture.

Assurez-vous que [`fstrim`](https://en.wikipedia.org/wiki/Trim_\(computing\)) est activé pour les disques NVME et SSD dans votre système d’exploitation (généralement via une tâche cron ou un service systemd).

<div id="file-system">
  ## Système de fichiers
</div>

Ext4 est l’option la plus fiable. Définissez l’option de montage `noatime`. XFS fonctionne également bien.
La plupart des autres systèmes de fichiers devraient aussi convenir.

FAT-32 et exFAT ne sont pas pris en charge en raison de l’absence de liens physiques.

N’utilisez pas de systèmes de fichiers compressés, car ClickHouse gère lui-même la compression, et de façon plus efficace.
L’utilisation de systèmes de fichiers chiffrés n’est pas recommandée, car vous pouvez utiliser le chiffrement intégré de ClickHouse, qui est plus performant.

Même si ClickHouse peut fonctionner sur NFS, ce n’est pas l’idéal.

<div id="linux-kernel">
  ## Noyau Linux
</div>

N&#39;utilisez pas de noyau Linux obsolète.

<div id="network">
  ## Réseau
</div>

Si vous utilisez IPv6, augmentez la taille du cache de routage.
Le noyau Linux, avant la version 3.2, présentait de nombreux problèmes dans son implémentation d’IPv6.

Utilisez, si possible, un réseau d’au moins 10 Gb. Un réseau à 1 Gb fonctionnera aussi, mais il sera nettement moins performant pour appliquer des correctifs à des répliques contenant des dizaines de téraoctets de données, ou pour traiter des requêtes distribuées avec une grande quantité de données intermédiaires.

<div id="huge-pages">
  ## Huge Pages
</div>

Réglez toujours les transparent huge pages (THP) sur `madvise`. Sur les anciens noyaux (antérieurs à 5.9), des THP réglées sur `always` peuvent entraîner une dégradation significative des performances : le noyau passe un temps excessif à défragmenter la mémoire, en particulier sur les systèmes disposant de plus de 64 Go de RAM. Le noyau 5.9 a introduit la compaction proactive, qui gère bien mieux les THP, mais ClickHouse affiche toujours un avertissement au démarrage si les THP sont réglées sur `always` ; `madvise` reste donc le paramètre recommandé, quelle que soit la version du noyau.

```bash
$ echo 'madvise' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

Si vous souhaitez modifier de façon permanente le paramètre transparent huge pages, modifiez le fichier `/etc/default/grub` pour ajouter `transparent_hugepage=madvise` à l’option `GRUB_CMDLINE_LINUX_DEFAULT` :

```bash
$ GRUB_CMDLINE_LINUX_DEFAULT="transparent_hugepage=madvise ..."
```

Après cela, exécutez la commande `sudo update-grub`, puis redémarrez pour appliquer les changements.

<div id="hypervisor-configuration">
  ## Configuration de l’hyperviseur
</div>

Si vous utilisez OpenStack, définissez

```ini
cpu_mode=host-passthrough
```

dans `nova.conf`.

Si vous utilisez libvirt, définissez

```xml
<cpu mode='host-passthrough'/>
```

dans la configuration XML.

Ceci est important pour que ClickHouse puisse récupérer les bonnes informations avec l’instruction `cpuid`.
Sinon, vous risquez des plantages `Illegal instruction` si l’hyperviseur s’exécute sur d’anciens modèles de CPU.

<div id="zookeeper">
  ## ClickHouse Keeper et ZooKeeper
</div>

ClickHouse Keeper est recommandé comme remplacement de ZooKeeper pour les clusters ClickHouse. Consultez la documentation de [ClickHouse Keeper](../guides/sre/keeper/index.md)

Si vous souhaitez continuer à utiliser ZooKeeper, il est préférable d’utiliser une version récente de ZooKeeper, 3.4.9 ou ultérieure. La version fournie avec les distributions Linux stables peut être obsolète.

N’utilisez jamais de scripts écrits manuellement pour transférer des données entre différents clusters ZooKeeper, car le résultat sera incorrect pour les nœuds séquentiels. N’utilisez jamais non plus l’utilitaire &quot;zkcopy&quot; pour la même raison : https://github.com/ksprojects/zkcopy/issues/15

Si vous souhaitez scinder un cluster ZooKeeper existant en deux, la bonne méthode consiste à augmenter le nombre de ses répliques, puis à le reconfigurer en deux clusters indépendants.

Vous pouvez exécuter ClickHouse Keeper sur le même serveur que ClickHouse dans des environnements de test, ou dans des environnements avec un faible taux d’ingestion.
Pour les environnements de production, nous recommandons d’utiliser des serveurs distincts pour ClickHouse et ZooKeeper/Keeper, ou de placer les fichiers de ClickHouse et ceux de Keeper sur des disques distincts. En effet, ZooKeeper/Keeper sont très sensibles à la latence disque et ClickHouse peut utiliser toutes les ressources système disponibles.

Il est possible d’avoir des observateurs ZooKeeper dans un ensemble, mais les serveurs ClickHouse ne doivent pas interagir avec eux.

Ne modifiez pas le paramètre `minSessionTimeout` : des valeurs élevées peuvent affecter la stabilité des redémarrages de ClickHouse.

Avec les paramètres par défaut, ZooKeeper est une bombe à retardement :

> Le serveur ZooKeeper ne supprimera pas les fichiers des anciens snapshots et logs lorsqu’il utilise la configuration par défaut (voir `autopurge`) ; cela relève de la responsabilité de l’opérateur.

Cette bombe doit être désamorcée.

La configuration ZooKeeper (3.5.1) ci-dessous est utilisée dans un grand environnement de production :

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

Version de Java :

```text
openjdk 11.0.5-shenandoah 2019-10-15
OpenJDK Runtime Environment (build 11.0.5-shenandoah+10-adhoc.heretic.src)
OpenJDK 64-Bit Server VM (build 11.0.5-shenandoah+10-adhoc.heretic.src, mixed mode)
```

Paramètres JVM :

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

Initialisation de Salt :

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
  ## Antivirus
</div>

Si vous utilisez un antivirus, configurez-le pour qu’il ignore les dossiers contenant les fichiers de données de ClickHouse (`/var/lib/clickhouse`), sinon les performances risquent d’être réduites et vous pouvez rencontrer des erreurs inattendues lors de l’ingestion de données et des fusions d’arrière-plan.

<div id="related-content">
  ## Contenu associé
</div>

* [Premiers pas avec ClickHouse ? Voici 13 « péchés capitaux » et comment les éviter](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse)