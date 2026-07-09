---
description: 'Documentation sur la mise à niveau'
sidebar_title: 'Mise à niveau en autogestion'
slug: /operations/update
title: 'Mise à niveau en autogestion'
doc_type: 'guide'
---

<div id="clickhouse-upgrade-overview">
  ## Aperçu de la mise à niveau de ClickHouse
</div>

Ce document contient :

* des consignes générales
* un plan recommandé
* des informations spécifiques sur la mise à niveau des binaires sur vos systèmes

<div id="general-guidelines">
  ## Consignes générales
</div>

Ces remarques devraient vous aider à planifier et à comprendre pourquoi nous formulons les recommandations présentées plus loin dans ce document.

<div id="upgrade-clickhouse-server-separately-from-clickhouse-keeper-or-zookeeper">
  ### Mettre à niveau serveur ClickHouse séparément de ClickHouse Keeper ou ZooKeeper
</div>

Sauf si un correctif de sécurité est nécessaire pour ClickHouse Keeper ou Apache ZooKeeper, il n’est pas nécessaire de mettre à niveau Keeper lorsque vous mettez à niveau serveur ClickHouse. La stabilité de Keeper étant indispensable pendant le processus de mise à niveau, terminez les mises à niveau de serveur ClickHouse avant d’envisager celle de Keeper.

<div id="minor-version-upgrades-should-be-adopted-often">
  ### Les mises à niveau mineures doivent être effectuées régulièrement
</div>

Il est fortement recommandé de toujours effectuer la mise à niveau vers la dernière version mineure dès sa publication. Les versions mineures n&#39;introduisent pas de changements incompatibles, mais elles contiennent des corrections de bogues importantes (et peuvent inclure des correctifs de sécurité).

<div id="test-experimental-features-on-a-separate-clickhouse-server-running-the-target-version">
  ### Tester les fonctionnalités expérimentales sur un serveur ClickHouse distinct exécutant la version cible
</div>

La compatibilité des fonctionnalités expérimentales peut être rompue à tout moment, de quelque manière que ce soit. Si vous utilisez des fonctionnalités expérimentales, consultez les changelogs et envisagez de mettre en place un serveur ClickHouse distinct avec la version cible installée afin d’y tester votre usage de ces fonctionnalités.

<div id="downgrades">
  ### Retours à une version antérieure
</div>

Si vous effectuez une mise à niveau et constatez ensuite que la nouvelle version n’est pas compatible avec une fonctionnalité dont vous dépendez, il peut être possible de revenir à une version récente (âgée de moins d’un an), à condition de ne pas avoir commencé à utiliser l’une des nouvelles fonctionnalités. Une fois ces nouvelles fonctionnalités utilisées, le retour à une version antérieure ne fonctionnera plus.

<div id="multiple-clickhouse-server-versions-in-a-cluster">
  ### Plusieurs versions de serveur ClickHouse dans un cluster
</div>

Nous nous efforçons de maintenir une fenêtre de compatibilité d’un an (qui inclut 2 versions LTS). Cela signifie que deux versions quelconques doivent pouvoir fonctionner ensemble dans un cluster si l’écart entre elles est inférieur à un an (ou s’il y a moins de deux versions LTS entre elles). Cependant, il est recommandé de mettre à niveau tous les membres d’un cluster vers la même version le plus rapidement possible, car quelques problèmes mineurs peuvent survenir (comme un ralentissement des requêtes distribuées, des erreurs récupérables dans certaines opérations en arrière-plan de ReplicatedMergeTree, etc.).

Nous ne recommandons jamais d’exécuter différentes versions dans le même cluster lorsque les dates de publication sont espacées de plus d’un an. Bien que nous ne nous attendions pas à une perte de données, le cluster peut devenir inutilisable. Voici les problèmes auxquels vous devez vous attendre si l’écart entre les versions dépasse un an :

* le cluster peut ne pas fonctionner
* certaines requêtes (voire toutes) peuvent échouer avec des erreurs arbitraires
* des erreurs/avertissements arbitraires peuvent apparaître dans les logs
* il peut être impossible d’effectuer un retour à une version antérieure

<div id="incremental-upgrades">
  ### Mises à niveau par paliers
</div>

Si l’écart entre la version actuelle et la version cible dépasse un an, il est recommandé de procéder de l’une des manières suivantes :

* Effectuer une mise à niveau avec interruption de service (arrêter tous les serveurs, mettre à niveau tous les serveurs, puis redémarrer tous les serveurs).
* Ou effectuer la mise à niveau en passant par une version intermédiaire (une version publiée moins d’un an après la version actuelle).

<div id="recommended-plan">
  ## Plan recommandé
</div>

Voici les étapes recommandées pour effectuer une mise à niveau de ClickHouse sans interruption de service :

1. Assurez-vous que vos modifications de configuration ne se trouvent pas dans le fichier par défaut `/etc/clickhouse-server/config.xml`, mais dans `/etc/clickhouse-server/config.d/`, car `/etc/clickhouse-server/config.xml` peut être écrasé lors d&#39;une mise à niveau.
2. Consultez les [changelogs](/fr/whats-new/changelog/index.md) afin d&#39;identifier les changements incompatibles (en remontant de la version cible vers la version que vous utilisez actuellement).
3. Appliquez toutes les mises à jour indiquées dans les changements incompatibles qui peuvent l&#39;être avant la mise à niveau, et dressez la liste des modifications à effectuer après la mise à niveau.
4. Identifiez une ou plusieurs répliques pour chaque segment afin de les maintenir en service pendant la mise à niveau des autres répliques de chaque segment.
5. Sur les répliques à mettre à niveau, une par une :

* arrêtez le serveur ClickHouse
* mettez le serveur à niveau vers la version cible
* redémarrez le serveur ClickHouse
* attendez que les messages de Keeper indiquent que le système est stable
* passez à la réplique suivante6. Vérifiez la présence d&#39;erreurs dans le journal de Keeper et le journal de ClickHouse

7. Mettez à niveau les répliques identifiées à l&#39;étape 4 vers la nouvelle version
8. Reportez-vous à la liste des modifications des étapes 1 à 3 et appliquez celles qui doivent l&#39;être après la mise à niveau.

:::note
Ce message d&#39;erreur est normal lorsque plusieurs versions de ClickHouse s&#39;exécutent dans un environnement répliqué. Il disparaîtra une fois que toutes les répliques auront été mises à niveau vers la même version.

```text
MergeFromLogEntryTask: Code: 40. DB::Exception: Checksums of parts don't match:
hash of uncompressed files doesn't match. (CHECKSUM_DOESNT_MATCH)  Data after merge is not
byte-identical to data on another replicas.
```

:::

<div id="clickhouse-server-binary-upgrade-process">
  ## Procédure de mise à niveau du binaire du serveur ClickHouse
</div>

Si ClickHouse a été installé à partir de paquets `deb`, exécutez les commandes suivantes sur le serveur :

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Si vous avez installé ClickHouse autrement qu’avec les paquets `deb` recommandés, utilisez la méthode de mise à jour appropriée.

:::note
Vous pouvez mettre à jour plusieurs serveurs à la fois, à condition qu’il n’y ait jamais de moment où toutes les répliques d’un segment sont hors ligne.
:::

Mise à niveau d’une ancienne version de ClickHouse vers une version spécifique :

Par exemple :

`xx.yy.a.b` est une version stable actuelle. La dernière version stable est disponible [ici](https://github.com/ClickHouse/ClickHouse/releases)

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```