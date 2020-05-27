---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: "La Sauvegarde Des Donn\xE9es"
---

# La Sauvegarde Des Données {#data-backup}

Alors [réplication](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not protect against human errors: accidental deletion of data, deletion of the wrong table or a table on the wrong cluster, and software bugs that result in incorrect data processing or data corruption. In many cases mistakes like these will affect all replicas. ClickHouse has built-in safeguards to prevent some types of mistakes — for example, by default [vous ne pouvez pas simplement supprimer des tables avec un moteur de type MergeTree contenant plus de 50 Go de données](https://github.com/ClickHouse/ClickHouse/blob/v18.14.18-stable/programs/server/config.xml#L322-L330). Toutefois, ces garanties ne couvrent pas tous les cas possibles et peuvent être contournés.

Afin d'atténuer efficacement les erreurs humaines possibles, vous devez préparer soigneusement une stratégie de sauvegarde et de restauration de vos données **préalablement**.

Chaque entreprise a différentes ressources disponibles et les exigences de l'entreprise, donc il n'y a pas de solution universelle pour les sauvegardes et restaurations ClickHouse qui s'adaptera à toutes les situations. Ce qui fonctionne pour un gigaoctet de données ne fonctionnera probablement pas pour des dizaines de pétaoctets. Il existe une variété d'approches possibles avec leurs propres avantages et inconvénients, qui sera discuté ci-dessous. C'est une bonne idée d'utiliser plusieurs approches au lieu d'un seul, afin de compenser leurs lacunes.

!!! note "Note"
    Gardez à l'esprit que si vous avez sauvegardé quelque chose et que vous n'avez jamais essayé de le restaurer, il est probable que la restauration ne fonctionnera pas correctement lorsque vous en avez réellement besoin (ou du moins cela prendra plus de temps que ce que les entreprises peuvent tolérer). Donc, quelle que soit l'approche de sauvegarde que vous choisissez, assurez-vous d'automatiser le processus de restauration et de le pratiquer sur un cluster clickhouse de rechange régulièrement.

## Dupliquer Les Données Sources Ailleurs {#duplicating-source-data-somewhere-else}

Souvent, les données qui sont ingérées dans ClickHouse sont livrées via une sorte de file d'attente persistante, telle que [Apache Kafka](https://kafka.apache.org). Dans ce cas, il est possible de configurer un ensemble supplémentaire d'abonnés qui liront le même flux de données pendant qu'il est écrit dans ClickHouse et le stockeront dans un stockage à froid quelque part. La plupart des entreprises ont déjà un stockage à froid recommandé par défaut, qui pourrait être un magasin d'objets ou un système de fichiers distribué comme [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

## Instantanés Du Système De Fichiers {#filesystem-snapshots}

Certains systèmes de fichiers locaux fournissent des fonctionnalités d'instantané (par exemple, [ZFS](https://en.wikipedia.org/wiki/ZFS)), mais ils pourraient ne pas être le meilleur choix pour servir les requêtes actives. Une solution possible consiste à créer des répliques supplémentaires avec ce type de système de fichiers et à les exclure du [Distribué](../engines/table-engines/special/distributed.md) les tables qui sont utilisés pour `SELECT` requête. Les instantanés sur ces répliques seront hors de portée des requêtes qui modifient les données. En prime, ces répliques pourraient avoir des configurations matérielles spéciales avec plus de disques attachés par serveur, ce qui serait rentable.

## clickhouse-copieur {#clickhouse-copier}

[clickhouse-copieur](utilities/clickhouse-copier.md) est un outil polyvalent qui a été initialement créé pour re-shard petaoctet - sized tables. Il peut également être utilisé à des fins de sauvegarde et de restauration car il copie de manière fiable les données entre les tables ClickHouse et les clusters.

Pour de plus petits volumes de données, un simple `INSERT INTO ... SELECT ...` les tables distantes peuvent également fonctionner.

## Manipulations avec des pièces {#manipulations-with-parts}

ClickHouse permet d'utiliser le `ALTER TABLE ... FREEZE PARTITION ...` requête pour créer une copie locale des partitions de table. Ceci est implémenté en utilisant des liens durs vers le `/var/lib/clickhouse/shadow/` dossier, donc il ne consomme généralement pas d'espace disque supplémentaire pour les anciennes données. Les copies créées des fichiers ne sont pas gérées par clickhouse server, vous pouvez donc les laisser là: vous aurez une sauvegarde simple qui ne nécessite aucun système externe supplémentaire, mais elle sera toujours sujette à des problèmes matériels. Pour cette raison, il est préférable de les copier à distance vers un autre emplacement, puis de supprimer les copies locales. Les systèmes de fichiers distribués et les magasins d'objets sont toujours une bonne option pour cela, mais les serveurs de fichiers attachés normaux avec une capacité suffisante peuvent également fonctionner (dans ce cas, le transfert se fera via le système de fichiers réseau ou peut-être [rsync](https://en.wikipedia.org/wiki/Rsync)).

Pour plus d'informations sur les requêtes liées aux manipulations de [Modifier la documentation](../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

Un outil tiers est disponible pour automatiser cette approche: [clickhouse-sauvegarde](https://github.com/AlexAkulov/clickhouse-backup).

[Article Original](https://clickhouse.tech/docs/en/operations/backup/) <!--hide-->
