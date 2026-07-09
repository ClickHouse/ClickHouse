---
description: 'Présentation de la réplication des données avec la famille de moteurs de table Replicated* dans ClickHouse'
sidebar_label: 'Replicated*'
sidebar_position: 20
slug: /engines/table-engines/mergetree-family/replication
title: 'Moteurs de table Replicated*'
doc_type: 'reference'
---

:::note
Dans ClickHouse Cloud, la réplication est gérée pour vous. Veuillez créer vos tables sans ajouter d’arguments. Par exemple, dans le texte ci-dessous, vous remplacerez :

```sql
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/table_name',
    '{replica}'
)
```

avec :

```sql
ENGINE = ReplicatedMergeTree
```

:::

La réplication n&#39;est prise en charge que pour les tables de la famille MergeTree

* ReplicatedSummingMergeTree
* ReplicatedCoalescingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedGraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree

La réplication fonctionne au niveau de chaque table, et non de l&#39;ensemble du serveur. Un serveur peut stocker simultanément des tables répliquées et des tables non répliquées.

La réplication ne dépend pas du sharding. Chaque shard dispose de sa propre réplication, indépendante.

Les données compressées des requêtes `INSERT` et `ALTER` sont répliquées (pour plus d&#39;informations, consultez la documentation de [ALTER](/fr/sql-reference/statements/alter).

Les requêtes `CREATE`, `DROP`, `ATTACH`, `DETACH` et `RENAME` sont exécutées sur un seul serveur et ne sont pas répliquées :

* La requête `CREATE TABLE` crée une nouvelle table pouvant être répliquée sur le serveur où elle est exécutée. Si cette table existe déjà sur d&#39;autres serveurs, une nouvelle réplique est ajoutée.
* La requête `DROP TABLE` supprime la réplique située sur le serveur où la requête est exécutée.
* La requête `RENAME` renomme la table sur l&#39;une des répliques. Autrement dit, les tables répliquées peuvent porter des noms différents selon les répliques.

ClickHouse utilise [ClickHouse Keeper](/fr/guides/sre/keeper/index.md) pour stocker les métadonnées des répliques. Il est possible d&#39;utiliser ZooKeeper version 3.4.5 ou ultérieure, mais ClickHouse Keeper est recommandé.

Pour utiliser la réplication, définissez les paramètres dans la section [zookeeper](/fr/operations/server-configuration-parameters/settings#zookeeper) de la configuration du serveur.

:::note
Ne négligez pas le paramètre de sécurité. ClickHouse prend en charge le schéma [ACL](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) `digest` du sous-système de sécurité de ZooKeeper.
:::

Exemple de configuration des adresses du cluster ClickHouse Keeper :

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <node>
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

ClickHouse prend également en charge le stockage des méta-informations des répliques dans un cluster ZooKeeper auxiliaire. Pour ce faire, indiquez le nom et le chemin du cluster ZooKeeper comme arguments du moteur.
Autrement dit, il permet de stocker les métadonnées de différentes tables dans différents clusters ZooKeeper.

Exemple de définition des adresses du cluster ZooKeeper auxiliaire :

```xml
<auxiliary_zookeepers>
    <zookeeper2>
        <node>
            <host>example_2_1</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_2</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_3</host>
            <port>2181</port>
        </node>
    </zookeeper2>
    <zookeeper3>
        <node>
            <host>example_3_1</host>
            <port>2181</port>
        </node>
    </zookeeper3>
</auxiliary_zookeepers>
```

Pour stocker les métadonnées de la table dans un cluster ZooKeeper auxiliaire plutôt que dans le cluster ZooKeeper par défaut, vous pouvez utiliser SQL pour créer la table avec le
moteur ReplicatedMergeTree comme suit :

```sql
CREATE TABLE table_name ( ... ) ENGINE = ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'replica_name') ...
```

Vous pouvez spécifier n’importe quel cluster ZooKeeper existant, et le système utilisera un répertoire dessus pour ses propres données (le répertoire est spécifié lors de la création d’une table pouvant être répliquée).

Si ZooKeeper n’est pas défini dans le fichier de configuration, vous ne pouvez pas créer de tables répliquées, et toutes les tables répliquées existantes seront en lecture seule.

ZooKeeper n&#39;est pas utilisé pour les requêtes `SELECT`, car la réplication n&#39;affecte pas les performances de `SELECT` et ces requêtes s&#39;exécutent aussi rapidement que sur des tables non répliquées. Lors de l&#39;interrogation de tables distribuées répliquées, le comportement de ClickHouse est contrôlé par les paramètres [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](/fr/operations/settings/settings.md/#max_replica_delay_for_distributed_queries) et [fallback&#95;to&#95;stale&#95;replicas&#95;for&#95;distributed&#95;queries](/fr/operations/settings/settings.md/#fallback_to_stale_replicas_for_distributed_queries).

Pour chaque requête `INSERT`, environ dix entrées sont ajoutées à ZooKeeper via plusieurs transactions. (Plus précisément, cela s&#39;applique à chaque bloc de données inséré ; une requête INSERT contient un bloc, ou un bloc par tranche de `max_insert_block_size = 1048576` lignes.) Cela entraîne des latences légèrement plus élevées pour `INSERT` que pour les tables non répliquées. Mais si vous suivez les recommandations et insérez les données par lots, à raison de pas plus d&#39;un `INSERT` par seconde, cela ne pose aucun problème. L&#39;ensemble du cluster ClickHouse utilisant un cluster ZooKeeper pour la coordination peut traiter au total plusieurs centaines d&#39;`INSERTs` par seconde. Le débit des insertions de données (le nombre de lignes par seconde) est tout aussi élevé que pour des données non répliquées.

Pour les très grands clusters, vous pouvez utiliser différents clusters ZooKeeper pour différents shards. Cependant, d&#39;après notre expérience, cela ne s&#39;est pas avéré nécessaire, y compris sur des clusters de production comptant environ 300 serveurs.

La réplication est asynchrone et multi-maître. Les requêtes `INSERT` (ainsi que `ALTER`) peuvent être envoyées à n&#39;importe quel serveur disponible. Les données sont insérées sur le serveur où la requête est exécutée, puis copiées vers les autres serveurs. Comme la réplication est asynchrone, les données récemment insérées apparaissent sur les autres répliques avec une certaine latence. Si une partie des répliques n&#39;est pas disponible, les données sont écrites lorsqu&#39;elles redeviennent disponibles. Si une réplique est disponible, la latence correspond au temps nécessaire pour transférer le bloc de données compressées sur le réseau. Le nombre de threads effectuant des tâches en arrière-plan pour les tables répliquées peut être défini par le paramètre [background&#95;schedule&#95;pool&#95;size](/fr/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size).

Le moteur `ReplicatedMergeTree` utilise un pool de threads distinct pour les replicated fetches. La taille du pool est limitée par le paramètre [background&#95;fetches&#95;pool&#95;size](/fr/operations/server-configuration-parameters/settings#background_fetches_pool_size), qui peut être ajusté après un redémarrage du serveur.

Par défaut, une requête `INSERT` attend la confirmation de l&#39;écriture des données d&#39;une seule réplique. Si les données n&#39;ont été écrites avec succès que sur une seule réplique et que le serveur hébergeant cette réplique cesse d&#39;exister, les données stockées seront perdues. Pour obtenir une confirmation d&#39;écriture des données depuis plusieurs répliques, utilisez l&#39;option `insert_quorum`.

Chaque bloc de données est écrit de manière atomique. La requête `INSERT` est divisée en blocs allant jusqu&#39;à `max_insert_block_size = 1048576` lignes. En d&#39;autres termes, si la requête `INSERT` contient moins de 1048576 lignes, elle est effectuée de manière atomique.

Les blocs de données sont dédupliqués. En cas d&#39;écritures multiples du même bloc de données (blocs de données de même taille contenant les mêmes lignes dans le même ordre), le bloc n&#39;est écrit qu&#39;une seule fois. Cela s&#39;explique par le fait qu&#39;en cas de défaillance réseau, l&#39;application cliente ne sait pas si les données ont été écrites dans la base de données ; la requête `INSERT` peut donc simplement être répétée. Peu importe à quelle réplique les `INSERTs` ont été envoyés avec des données identiques. Les `INSERTs` sont idempotents. Les paramètres de déduplication sont contrôlés par les paramètres du serveur [merge&#95;tree](/fr/operations/server-configuration-parameters/settings.md/#merge_tree).

Pendant la réplication, seules les données sources à insérer sont transférées sur le réseau. Les transformations ultérieures des données (fusion) sont coordonnées et effectuées de la même manière sur toutes les répliques. Cela minimise l&#39;utilisation du réseau, ce qui signifie que la réplication fonctionne bien lorsque les répliques se trouvent dans différents datacenters. (Notez que la duplication des données dans différents datacenters est l&#39;objectif principal de la réplication.)

Vous pouvez avoir n&#39;importe quel nombre de répliques des mêmes données. D&#39;après notre expérience, une solution relativement fiable et pratique consiste à utiliser une double réplication en production, chaque serveur utilisant RAID-5 ou RAID-6 (et RAID-10 dans certains cas).

Le système surveille la synchronisation des données sur les répliques et est capable de se rétablir après une défaillance. Le basculement est automatique (pour de faibles écarts de données) ou semi-automatique (lorsque les données diffèrent trop, ce qui peut indiquer une erreur de configuration).

<div id="creating-replicated-tables">
  ## Création de tables répliquées
</div>

:::note
Dans ClickHouse Cloud, la réplication est gérée automatiquement.

Créez des tables avec [`MergeTree`](/fr/engines/table-engines/mergetree-family/mergetree), sans argument de réplication. Le système convertit en interne [`MergeTree`](/fr/engines/table-engines/mergetree-family/mergetree) en [`SharedMergeTree`](/fr/cloud/reference/shared-merge-tree) pour assurer la réplication et la distribution des données.

Évitez d’utiliser `ReplicatedMergeTree` ou de spécifier des paramètres de réplication, car celle-ci est gérée par la plateforme.

:::

<div id="replicatedmergetree-parameters">
  ### Paramètres de Replicated*MergeTree
</div>

| Paramètre          | Description                                                                                                          |
| ------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `zoo_path`         | Le chemin de la table dans ClickHouse Keeper.                                                                        |
| `replica_name`     | Le nom de la réplique dans ClickHouse Keeper.                                                                        |
| `other_parameters` | Les paramètres du moteur utilisés pour créer la version répliquée, par exemple la version dans `ReplacingMergeTree`. |

Exemple :

```sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
```

<details markdown="1">
  <summary>Exemple avec une syntaxe obsolète</summary>

  ```sql
  CREATE TABLE table_name
  (
      EventDate DateTime,
      CounterID UInt32,
      UserID UInt32
  ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192);
  ```
</details>

Comme le montre l’exemple, ces paramètres peuvent contenir des substitutions entre `{}`. Les valeurs substituées proviennent de la section [macros](/fr/operations/server-configuration-parameters/settings.md/#macros) du fichier de configuration.

Exemple :

```xml
<macros>
    <shard>02</shard>
    <replica>example05-02-1</replica>
</macros>
```

Le chemin vers la table dans ClickHouse Keeper doit être unique pour chaque table répliquée. Les tables sur des shards différents doivent avoir des chemins différents.
Dans ce cas, le chemin se compose des parties suivantes :

`/clickhouse/tables/` est le préfixe commun. Nous recommandons d’utiliser exactement celui-ci.

`{shard}` sera remplacé par l’identifiant du shard.

`table_name` est le nom du nœud de la table dans ClickHouse Keeper. Il est préférable de lui donner le même nom que la table. Il est défini explicitement, car, contrairement au nom de la table, il ne change pas après une requête RENAME.
*ASTUCE* : vous pouvez également ajouter un nom de base de données devant `table_name`. Par exemple : `db_name.table_name`

Les deux substitutions intégrées `{database}` et `{table}` peuvent être utilisées ; elles se développent respectivement en nom de table et en nom de base de données (sauf si ces macros sont définies dans la section `macros`). Le chemin ZooKeeper peut donc être spécifié comme `'/clickhouse/tables/{shard}/{database}/{table}'`.
Soyez prudent lors du renommage des tables si vous utilisez ces substitutions intégrées. Le chemin dans ClickHouse Keeper ne peut pas être modifié et, lorsque la table est renommée, les macros se développent vers un autre chemin ; la table fera alors référence à un chemin qui n’existe pas dans ClickHouse Keeper et passera en mode lecture seule.

Le nom de la réplique identifie les différentes répliques d’une même table. Vous pouvez utiliser le nom du serveur à cette fin, comme dans l’exemple. Le nom doit seulement être unique au sein de chaque shard.

Vous pouvez définir les paramètres explicitement au lieu d’utiliser des substitutions. Cela peut être pratique pour les tests et pour configurer de petits clusters. Cependant, vous ne pouvez pas utiliser de requêtes DDL distribuées (`ON CLUSTER`) dans ce cas.

Lorsque vous travaillez avec de grands clusters, nous recommandons d’utiliser des substitutions, car elles réduisent le risque d’erreur.

Vous pouvez spécifier des arguments par défaut pour le moteur de table `Replicated` dans le fichier de configuration du serveur. Par exemple :

```xml
<default_replica_path>/clickhouse/tables/{shard}/{database}/{table}</default_replica_path>
<default_replica_name>{replica}</default_replica_name>
```

Dans ce cas, vous pouvez ne pas spécifier d’arguments lors de la création des tables :

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree
ORDER BY x;
```

Cela équivaut à :

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
ORDER BY x;
```

Exécutez la requête `CREATE TABLE` sur chaque réplique. Cette requête crée une nouvelle table répliquée ou ajoute une nouvelle réplique à une table existante.

Si vous ajoutez une nouvelle réplique alors que la table contient déjà des données sur d&#39;autres répliques, ces données seront copiées depuis les autres répliques vers la nouvelle une fois la requête exécutée. En d&#39;autres termes, la nouvelle réplique se synchronise avec les autres.

Pour supprimer une réplique, exécutez `DROP TABLE`. Cependant, une seule réplique est supprimée : celle qui se trouve sur le serveur où vous exécutez la requête.

<div id="recovery-after-failures">
  ## Récupération après des pannes
</div>

Si ClickHouse Keeper est indisponible au démarrage d’un serveur, les tables répliquées passent en mode lecture seule. Le système tente périodiquement de se reconnecter à ClickHouse Keeper.

Si ClickHouse Keeper est indisponible pendant un `INSERT`, ou si une erreur survient lors d’une interaction avec ClickHouse Keeper, une exception est levée.

Après s’être connecté à ClickHouse Keeper, le système vérifie si l’ensemble des données du système de fichiers local correspond à l’ensemble de données attendu (ces informations sont stockées dans ClickHouse Keeper). En cas de légères incohérences, le système les corrige en synchronisant les données avec les répliques.

Si le système détecte des data parts corrompues (avec une taille de fichier incorrecte) ou des parts non reconnues (des parts écrites dans le système de fichiers mais non enregistrées dans ClickHouse Keeper), il les déplace vers le sous-répertoire `detached` (elles ne sont pas supprimées). Les parts manquantes sont copiées depuis les répliques.

Notez que ClickHouse n’effectue aucune action destructive, comme la suppression automatique d’une grande quantité de données.

Lorsque le serveur démarre (ou établit une nouvelle session avec ClickHouse Keeper), il vérifie uniquement le nombre et la taille de tous les fichiers. Si les tailles de fichier correspondent mais que des octets ont été modifiés quelque part au milieu, cela n’est pas détecté immédiatement, mais seulement lorsqu’une tentative de lecture des données est effectuée pour une requête `SELECT`. La requête lève une exception signalant une somme de contrôle non concordante ou une taille incorrecte d’un block compressé. Dans ce cas, les data parts sont ajoutées à la file de vérification et copiées depuis les répliques si nécessaire.

Si l’ensemble local de données diffère trop de l’ensemble attendu, un mécanisme de sécurité se déclenche. Le serveur en consigne l’événement dans le journal et refuse de démarrer. Cela s’explique par le fait que cette situation peut indiquer une erreur de configuration, par exemple si une réplique d’un shard a été accidentellement configurée comme une réplique d’un autre shard. Toutefois, les seuils de ce mécanisme sont réglés assez bas, et cette situation peut aussi se produire lors d’une récupération normale après incident. Dans ce cas, les données sont restaurées de manière semi-automatique, en « appuyant sur un bouton ».

Pour démarrer la récupération, créez le nœud `/path_to_table/replica_name/flags/force_restore_data` dans ClickHouse Keeper avec n’importe quel contenu, ou exécutez la commande permettant de restaurer toutes les tables répliquées :

```bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

Redémarrez ensuite le serveur. Au démarrage, le serveur supprime ces indicateurs et lance la procédure de récupération.

<div id="recovery-after-complete-data-loss">
  ## Récupération après une perte complète des données
</div>

Si toutes les données et métadonnées ont disparu d’un des serveurs, suivez ces étapes pour effectuer la récupération :

1. Installez ClickHouse sur le serveur. Définissez correctement les substitutions dans le fichier de configuration contenant l’identifiant du shard et les répliques, si vous les utilisez.
2. Si vous aviez des tables non répliquées qui doivent être dupliquées manuellement sur les serveurs, copiez leurs données depuis une réplique (dans le répertoire `/var/lib/clickhouse/data/db_name/table_name/`).
3. Copiez les définitions de table situées dans `/var/lib/clickhouse/metadata/` depuis une réplique. Si un identifiant de shard ou de réplique est défini explicitement dans les définitions de table, corrigez-le pour qu’il corresponde à cette réplique. (Sinon, démarrez le serveur et exécutez toutes les requêtes `ATTACH TABLE` qui auraient dû se trouver dans les fichiers .sql de `/var/lib/clickhouse/metadata/`.)
4. Pour lancer la récupération, créez le nœud ClickHouse Keeper `/path_to_table/replica_name/flags/force_restore_data` avec n’importe quel contenu, ou exécutez la commande suivante pour restaurer toutes les tables répliquées : `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

Démarrez ensuite le serveur (ou redémarrez-le s’il est déjà en cours de fonctionnement). Les données seront téléchargées depuis les répliques.

Une autre option consiste à supprimer de ClickHouse Keeper les informations relatives à la réplique perdue (`/path_to_table/replica_name`), puis à recréer la réplique comme décrit dans « [Création de tables répliquées](#creating-replicated-tables) ».

Aucune limitation du débit réseau n’est appliquée pendant la récupération. Gardez-le à l’esprit si vous restaurez de nombreuses répliques en même temps.

<div id="converting-from-mergetree-to-replicatedmergetree">
  ## Conversion de MergeTree vers ReplicatedMergeTree
</div>

Nous utilisons le terme `MergeTree` pour désigner tous les moteurs de table de la famille `MergeTree`, comme pour `ReplicatedMergeTree`.

Si vous avez une table `MergeTree` répliquée manuellement, vous pouvez la convertir en table répliquée. Cela peut être nécessaire si vous avez déjà accumulé une grande quantité de données dans une table `MergeTree` et souhaitez désormais activer la réplication.

L’instruction [ATTACH TABLE ... AS REPLICATED](/fr/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) permet d’attacher une table `MergeTree` détachée en tant que `ReplicatedMergeTree`.

Une table `MergeTree` peut être convertie automatiquement au redémarrage du serveur si l’indicateur `convert_to_replicated` est défini dans le répertoire de données de la table (`/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/` pour la base de données `Atomic`).
Créez un fichier vide `convert_to_replicated` et la table sera chargée en tant que table répliquée au prochain redémarrage du serveur.

Cette requête permet d’obtenir le chemin des données de la table. Si la table comporte plusieurs chemins de données, vous devez utiliser le premier.

```sql
SELECT data_paths FROM system.tables WHERE table = 'table_name' AND database = 'database_name';
```

Notez que la table ReplicatedMergeTree sera créée avec les valeurs des paramètres `default_replica_path` et `default_replica_name`.
Pour créer une table convertie sur les autres répliques, vous devrez spécifier explicitement son chemin dans le premier argument du moteur `ReplicatedMergeTree`. La requête suivante permet d&#39;obtenir ce chemin.

```sql
SELECT zookeeper_path FROM system.replicas WHERE table = 'table_name';
```

Il existe également une méthode manuelle pour effectuer cette opération.

Si les données diffèrent d&#39;une réplique à l&#39;autre, synchronisez-les d&#39;abord ou supprimez ces données sur toutes les répliques sauf une.

Renommez la table MergeTree existante, puis créez une table `ReplicatedMergeTree` avec son ancien nom.
Déplacez les données de l&#39;ancienne table vers le sous-répertoire `detached` du répertoire contenant les données de la nouvelle table (`/var/lib/clickhouse/data/db_name/table_name/`).
Exécutez ensuite `ALTER TABLE ATTACH PARTITION` sur l&#39;une des répliques pour ajouter ces data parts à l&#39;ensemble actif.

<div id="converting-from-replicatedmergetree-to-mergetree">
  ## Conversion de ReplicatedMergeTree en MergeTree
</div>

Utilisez l’instruction [ATTACH TABLE ... AS NOT REPLICATED](/fr/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) pour rattacher une table `ReplicatedMergeTree` détachée en tant que table `MergeTree` sur un seul serveur.

Une autre méthode consiste à redémarrer le serveur. Créez une table `MergeTree` sous un autre nom. Déplacez toutes les données du répertoire contenant les données de la table `ReplicatedMergeTree` vers le répertoire de données de la nouvelle table. Supprimez ensuite la table `ReplicatedMergeTree`, puis redémarrez le serveur.

Si vous souhaitez supprimer une table `ReplicatedMergeTree` sans démarrer le serveur :

* Supprimez le fichier `.sql` correspondant dans le répertoire des métadonnées (`/var/lib/clickhouse/metadata/`).
* Supprimez le chemin correspondant dans ClickHouse Keeper (`/path_to_table/replica_name`).

Après cela, vous pouvez démarrer le serveur, créer une table `MergeTree`, déplacer les données vers son répertoire, puis redémarrer le serveur.

<div id="recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged">
  ## Récupération lorsque les métadonnées du cluster ClickHouse Keeper sont perdues ou endommagées
</div>

Si les données de ClickHouse Keeper ont été perdues ou endommagées, vous pouvez les préserver en les déplaçant vers une table non répliquée, comme décrit ci-dessus.

**Voir aussi**

* [background&#95;schedule&#95;pool&#95;size](/fr/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size)
* [background&#95;fetches&#95;pool&#95;size](/fr/operations/server-configuration-parameters/settings.md/#background_fetches_pool_size)
* [execute&#95;merges&#95;on&#95;single&#95;replica&#95;time&#95;threshold](/fr/operations/settings/merge-tree-settings#execute_merges_on_single_replica_time_threshold)
* [max&#95;replicated&#95;fetches&#95;network&#95;bandwidth](/fr/operations/settings/merge-tree-settings.md/#max_replicated_fetches_network_bandwidth)
* [max&#95;replicated&#95;sends&#95;network&#95;bandwidth](/fr/operations/settings/merge-tree-settings.md/#max_replicated_sends_network_bandwidth)