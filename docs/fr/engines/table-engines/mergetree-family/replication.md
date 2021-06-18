---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: "R\xE9plication Des Donn\xE9es"
---

# Réplication Des Données {#table_engines-replication}

La réplication n'est prise en charge que pour les tables de la famille MergeTree:

-   ReplicatedMergeTree
-   ReplicatedSummingMergeTree
-   ReplicatedReplacingMergeTree
-   ReplicatedAggregatingMergeTree
-   ReplicatedCollapsingMergeTree
-   ReplicatedVersionedCollapsingMergetree
-   ReplicatedGraphiteMergeTree

La réplication fonctionne au niveau d'une table individuelle, Pas du serveur entier. Un serveur peut stocker des tables répliquées et non répliquées en même temps.

La réplication ne dépend pas de la fragmentation. Chaque fragment a sa propre réplication indépendante.

Données compressées pour `INSERT` et `ALTER` les requêtes sont répliquées (pour plus d'informations, consultez la documentation de [ALTER](../../../sql-reference/statements/alter.md#query_language_queries_alter)).

`CREATE`, `DROP`, `ATTACH`, `DETACH` et `RENAME` les requêtes sont exécutées sur un seul serveur et ne sont pas répliquées:

-   Le `CREATE TABLE` query crée une nouvelle table réplicable sur le serveur où la requête est exécutée. Si cette table existe déjà sur d'autres serveurs, il ajoute une nouvelle réplique.
-   Le `DROP TABLE` requête supprime la réplique situé sur le serveur où l'exécution de la requête.
-   Le `RENAME` requête renomme la table sur l'une des répliques. En d'autres termes, les tables répliquées peuvent avoir des noms différents sur différentes répliques.

Clickhouse utilise [Apache ZooKeeper](https://zookeeper.apache.org) pour stocker des informations méta répliques. Utilisez ZooKeeper version 3.4.5 ou plus récente.

Pour utiliser la réplication, définissez les paramètres [zookeeper](../../../operations/server-configuration-parameters/settings.md#server-settings_zookeeper) section de configuration du serveur.

!!! attention "Attention"
    Ne négligez pas la sécurité. Clickhouse soutient le `digest` [Schéma ACL](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) du sous-système de sécurité ZooKeeper.

Exemple de définition des adresses du cluster ZooKeeper:

``` xml
<zookeeper>
    <node index="1">
        <host>example1</host>
        <port>2181</port>
    </node>
    <node index="2">
        <host>example2</host>
        <port>2181</port>
    </node>
    <node index="3">
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

Vous pouvez spécifier N'importe quel cluster Zookeeper existant et le système utilisera un répertoire pour ses propres données (le répertoire est spécifié lors de la création d'une table réplicable).

Si ZooKeeper n'est pas défini dans le fichier de configuration, vous ne pouvez pas créer de tables répliquées et toutes les tables répliquées existantes seront en lecture seule.

La gardienne n'est pas utilisé dans `SELECT` requêtes car la réplication n'affecte pas les performances de `SELECT` et les requêtes s'exécutent aussi vite que pour les tables non répliquées. Lors de l'interrogation de tables répliquées distribuées, le comportement de ClickHouse est contrôlé par les paramètres [max\_replica\_delay\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-max_replica_delay_for_distributed_queries) et [fallback\_to\_stale\_replicas\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-fallback_to_stale_replicas_for_distributed_queries).

Pour chaque `INSERT` requête, environ dix entrées sont ajoutées à ZooKeeper par le biais de plusieurs transactions. (Pour être plus précis, c'est pour chaque bloc de données inséré; une requête D'insertion contient un bloc ou un bloc par `max_insert_block_size = 1048576` rangée.) Cela conduit à des latences légèrement plus longues pour `INSERT` par rapport aux tables non répliquées. Mais si vous suivez les recommandations pour insérer des données dans des lots de pas plus d'un `INSERT` par seconde, cela ne crée aucun problème. L'ensemble du cluster clickhouse utilisé pour coordonner un cluster ZooKeeper a un total de plusieurs centaines `INSERTs` par seconde. Le débit sur les insertions de données (le nombre de lignes par seconde) est aussi élevé que pour les non-données répliquées.

Pour les clusters très volumineux, vous pouvez utiliser différents clusters ZooKeeper pour différents fragments. Cependant, cela ne s'est pas avéré nécessaire sur le Yandex.Cluster Metrica (environ 300 serveurs).

La réplication est asynchrone et multi-maître. `INSERT` les requêtes (ainsi que `ALTER`) peuvent être envoyés à n'importe quel serveur disponible. Les données sont insérées sur le serveur où la requête est exécutée, puis il est copié sur les autres serveurs. Comme il est asynchrone, les données récemment insérées apparaissent sur les autres répliques avec une certaine latence. Si une partie des répliques ne sont pas disponibles, les données sont écrites lorsqu'elles sont disponibles. Si une réplique est disponible, la latence correspond au temps nécessaire pour transférer le bloc de données compressées sur le réseau.

Par défaut, une requête INSERT attend la confirmation de l'écriture des données à partir d'un seul réplica. Si les données ont été correctement écrit sur une seule réplique et le serveur avec cette réplique cesse d'exister, les données enregistrées seront perdues. Pour activer la confirmation des Écritures de données à partir de plusieurs réplicas, utilisez `insert_quorum` option.

Chaque bloc de données est écrit de manière atomique. La requête D'insertion est divisée en blocs jusqu'à `max_insert_block_size = 1048576` rangée. En d'autres termes, si l' `INSERT` la requête a moins de 1048576 lignes, elle est faite de manière atomique.

Les blocs de données sont dédupliquées. Pour plusieurs écritures du même bloc de données (blocs de données de même taille contenant les mêmes lignes dans le même ordre), le bloc n'est écrit qu'une seule fois. La raison en est en cas de défaillance du réseau lorsque l'application cliente ne sait pas si les données ont été écrites dans la base de données, de sorte que le `INSERT` requête peut simplement être répété. Peu importe à quelles insertions de réplica ont été envoyées avec des données identiques. `INSERTs` sont idempotents. Les paramètres de déduplication sont contrôlés par [merge\_tree](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-merge_tree) les paramètres du serveur.

Pendant la réplication, seules les données source à insérer sont transférées sur le réseau. D'autres transformations de données (fusion) sont coordonnées et effectuées sur toutes les répliques de la même manière. Cela minimise l'utilisation du réseau, ce qui signifie que la réplication fonctionne bien lorsque les répliques résident dans différents centres de données. (Notez que la duplication de données dans différents centres de données est l'objectif principal de la réplication.)

Vous pouvez avoir n'importe quel nombre de répliques des mêmes données. Yandex.Metrica utilise la double réplication en production. Chaque serveur utilise RAID-5 ou RAID-6, et RAID-10 dans certains cas. C'est une solution relativement fiable et pratique.

Le système surveille la synchronicité des données sur les répliques et est capable de récupérer après une défaillance. Le basculement est automatique (pour les petites différences de données) ou semi-automatique (lorsque les données diffèrent trop, ce qui peut indiquer une erreur de configuration).

## Création De Tables Répliquées {#creating-replicated-tables}

Le `Replicated` le préfixe est ajouté au nom du moteur de table. Exemple:`ReplicatedMergeTree`.

**Répliqué \* MergeTree paramètres**

-   `zoo_path` — The path to the table in ZooKeeper.
-   `replica_name` — The replica name in ZooKeeper.

Exemple:

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}')
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
```

<details markdown="1">

<summary>Exemple de syntaxe obsolète</summary>

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)
```

</details>

Comme le montre l'exemple, ces paramètres peuvent contenir des substitutions entre crochets. Les valeurs substituées sont tirées de la ‘macros’ section du fichier de configuration. Exemple:

``` xml
<macros>
    <layer>05</layer>
    <shard>02</shard>
    <replica>example05-02-1.yandex.ru</replica>
</macros>
```

Le chemin d'accès à la table dans ZooKeeper doit être unique pour chaque table répliquée. Les Tables sur différents fragments doivent avoir des chemins différents.
Dans ce cas, le chemin se compose des parties suivantes:

`/clickhouse/tables/` est le préfixe commun. Nous vous recommandons d'utiliser exactement celui-ci.

`{layer}-{shard}` est l'identificateur de fragment. Dans cet exemple, il se compose de deux parties, depuis l'Yandex.Le cluster Metrica utilise le sharding à deux niveaux. Pour la plupart des tâches, vous ne pouvez laisser que la substitution {shard}, qui sera étendue à l'Identificateur de partition.

`table_name` est le nom du nœud de la table dans ZooKeeper. C'est une bonne idée de le rendre identique au nom de la table. Il est défini explicitement, car contrairement au nom de la table, il ne change pas après une requête de renommage.
*HINT*: vous pouvez ajouter un nom de base de données devant `table_name` Aussi. E. g. `db_name.table_name`

Le nom du réplica identifie différentes réplicas de la même table. Vous pouvez utiliser le nom de serveur pour cela, comme dans l'exemple. Le nom doit seulement être unique dans chaque fragment.

Vous pouvez définir les paramètres explicitement au lieu d'utiliser des substitutions. Cela peut être pratique pour tester et configurer de petits clusters. Cependant, vous ne pouvez pas utiliser de requêtes DDL distribuées (`ON CLUSTER`) dans ce cas.

Lorsque vous travaillez avec de grands clusters, nous vous recommandons d'utiliser des substitutions car elles réduisent la probabilité d'erreur.

Exécutez l' `CREATE TABLE` requête sur chaque réplique. Cette requête crée une nouvelle table répliquée, ou ajoute une nouvelle réplique à un existant.

Si vous ajoutez une nouvelle réplique après que la table contient déjà des données sur d'autres répliques, les données seront copiées des autres répliques vers la nouvelle après l'exécution de la requête. En d'autres termes, la nouvelle réplique synchronise avec les autres.

Pour supprimer une réplique, exécutez `DROP TABLE`. However, only one replica is deleted – the one that resides on the server where you run the query.

## Récupération Après Des Échecs {#recovery-after-failures}

Si ZooKeeper n'est pas disponible au démarrage d'un serveur, les tables répliquées passent en mode Lecture seule. Le système tente périodiquement de se connecter à ZooKeeper.

Si ZooKeeper est indisponible pendant un `INSERT`, ou une erreur se produit lors de l'interaction avec ZooKeeper, une exception est levée.

Après la connexion à ZooKeeper, le système vérifie si l'ensemble de données du système de fichiers local correspond à l'ensemble de données attendu (ZooKeeper stocke ces informations). S'il y a des incohérences mineures, le système les résout en synchronisant les données avec les répliques.

Si le système détecte des parties de données brisées (avec la mauvaise taille des fichiers) ou des parties non reconnues (parties écrites dans le système de fichiers mais non enregistrées dans ZooKeeper), il les déplace vers le `detached` sous-répertoire (ils ne sont pas supprimés). Toutes les pièces manquantes sont copiées à partir des répliques.

Notez que ClickHouse n'effectue aucune action destructrice telle que la suppression automatique d'une grande quantité de données.

Lorsque le serveur démarre (ou établit une nouvelle session avec ZooKeeper), il vérifie uniquement la quantité et la taille de tous les fichiers. Si les tailles de fichier correspondent mais que les octets ont été modifiés quelque part au milieu, cela n'est pas détecté immédiatement, mais uniquement lorsque vous tentez de lire les données `SELECT` requête. La requête lève une exception concernant une somme de contrôle ou une taille non correspondante d'un bloc compressé. Dans ce cas, des parties de données sont ajoutées à la file d'attente de vérification et copiées à partir des répliques si nécessaire.

Si la série de données diffère trop de celle attendue, un mécanisme de sécurité est déclenché. Le serveur entre cela dans le journal et refuse de lancer. La raison en est que ce cas peut indiquer une erreur de configuration, par exemple si une réplique sur un fragment a été accidentellement configurée comme une réplique sur un fragment différent. Cependant, les seuils pour ce mécanisme sont fixés assez bas, et cette situation peut se produire pendant la récupération normale de la défaillance. Dans ce cas, les données sont restaurées semi-automatiquement - par “pushing a button”.

Pour démarrer la récupération, créez le nœud `/path_to_table/replica_name/flags/force_restore_data` dans ZooKeeper avec n'importe quel contenu, ou exécutez la commande pour restaurer toutes les tables répliquées:

``` bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

Puis redémarrez le serveur. Au démarrage, le serveur supprime ces indicateurs et démarre la récupération.

## Récupération Après La Perte De Données Complète {#recovery-after-complete-data-loss}

Si toutes les données et métadonnées ont disparu de l'un des serveurs, procédez comme suit pour la récupération:

1.  Installez ClickHouse sur le serveur. Définissez correctement les substitutions dans le fichier de configuration qui contient l'Identificateur de fragment et les répliques, si vous les utilisez.
2.  Si vous avez des tables non compliquées qui doivent être dupliquées manuellement sur les serveurs, copiez leurs données à partir d'un réplica (dans le répertoire `/var/lib/clickhouse/data/db_name/table_name/`).
3.  Copier les définitions de table situées dans `/var/lib/clickhouse/metadata/` à partir d'une réplique. Si un identificateur de fragment ou de réplica est défini explicitement dans les définitions de table, corrigez-le de manière à ce qu'il corresponde à ce réplica. (Alternativement, démarrez le serveur et faites tous les `ATTACH TABLE` les requêtes qui auraient dû être dans les .les fichiers sql dans `/var/lib/clickhouse/metadata/`.)
4.  Pour démarrer la récupération, créez le nœud ZooKeeper `/path_to_table/replica_name/flags/force_restore_data` tout contenu, ou d'exécuter la commande pour restaurer toutes les tables répliquées: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

Ensuite, démarrez le serveur (redémarrez, s'il est déjà en cours d'exécution). Les données seront téléchargées à partir de répliques.

Une autre option de récupération consiste à supprimer des informations sur la réplique perdue de ZooKeeper (`/path_to_table/replica_name`), puis créez à nouveau la réplique comme décrit dans “[Création de tables répliquées](#creating-replicated-tables)”.

Il n'y a aucune restriction sur la bande passante réseau pendant la récupération. Gardez cela à l'esprit si vous restaurez de nombreuses répliques à la fois.

## Conversion de MergeTree en ReplicatedMergeTree {#converting-from-mergetree-to-replicatedmergetree}

Nous utilisons le terme `MergeTree` pour consulter tous les moteurs de la `MergeTree family` le même que pour `ReplicatedMergeTree`.

Si vous avez eu une `MergeTree` table qui a été répliquée manuellement, vous pouvez le convertir en une table répliquée. Vous devrez peut-être le faire si vous avez déjà recueilli une grande quantité de données dans un `MergeTree` table et maintenant vous voulez activer la réplication.

Si les données diffèrent sur différentes répliques, synchronisez-les d'abord ou supprimez-les sur toutes les répliques sauf une.

Renommez la table mergetree existante, puis créez un `ReplicatedMergeTree` table avec l'ancien nom.
Déplacez les données de l'ancienne table vers `detached` sous-répertoire à l'intérieur du répertoire avec les nouvelles données de la table (`/var/lib/clickhouse/data/db_name/table_name/`).
Ensuite, exécutez `ALTER TABLE ATTACH PARTITION` sur l'une des répliques d'ajouter ces données à des parties de l'ensemble de travail.

## Conversion de ReplicatedMergeTree en MergeTree {#converting-from-replicatedmergetree-to-mergetree}

Créez une table MergeTree avec un nom différent. Déplacez toutes les données du répertoire avec le `ReplicatedMergeTree` données de la table dans le répertoire de données de la nouvelle table. Ensuite, supprimer le `ReplicatedMergeTree` table et redémarrez le serveur.

Si vous voulez vous débarrasser d'un `ReplicatedMergeTree` table sans lancer le serveur:

-   Supprimer la `.sql` fichier dans le répertoire de métadonnées (`/var/lib/clickhouse/metadata/`).
-   Supprimer le chemin correspondant dans ZooKeeper (`/path_to_table/replica_name`).

Après cela, vous pouvez lancer le serveur, créer un `MergeTree` tableau, déplacer les données de son répertoire, puis redémarrez le serveur.

## Récupération lorsque les métadonnées du Cluster Zookeeper sont perdues ou endommagées {#recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged}

Si les données de ZooKeeper ont été perdues ou endommagées, vous pouvez les enregistrer en les déplaçant dans une table non compliquée comme décrit ci-dessus.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/replication/) <!--hide-->
