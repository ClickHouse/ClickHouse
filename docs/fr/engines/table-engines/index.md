---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Moteurs De Table
toc_priority: 26
toc_title: Introduction
---

# Moteurs De Table {#table_engines}

Le moteur de table (type de table) détermine:

-   Comment et où les données sont stockées, où les écrire et où les lire.
-   Quelles requêtes sont prises en charge et comment.
-   Accès simultané aux données.
-   Utilisation des index, si elle est présente.
-   Indique si l'exécution d'une requête multithread est possible.
-   Paramètres de réplication des données.

## Familles De Moteurs {#engine-families}

### MergeTree {#mergetree}

Les moteurs de table les plus universels et fonctionnels pour les tâches à forte charge. La propriété partagée par ces moteurs est l'insertion rapide des données avec traitement ultérieur des données d'arrière-plan. `MergeTree` les moteurs de la famille prennent en charge la réplication des données (avec [Répliqué\*](mergetree-family/replication.md#table_engines-replication) versions de moteurs), le partitionnement, et d'autres fonctionnalités non prises en charge dans d'autres moteurs.

Moteurs dans la famille:

-   [MergeTree](mergetree-family/mergetree.md#mergetree)
-   [ReplacingMergeTree](mergetree-family/replacingmergetree.md#replacingmergetree)
-   [SummingMergeTree](mergetree-family/summingmergetree.md#summingmergetree)
-   [AggregatingMergeTree](mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
-   [CollapsingMergeTree](mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
-   [VersionedCollapsingMergeTree](mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
-   [GraphiteMergeTree](mergetree-family/graphitemergetree.md#graphitemergetree)

### Journal {#log}

Léger [moteur](log-family/index.md) avec une fonctionnalité minimale. Ils sont les plus efficaces lorsque vous devez écrire rapidement de nombreuses petites tables (jusqu'à environ 1 million de lignes) et les lire plus tard dans leur ensemble.

Moteurs dans la famille:

-   [TinyLog](log-family/tinylog.md#tinylog)
-   [StripeLog](log-family/stripelog.md#stripelog)
-   [Journal](log-family/log.md#log)

### Moteurs D'Intégration {#integration-engines}

Moteurs de communication avec d'autres systèmes de stockage et de traitement de données.

Moteurs dans la famille:

-   [Kafka](integrations/kafka.md#kafka)
-   [MySQL](integrations/mysql.md#mysql)
-   [ODBC](integrations/odbc.md#table-engine-odbc)
-   [JDBC](integrations/jdbc.md#table-engine-jdbc)
-   [HDFS](integrations/hdfs.md#hdfs)

### Moteurs Spéciaux {#special-engines}

Moteurs dans la famille:

-   [Distribué](special/distributed.md#distributed)
-   [MaterializedView](special/materializedview.md#materializedview)
-   [Dictionnaire](special/dictionary.md#dictionary)
-   \[Fusion\](spécial/de fusion.md\#fusion
-   [Fichier](special/file.md#file)
-   [NULL](special/null.md#null)
-   [Définir](special/set.md#set)
-   [Rejoindre](special/join.md#join)
-   [URL](special/url.md#table_engines-url)
-   [Vue](special/view.md#table_engines-view)
-   [Mémoire](special/memory.md#memory)
-   [Tampon](special/buffer.md#buffer)

## Les Colonnes Virtuelles {#table_engines-virtual_columns}

Colonne virtuelle est un attribut de moteur de table intégrale qui est défini dans le code source du moteur.

Vous ne devez pas spécifier de colonnes virtuelles dans `CREATE TABLE` requête et vous ne pouvez pas les voir dans `SHOW CREATE TABLE` et `DESCRIBE TABLE` les résultats de la requête. Les colonnes virtuelles sont également en lecture seule, vous ne pouvez donc pas insérer de données dans des colonnes virtuelles.

Pour sélectionner des données dans une colonne virtuelle, vous devez spécifier son nom `SELECT` requête. `SELECT *` ne renvoie pas de valeurs à partir de colonnes virtuelles.

Si vous créez une table avec une colonne portant le même nom que l'une des colonnes virtuelles de la table, la colonne virtuelle devient inaccessible. Nous ne recommandons pas de faire cela. Pour éviter les conflits, les noms de colonnes virtuelles sont généralement précédés d'un trait de soulignement.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
