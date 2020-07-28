---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_folder_title: Table Engines
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

### Mergetree {#mergetree}

Les moteurs de table les plus universels et fonctionnels pour les tâches à forte charge. La propriété partagée par ces moteurs est l'insertion rapide des données avec traitement ultérieur des données d'arrière-plan. `MergeTree` les moteurs de la famille prennent en charge la réplication des données (avec [Répliqué\*](mergetree_family/replication.md) versions de moteurs), le partitionnement, et d'autres fonctionnalités non prises en charge dans d'autres moteurs.

Moteurs dans la famille:

-   [MergeTree](mergetree_family/mergetree.md)
-   [ReplacingMergeTree](mergetree_family/replacingmergetree.md)
-   [SummingMergeTree](mergetree_family/summingmergetree.md)
-   [AggregatingMergeTree](mergetree_family/aggregatingmergetree.md)
-   [CollapsingMergeTree](mergetree_family/collapsingmergetree.md)
-   [VersionedCollapsingMergeTree](mergetree_family/versionedcollapsingmergetree.md)
-   [GraphiteMergeTree](mergetree_family/graphitemergetree.md)

### Journal {#log}

Léger [moteur](log_family/index.md) avec une fonctionnalité minimale. Ils sont les plus efficaces lorsque vous devez écrire rapidement de nombreuses petites tables (jusqu'à environ 1 million de lignes) et les lire plus tard dans leur ensemble.

Moteurs dans la famille:

-   [TinyLog](log_family/tinylog.md)
-   [StripeLog](log_family/stripelog.md)
-   [Journal](log_family/log.md)

### Moteurs D'Intégration {#integration-engines}

Moteurs de communication avec d'autres systèmes de stockage et de traitement de données.

Moteurs dans la famille:

-   [Kafka](integrations/kafka.md)
-   [MySQL](integrations/mysql.md)
-   [ODBC](integrations/odbc.md)
-   [JDBC](integrations/jdbc.md)
-   [HDFS](integrations/hdfs.md)

### Moteurs Spéciaux {#special-engines}

Moteurs dans la famille:

-   [Distribué](special/distributed.md)
-   [MaterializedView](special/materializedview.md)
-   [Dictionnaire](special/dictionary.md)
-   [Fusionner](special/merge.md)
-   [Fichier](special/file.md)
-   [NULL](special/null.md)
-   [Définir](special/set.md)
-   [Rejoindre](special/join.md)
-   [URL](special/url.md)
-   [Vue](special/view.md)
-   [Mémoire](special/memory.md)
-   [Tampon](special/buffer.md)

## Les Colonnes Virtuelles {#table_engines-virtual-columns}

Colonne virtuelle est un attribut de moteur de table intégrale qui est défini dans le code source du moteur.

Vous ne devez pas spécifier de colonnes virtuelles dans `CREATE TABLE` requête et vous ne pouvez pas les voir dans `SHOW CREATE TABLE` et `DESCRIBE TABLE` les résultats de la requête. Les colonnes virtuelles sont également en lecture seule, vous ne pouvez donc pas insérer de données dans des colonnes virtuelles.

Pour sélectionner des données dans une colonne virtuelle, vous devez spécifier son nom `SELECT` requête. `SELECT *` ne renvoie pas de valeurs à partir de colonnes virtuelles.

Si vous créez une table avec une colonne portant le même nom que l'une des colonnes virtuelles de la table, la colonne virtuelle devient inaccessible. Nous ne recommandons pas de faire cela. Pour éviter les conflits, les noms de colonnes virtuelles sont généralement précédés d'un trait de soulignement.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
