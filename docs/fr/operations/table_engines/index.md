---
machine_translated: true
---

# Moteurs de Table {#table_engines}

Le moteur de table (type de table) détermine:

-   Comment et où les données sont stockées, où les écrire et où les lire.
-   Quelles requêtes sont prises en charge et comment.
-   Accès simultané aux données.
-   Utilisation des index, si elle est présente.
-   Indique si l'exécution d'une requête multithread est possible.
-   Paramètres de réplication des données.

## Familles De Moteurs {#engine-families}

### MergeTree {#mergetree}

Les moteurs de table les plus universels et fonctionnels pour les tâches à forte charge. La propriété partagée par ces moteurs est l'insertion rapide des données avec traitement ultérieur des données d'arrière-plan. `MergeTree` les moteurs de la famille prennent en charge la réplication des données (avec [Répliqué\*](replication.md) versions de moteurs), le partitionnement, et d'autres fonctionnalités non prises en charge dans d'autres moteurs.

Moteurs dans la famille:

-   [MergeTree](mergetree.md)
-   [ReplacingMergeTree](replacingmergetree.md)
-   [SummingMergeTree](summingmergetree.md)
-   [AggregatingMergeTree](aggregatingmergetree.md)
-   [CollapsingMergeTree](collapsingmergetree.md)
-   [VersionedCollapsingMergeTree](versionedcollapsingmergetree.md)
-   [GraphiteMergeTree](graphitemergetree.md)

### Journal {#log}

Léger [moteur](log_family.md) avec une fonctionnalité minimale. Ils sont les plus efficaces lorsque vous devez écrire rapidement de nombreuses petites tables (jusqu'à environ 1 million de lignes) et les lire plus tard dans leur ensemble.

Moteurs dans la famille:

-   [TinyLog](tinylog.md)
-   [StripeLog](stripelog.md)
-   [Journal](log.md)

### Moteurs d'Integration {#integration-engines}

Moteurs de communication avec d'autres systèmes de stockage et de traitement de données.

Moteurs dans la famille:

-   [Kafka](kafka.md)
-   [MySQL](mysql.md)
-   [ODBC](odbc.md)
-   [JDBC](jdbc.md)
-   [HDFS](hdfs.md)

### Moteurs spéciaux {#special-engines}

Moteurs dans la famille:

-   [Distribué](distributed.md)
-   [MaterializedView](materializedview.md)
-   [Dictionnaire](dictionary.md)
-   [Fusionner](merge.md)
-   [Fichier](file.md)
-   [NULL](null.md)
-   [Définir](set.md)
-   [Rejoindre](join.md)
-   [URL](url.md)
-   [Vue](view.md)
-   [Mémoire](memory.md)
-   [Tampon](buffer.md)

## Les colonnes virtuelles {#table_engines-virtual-columns}

Colonne virtuelle est un attribut de moteur de table intégrale qui est défini dans le code source du moteur.

Vous ne devez pas spécifier de colonnes virtuelles dans `CREATE TABLE` requête et vous ne pouvez pas les voir dans `SHOW CREATE TABLE` et `DESCRIBE TABLE` les résultats de la requête. Les colonnes virtuelles sont également en lecture seule, vous ne pouvez donc pas insérer de données dans des colonnes virtuelles.

Pour sélectionner des données dans une colonne virtuelle, vous devez spécifier son nom `SELECT` requête. `SELECT *` ne renvoie pas de valeurs à partir de colonnes virtuelles.

Si vous créez une table avec une colonne portant le même nom que l'une des colonnes virtuelles de la table, la colonne virtuelle devient inaccessible. Nous ne recommandons pas de faire cela. Pour éviter les conflits, les noms de colonnes virtuelles sont généralement précédés d'un trait de soulignement.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
