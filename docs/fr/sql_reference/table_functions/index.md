---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_folder_title: Table Functions
toc_priority: 34
toc_title: Introduction
---

# Les Fonctions De Table {#table-functions}

Les fonctions de Table sont des méthodes pour construire des tables.

Vous pouvez utiliser les fonctions de table dans:

-   [FROM](../statements/select.md#select-from) la clause de la `SELECT` requête.

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [Créer une TABLE en tant que \< table\_function ()\>](../statements/create.md#create-table-query) requête.

        It's one of the methods of creating a table.

!!! warning "Avertissement"
    Vous ne pouvez pas utiliser les fonctions de table si [allow\_ddl](../../operations/settings/permissions_for_queries.md#settings_allow_ddl) paramètre est désactivé.

| Fonction              | Description                                                                                                                         |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| [fichier](file.md)    | Crée un [Fichier](../../engines/table_engines/special/file.md)-moteur de table.                                                     |
| [fusionner](merge.md) | Crée un [Fusionner](../../engines/table_engines/special/merge.md)-moteur de table.                                                  |
| [nombre](numbers.md)  | Crée une table avec une seule colonne remplie de nombres entiers.                                                                   |
| [distant](remote.md)  | Vous permet d'accéder à des serveurs distants sans [Distribué](../../engines/table_engines/special/distributed.md)-moteur de table. |
| [URL](url.md)         | Crée un [URL](../../engines/table_engines/special/url.md)-moteur de table.                                                          |
| [mysql](mysql.md)     | Crée un [MySQL](../../engines/table_engines/integrations/mysql.md)-moteur de table.                                                 |
| [jdbc](jdbc.md)       | Crée un [JDBC](../../engines/table_engines/integrations/jdbc.md)-moteur de table.                                                   |
| [ODBC](odbc.md)       | Crée un [ODBC](../../engines/table_engines/integrations/odbc.md)-moteur de table.                                                   |
| [hdfs](hdfs.md)       | Crée un [HDFS](../../engines/table_engines/integrations/hdfs.md)-moteur de table.                                                   |

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/) <!--hide-->
