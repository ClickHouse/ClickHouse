---
machine_translated: true
---

# Les Fonctions De Table {#table-functions}

Les fonctions de Table sont des méthodes pour construire des tables.

Vous pouvez utiliser les fonctions de table dans:

-   [FROM](../select.md#select-from) la clause de la `SELECT` requête.

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [Créer une TABLE en tant que \< table\_function ()\>](../create.md#create-table-query) requête.

        It's one of the methods of creating a table.

!!! warning "Avertissement"
    Vous ne pouvez pas utiliser les fonctions de table si [allow\_ddl](../../operations/settings/permissions_for_queries.md#settings_allow_ddl) paramètre est désactivé.

| Fonction              | Description                                                                                                                    |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------|
| [fichier](file.md)    | Crée un [Fichier](../../operations/table_engines/file.md)-moteur de table.                                                     |
| [fusionner](merge.md) | Crée un [Fusionner](../../operations/table_engines/merge.md)-moteur de table.                                                  |
| [nombre](numbers.md)  | Crée une table avec une seule colonne remplie de nombres entiers.                                                              |
| [distant](remote.md)  | Vous permet d'accéder à des serveurs distants sans [Distribué](../../operations/table_engines/distributed.md)-moteur de table. |
| [URL](url.md)         | Crée un [URL](../../operations/table_engines/url.md)-moteur de table.                                                          |
| [mysql](mysql.md)     | Crée un [MySQL](../../operations/table_engines/mysql.md)-moteur de table.                                                      |
| [jdbc](jdbc.md)       | Crée un [JDBC](../../operations/table_engines/jdbc.md)-moteur de table.                                                        |
| [ODBC](odbc.md)       | Crée un [ODBC](../../operations/table_engines/odbc.md)-moteur de table.                                                        |
| [hdfs](hdfs.md)       | Crée un [HDFS](../../operations/table_engines/hdfs.md)-moteur de table.                                                        |

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/) <!--hide-->
