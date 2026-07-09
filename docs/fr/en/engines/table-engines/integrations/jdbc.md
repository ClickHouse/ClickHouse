---
description: 'Permet à ClickHouse de se connecter à des bases de données externes via JDBC.'
sidebar_label: 'JDBC'
sidebar_position: 100
slug: /engines/table-engines/integrations/jdbc
title: 'Moteur de table JDBC'
doc_type: 'référence'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="jdbc-table-engine">
  # Moteur de table JDBC
</div>

<CloudNotSupportedBadge />

:::note
clickhouse-jdbc-bridge contient du code expérimental et n&#39;est plus pris en charge. Il peut présenter des problèmes de fiabilité et des vulnérabilités de sécurité. Utilisez-le à vos risques.
ClickHouse recommande d&#39;utiliser les fonctions de table intégrées à ClickHouse, qui constituent une meilleure alternative pour les scénarios de requêtes ad hoc (Postgres, MySQL, MongoDB, etc.).
:::

Permet à ClickHouse de se connecter à des bases de données externes via [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

Pour établir la connexion JDBC, ClickHouse utilise le programme distinct [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge), qui doit s&#39;exécuter en tant que démon.

Ce moteur prend en charge le type de données [Nullable](../../../sql-reference/data-types/nullable.md).

<div id="creating-a-table">
  ## Création d’une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource, external_database, external_table)
```

**Paramètres du moteur**

* `datasource` — URI ou nom d’un SGBD externe.

  Format de l’URI : `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
  Exemple pour MySQL : `jdbc:mysql://localhost:3306/?user=root&password=root`.

* `external_database` — Nom d’une base de données dans un SGBD externe ou, à la place, schéma de table explicitement défini (voir les exemples).

* `external_table` — Nom de la table dans une base de données externe, ou requête `select` telle que `select * from table1 where column1=1`.

* Ces paramètres peuvent également être transmis à l’aide de [collections nommées](/fr/operations/named-collections.md).

<div id="usage-example">
  ## Exemple d&#39;utilisation
</div>

Création d&#39;une table sur un serveur MySQL en s&#39;y connectant directement via son client en ligne de commande :

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Création d’une table sur le serveur ClickHouse et sélection de données depuis celle-ci :

```sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

```sql
SELECT *
FROM jdbc_table
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

```sql
INSERT INTO jdbc_table(`int_id`, `float`)
SELECT toInt32(number), toFloat32(number * 1.0)
FROM system.numbers
```

<div id="see-also">
  ## Voir aussi
</div>

* [Fonction de table JDBC](../../../sql-reference/table-functions/jdbc.md).