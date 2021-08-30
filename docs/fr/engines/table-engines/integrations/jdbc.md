---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: JDBC
---

# JDBC {#table-engine-jdbc}

Permet à ClickHouse de se connecter à des bases de données externes via [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

Pour implémenter la connexion JDBC, ClickHouse utilise le programme séparé [clickhouse-JDBC-pont](https://github.com/alex-krash/clickhouse-jdbc-bridge) cela devrait fonctionner comme un démon.

Ce moteur prend en charge le [Nullable](../../../sql-reference/data-types/nullable.md) type de données.

## Création d'une Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(dbms_uri, external_database, external_table)
```

**Les Paramètres Du Moteur**

-   `dbms_uri` — URI of an external DBMS.

    Format: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    Exemple pour MySQL: `jdbc:mysql://localhost:3306/?user=root&password=root`.

-   `external_database` — Database in an external DBMS.

-   `external_table` — Name of the table in `external_database`.

## Exemple D'Utilisation {#usage-example}

Création d'une table dans le serveur MySQL en se connectant directement avec son client console:

``` text
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

Création d'une table dans le serveur ClickHouse et sélection des données:

``` sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

``` sql
SELECT *
FROM jdbc_table
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

## Voir Aussi {#see-also}

-   [Fonction de table JDBC](../../../sql-reference/table-functions/jdbc.md).

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/jdbc/) <!--hide-->
