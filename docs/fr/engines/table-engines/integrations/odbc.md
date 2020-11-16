---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: ODBC
---

# ODBC {#table-engine-odbc}

Permet à ClickHouse de se connecter à des bases de données externes via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

Pour implémenter en toute sécurité les connexions ODBC, ClickHouse utilise un programme distinct `clickhouse-odbc-bridge`. Si le pilote ODBC est chargé directement depuis `clickhouse-server`, les problèmes de pilote peuvent planter le serveur ClickHouse. Clickhouse démarre automatiquement `clickhouse-odbc-bridge` lorsque cela est nécessaire. Le programme ODBC bridge est installé à partir du même package que `clickhouse-server`.

Ce moteur prend en charge le [Nullable](../../../sql-reference/data-types/nullable.md) type de données.

## Création d'une Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(connection_settings, external_database, external_table)
```

Voir une description détaillée de la [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) requête.

La structure de la table peut différer de la structure de la table source:

-   Les noms de colonnes doivent être les mêmes que dans la table source, mais vous pouvez utiliser quelques-unes de ces colonnes et dans n'importe quel ordre.
-   Les types de colonnes peuvent différer de ceux de la table source. ClickHouse essaie de [jeter](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) valeurs des types de données ClickHouse.

**Les Paramètres Du Moteur**

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` fichier.
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

## Exemple D'Utilisation {#usage-example}

**Récupération des données de L'installation MySQL locale via ODBC**

Cet exemple est vérifié pour Ubuntu Linux 18.04 et MySQL server 5.7.

Assurez-vous que unixODBC et MySQL Connector sont installés.

Par défaut (si installé à partir de paquets), ClickHouse démarre en tant qu'utilisateur `clickhouse`. Ainsi, vous devez créer et configurer cet utilisateur dans le serveur MySQL.

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

Puis configurez la connexion dans `/etc/odbc.ini`.

``` bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

Vous pouvez vérifier la connexion en utilisant le `isql` utilitaire de l'installation unixODBC.

``` bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

Table dans MySQL:

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

Table dans ClickHouse, récupération des données de la table MySQL:

``` sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```

``` sql
SELECT * FROM odbc_t
```

``` text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

## Voir Aussi {#see-also}

-   [Dictionnaires externes ODBC](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [Fonction de table ODBC](../../../sql-reference/table-functions/odbc.md)

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/odbc/) <!--hide-->
