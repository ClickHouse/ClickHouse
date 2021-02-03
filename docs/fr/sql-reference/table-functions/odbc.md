---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: ODBC
---

# ODBC {#table-functions-odbc}

Renvoie la table connectée via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

``` sql
odbc(connection_settings, external_database, external_table)
```

Paramètre:

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` fichier.
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

Pour implémenter en toute sécurité les connexions ODBC, ClickHouse utilise un programme distinct `clickhouse-odbc-bridge`. Si le pilote ODBC est chargé directement depuis `clickhouse-server`, les problèmes de pilote peuvent planter le serveur ClickHouse. Clickhouse démarre automatiquement `clickhouse-odbc-bridge` lorsque cela est nécessaire. Le programme ODBC bridge est installé à partir du même package que `clickhouse-server`.

Les champs avec l' `NULL` les valeurs de la table externe sont converties en valeurs par défaut pour le type de données de base. Par exemple, si un champ de table MySQL distant a `INT NULL` type il est converti en 0 (la valeur par défaut pour ClickHouse `Int32` type de données).

## Exemple D'Utilisation {#usage-example}

**Obtenir des données de L'installation MySQL locale via ODBC**

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

Récupération des données de la table MySQL dans ClickHouse:

``` sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

## Voir Aussi {#see-also}

-   [Dictionnaires externes ODBC](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [Moteur de table ODBC](../../engines/table-engines/integrations/odbc.md).

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
