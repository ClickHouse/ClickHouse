---
description: 'Permet à ClickHouse de se connecter à des bases de données externes via ODBC.'
sidebar_label: 'ODBC'
sidebar_position: 150
slug: /engines/table-engines/integrations/odbc
title: 'Moteur de table ODBC'
doc_type: 'référence'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="odbc-table-engine">
  # Moteur de table ODBC
</div>

<CloudNotSupportedBadge />

Permet à ClickHouse de se connecter à des bases de données externes via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

Pour mettre en place des connexions ODBC en toute sécurité, ClickHouse utilise un programme distinct, `clickhouse-odbc-bridge`. Si le pilote ODBC est chargé directement depuis `clickhouse-server`, des problèmes de pilote peuvent provoquer un crash du serveur ClickHouse. ClickHouse démarre automatiquement `clickhouse-odbc-bridge` lorsqu&#39;il est nécessaire. Le programme ODBC bridge est installé à partir du même paquet que `clickhouse-server`.

Ce moteur prend en charge le type de données [Nullable](../../../sql-reference/data-types/nullable.md).

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(datasource, external_database, external_table)
```

Voir une description détaillée de la requête [CREATE TABLE](/fr/sql-reference/statements/create/table).

La structure de la table peut différer de celle de la table source :

* Les noms des colonnes doivent être les mêmes que dans la table source, mais vous pouvez n’utiliser que certaines d’entre elles, dans n’importe quel ordre.
* Les types de colonnes peuvent différer de ceux de la table source. ClickHouse tente de [convertir](/fr/sql-reference/functions/type-conversion-functions#CAST) les valeurs vers les types de données ClickHouse.
* Le paramètre [external&#95;table&#95;functions&#95;use&#95;nulls](/fr/operations/settings/settings#external_table_functions_use_nulls) définit la manière de gérer les colonnes Nullable. Valeur par défaut : 1. Si la valeur est 0, la fonction de table ne crée pas de colonnes Nullable et insère des valeurs par défaut à la place des valeurs nulles. Cela s’applique également aux valeurs NULL à l’intérieur des tableaux.

**Paramètres du moteur**

* `datasource` — Nom de la section contenant les paramètres de connexion dans le fichier `odbc.ini`.
* `external_database` — Nom d’une base de données dans un SGBD externe.
* `external_table` — Nom d’une table dans `external_database`.

Ces paramètres peuvent également être transmis à l’aide de [collections nommées](/fr/operations/named-collections.md).

<div id="usage-example">
  ## Exemple d&#39;utilisation
</div>

**Récupération de données depuis l&#39;installation locale de MySQL via ODBC**

Cet exemple a été testé sous Ubuntu Linux 18.04 et MySQL server 5.7.

Assurez-vous que unixODBC et MySQL Connector sont installés.

Par défaut (s&#39;il est installé à partir de paquets), ClickHouse démarre avec l&#39;utilisateur `clickhouse`. Vous devez donc créer et configurer cet utilisateur sur le serveur MySQL.

```bash
$ sudo mysql
```

```sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'localhost' WITH GRANT OPTION;
```

Configurez ensuite la connexion dans `/etc/odbc.ini`.

```bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USER = clickhouse
PASSWORD = clickhouse
```

Vous pouvez vérifier la connexion à l’aide de l’utilitaire `isql` fourni avec unixODBC.

```bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

Table dans MySQL :

```text
mysql> CREATE DATABASE test;
Query OK, 1 row affected (0,01 sec)

mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test.test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test.test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Table dans ClickHouse, qui récupère les données de la table MySQL :

```sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```

```sql
SELECT * FROM odbc_t
```

```text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

<div id="see-also">
  ## Voir aussi
</div>

* [Dictionnaires ODBC](/fr/sql-reference/statements/create/dictionary/sources/odbc)
* [Fonction de table ODBC](../../../sql-reference/table-functions/odbc.md)