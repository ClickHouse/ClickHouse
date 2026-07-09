---
description: 'Renvoie la table connectée via ODBC.'
sidebar_label: 'odbc'
sidebar_position: 150
slug: /sql-reference/table-functions/odbc
title: 'odbc'
doc_type: 'reference'
---

Renvoie la table connectée via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

<div id="syntax">
  ## Syntaxe
</div>

```sql
odbc(datasource, external_database, external_table)
odbc(datasource, external_table)
odbc(named_collection)
```

<div id="arguments">
  ## Arguments
</div>

| Argument            | Description                                                                         |
| ------------------- | ----------------------------------------------------------------------------------- |
| `datasource`        | Nom de la section contenant les paramètres de connexion dans le fichier `odbc.ini`. |
| `external_database` | Nom d&#39;une base de données dans un SGBD externe.                                 |
| `external_table`    | Nom d&#39;une table dans `external_database`.                                       |

Ces paramètres peuvent également être fournis à l&#39;aide de [collections nommées](/fr/operations/named-collections.md).

Pour mettre en œuvre des connexions ODBC en toute sécurité, ClickHouse utilise un programme distinct, `clickhouse-odbc-bridge`. Si le pilote ODBC est chargé directement depuis `clickhouse-server`, un problème de pilote peut provoquer le plantage du serveur ClickHouse. ClickHouse démarre automatiquement `clickhouse-odbc-bridge` lorsque nécessaire. Le programme ODBC bridge est installé à partir du même paquet que `clickhouse-server`.

Les champs de la table externe dont la valeur est `NULL` sont convertis en valeurs par défaut du type de données sous-jacent. Par exemple, si un champ d&#39;une table MySQL distante a le type `INT NULL`, il est converti en 0 (la valeur par défaut du type de données ClickHouse `Int32`).

<div id="usage-example">
  ## Exemple d’utilisation
</div>

**Récupération de données depuis l’installation MySQL locale via ODBC**

Cet exemple a été vérifié avec Ubuntu Linux 18.04 et MySQL server 5.7.

Assurez-vous que unixODBC et MySQL Connector sont installés.

Par défaut (s’il est installé à partir de paquets), ClickHouse démarre sous l’utilisateur `clickhouse`. Vous devez donc créer et configurer cet utilisateur sur le MySQL server.

```bash
$ sudo mysql
```

```sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

Ensuite, configurez la connexion dans `/etc/odbc.ini`.

```bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
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

Table MySQL :

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

Récupération des données de la table MySQL dans ClickHouse :

```sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

<div id="see-also">
  ## Voir aussi
</div>

* [Dictionnaires ODBC](/fr/sql-reference/statements/create/dictionary/sources/odbc)
* [Moteur de table ODBC](/fr/engines/table-engines/integrations/odbc).