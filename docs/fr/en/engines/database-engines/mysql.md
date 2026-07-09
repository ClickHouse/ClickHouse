---
description: "Permet de se connecter à des bases de données sur un serveur MySQL distant et d'exécuter des requêtes `INSERT` et `SELECT` afin d'échanger des données entre ClickHouse et MySQL."
sidebar_label: 'MySQL'
sidebar_position: 50
slug: /engines/database-engines/mysql
title: 'MySQL'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mysql-database-engine">
  # Moteur de base de données MySQL
</div>

<CloudNotSupportedBadge />

Permet de se connecter à des bases de données sur un serveur MySQL distant et d&#39;exécuter des requêtes `INSERT` et `SELECT` pour échanger des données entre ClickHouse et MySQL.

Le moteur de base de données `MySQL` traduit les requêtes pour le serveur MySQL, ce qui vous permet d&#39;effectuer des opérations telles que `SHOW TABLES` ou `SHOW CREATE TABLE`.

Vous ne pouvez pas exécuter les requêtes suivantes :

* `RENAME`
* `CREATE TABLE`
* `ALTER`

<div id="creating-a-database">
  ## Créer une base de données
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', ['database' | database], 'user', 'password')
[SETTINGS enable_compression=0]
```

**Paramètres du moteur**

* `host:port` — Adresse du serveur MySQL.
* `database` — Nom de la base de données distante.
* `user` — Utilisateur MySQL.
* `password` — Mot de passe de l’utilisateur.

**Réglages**

<div id="enable-compression">
  ### `enable_compression`
</div>

Active la compression zlib pour la connexion via le protocole MySQL. Lorsqu’elle est définie sur `1`, ClickHouse demande au serveur MySQL d’utiliser la compression au niveau du protocole.

Valeur par défaut : `0`.

Exemple :

```sql
CREATE DATABASE mysql_db
ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
SETTINGS enable_compression = 1;
```

<div id="data_types-support">
  ## Prise en charge des types de données
</div>

| MySQL                            | ClickHouse                                                   |
| -------------------------------- | ------------------------------------------------------------ |
| UNSIGNED TINYINT                 | [UInt8](../../sql-reference/data-types/int-uint.md)          |
| TINYINT                          | [Int8](../../sql-reference/data-types/int-uint.md)           |
| UNSIGNED SMALLINT                | [UInt16](../../sql-reference/data-types/int-uint.md)         |
| SMALLINT                         | [Int16](../../sql-reference/data-types/int-uint.md)          |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| INT, MEDIUMINT                   | [Int32](../../sql-reference/data-types/int-uint.md)          |
| UNSIGNED BIGINT                  | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| BIGINT                           | [Int64](../../sql-reference/data-types/int-uint.md)          |
| FLOAT                            | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE                           | [Float64](../../sql-reference/data-types/float.md)           |
| DATE                             | [Date](../../sql-reference/data-types/date.md)               |
| DATETIME, TIMESTAMP              | [DateTime](../../sql-reference/data-types/datetime.md)       |
| BINARY                           | [FixedString](../../sql-reference/data-types/fixedstring.md) |

Tous les autres types de données MySQL sont convertis en [String](../../sql-reference/data-types/string.md).

Le type [Nullable](../../sql-reference/data-types/nullable.md) est pris en charge.

<div id="global-variables-support">
  ## Prise en charge des variables globales
</div>

Pour une meilleure compatibilité, vous pouvez référencer les variables globales selon la syntaxe MySQL, sous la forme `@@identifier`.

Ces variables sont prises en charge :

* `version`
* `max_allowed_packet`

:::note
À l&#39;heure actuelle, ces variables sont des stubs et ne correspondent à rien.
:::

Exemple :

```sql
SELECT @@version;
```

<div id="examples-of-use">
  ## Exemples d’utilisation
</div>

Table dans MySQL :

```text
mysql> USE test;
Database changed

mysql> CREATE TABLE `mysql_table` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from mysql_table;
+------+-----+
| int_id | value |
+------+-----+
|      1 |     2 |
+------+-----+
1 row in set (0,00 sec)
```

Base de données dans ClickHouse échangeant des données avec le serveur MySQL :

```sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password') SETTINGS read_write_timeout=10000, connect_timeout=100;
```

```sql
SHOW DATABASES
```

```text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```

```sql
SHOW TABLES FROM mysql_db
```

```text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

```sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```