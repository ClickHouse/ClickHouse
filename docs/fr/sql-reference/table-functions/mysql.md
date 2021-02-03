---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: mysql
---

# mysql {#mysql}

Permettre `SELECT` requêtes à effectuer sur des données stockées sur un serveur MySQL distant.

``` sql
mysql('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

**Paramètre**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` les requêtes de `REPLACE INTO`. Si `replace_query=1` la requête est remplacé.

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` expression qui est ajoutée à la `INSERT` requête.

        Example: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, where `on_duplicate_clause` is `UPDATE c2 = c2 + 1`. See the MySQL documentation to find which `on_duplicate_clause` you can use with the `ON DUPLICATE KEY` clause.

        To specify `on_duplicate_clause` you need to pass `0` to the `replace_query` parameter. If you simultaneously pass `replace_query = 1` and `on_duplicate_clause`, ClickHouse generates an exception.

Simple `WHERE` des clauses telles que `=, !=, >, >=, <, <=` sont actuellement exécutés sur le serveur MySQL.

Le reste des conditions et le `LIMIT` les contraintes d'échantillonnage sont exécutées dans ClickHouse uniquement après la fin de la requête à MySQL.

**Valeur Renvoyée**

Un objet table avec les mêmes colonnes que la table MySQL d'origine.

## Exemple D'Utilisation {#usage-example}

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

Sélection des données de ClickHouse:

``` sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

## Voir Aussi {#see-also}

-   [Le ‘MySQL’ tableau moteur](../../engines/table-engines/integrations/mysql.md)
-   [Utilisation de MySQL comme source de dictionnaire externe](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/mysql/) <!--hide-->
