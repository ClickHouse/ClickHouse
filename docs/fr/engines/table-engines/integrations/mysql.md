---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: MySQL
---

# Mysql {#mysql}

Le moteur MySQL vous permet d'effectuer `SELECT` requêtes sur les données stockées sur un serveur MySQL distant.

## Création d'une Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

Voir une description détaillée de la [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) requête.

La structure de la table peut différer de la structure de la table MySQL d'origine:

-   Les noms de colonnes doivent être les mêmes que dans la table MySQL d'origine, mais vous pouvez utiliser seulement certaines de ces colonnes et dans n'importe quel ordre.
-   Les types de colonnes peuvent différer de ceux de la table MySQL d'origine. ClickHouse essaie de [jeter](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) valeurs des types de données ClickHouse.

**Les Paramètres Du Moteur**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` les requêtes de `REPLACE INTO`. Si `replace_query=1` la requête est substitué.

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` expression qui est ajoutée à la `INSERT` requête.

    Exemple: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, où `on_duplicate_clause` être `UPDATE c2 = c2 + 1`. Voir la [Documentation de MySQL](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) pour trouver lequel `on_duplicate_clause` vous pouvez utiliser avec le `ON DUPLICATE KEY` clause.

    Spécifier `on_duplicate_clause` vous avez besoin de passer `0` à l' `replace_query` paramètre. Si vous passez simultanément `replace_query = 1` et `on_duplicate_clause`, Clickhouse génère une exception.

Simple `WHERE` des clauses telles que `=, !=, >, >=, <, <=` sont exécutés sur le serveur MySQL.

Le reste des conditions et le `LIMIT` les contraintes d'échantillonnage sont exécutées dans ClickHouse uniquement après la fin de la requête à MySQL.

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

Table dans ClickHouse, récupération des données de la table MySQL créée ci-dessus:

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## Voir Aussi {#see-also}

-   [Le ‘mysql’ fonction de table](../../../sql-reference/table-functions/mysql.md)
-   [Utilisation de MySQL comme source de dictionnaire externe](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/mysql/) <!--hide-->
