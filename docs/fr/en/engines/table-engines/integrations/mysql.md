---
description: 'Documentation pour le moteur de table MySQL'
sidebar_label: 'MySQL'
sidebar_position: 138
slug: /engines/table-engines/integrations/mysql
title: 'Moteur de table MySQL'
doc_type: 'reference'
---

Le moteur MySQL vous permet d’exécuter des requêtes `SELECT` et `INSERT` sur des données stockées dans un serveur MySQL distant.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = MySQL({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
SETTINGS
    [ connection_pool_size=16, ]
    [ connection_max_tries=3, ]
    [ connection_wait_timeout=5, ]
    [ connection_auto_close=true, ]
    [ connect_timeout=10, ]
    [ read_write_timeout=300, ]
    [ enable_compression=false ]
;
```

Consultez une description détaillée de la requête [CREATE TABLE](/fr/sql-reference/statements/create/table).

La structure de la table peut différer de celle de la table MySQL d’origine :

* Les noms des colonnes doivent être les mêmes que dans la table MySQL d’origine, mais vous pouvez n’utiliser qu’une partie de ces colonnes, dans n’importe quel ordre.
* Les types de colonnes peuvent différer de ceux de la table MySQL d’origine. ClickHouse essaie de [convertir](../../../engines/database-engines/mysql.md#data_types-support) les valeurs en types de données ClickHouse.
* Le paramètre [external&#95;table&#95;functions&#95;use&#95;nulls](/fr/operations/settings/settings#external_table_functions_use_nulls) définit comment gérer les colonnes Nullable. Valeur par défaut : 1. Si sa valeur est 0, la fonction de table ne crée pas de colonnes Nullable et insère des valeurs par défaut à la place des valeurs nulles. Cela s’applique également aux valeurs NULL à l’intérieur des tableaux.

**Paramètres du moteur**

* `host:port` — Adresse du serveur MySQL.
* `database` — Nom de la base de données distante.
* `table` — Nom de la table distante, ou requête transmise telle quelle à MySQL (voir [Utilisation d’une requête à la place d’un nom de table](#passing-a-query)).
* `user` — Utilisateur MySQL.
* `password` — Mot de passe de l’utilisateur.
* `replace_query` — Indicateur qui convertit les requêtes `INSERT INTO` en `REPLACE INTO`. Si `replace_query=1`, la requête est remplacée.
* `on_duplicate_clause` — Expression `ON DUPLICATE KEY on_duplicate_clause` ajoutée à la requête `INSERT`.
  Exemple : `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, où `on_duplicate_clause` vaut `UPDATE c2 = c2 + 1`. Consultez la [documentation MySQL](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) pour savoir quelles valeurs de `on_duplicate_clause` vous pouvez utiliser avec la clause `ON DUPLICATE KEY`.
  Pour spécifier `on_duplicate_clause`, vous devez transmettre `0` au paramètre `replace_query`. Si vous transmettez simultanément `replace_query = 1` et `on_duplicate_clause`, ClickHouse génère une exception.

Les arguments peuvent également être transmis à l’aide de [collections nommées](/fr/operations/named-collections.md). Dans ce cas, `host` et `port` doivent être spécifiés séparément. Cette approche est recommandée pour l’environnement de production.

Les clauses `WHERE` simples, telles que `=, !=, >, >=, <, <=`, sont exécutées sur le serveur MySQL.

Les autres conditions et la contrainte d’échantillonnage `LIMIT` ne sont exécutées dans ClickHouse qu’une fois la requête MySQL terminée.

<div id="passing-a-query">
  ## Utilisation d’une requête à la place d’un nom de table
</div>

Au lieu d’un nom de table, l’argument `table` peut être une requête `SELECT` transmise telle quelle à MySQL. La structure de la table est déduite du résultat de la requête. La requête peut être écrite soit sous forme de sous-requête, soit encapsulée dans la fonction `query` :

```sql
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Cela est utile pour déporter vers MySQL les jointures, les agrégations ou tout autre traitement. Une telle table est en lecture seule : les `INSERT` n&#39;y sont pas autorisés. La même syntaxe est prise en charge par la fonction de table [`mysql`](/fr/sql-reference/table-functions/mysql).

:::note
La forme de sous-requête `(SELECT ...)` est analysée par ClickHouse puis re-sérialisée dans le dialecte MySQL (avec des identifiants entre accents graves) avant d&#39;être envoyée au serveur. Elle doit donc être valide en ClickHouse SQL. Pour transmettre une syntaxe spécifique à MySQL que ClickHouse n&#39;analyse pas, utilisez la forme `query('...')`, dont le texte est envoyé tel quel à MySQL.

Tout `WHERE`, `LIMIT`, agrégation, etc. externe de la requête ClickHouse englobante n&#39;est **pas** déporté dans la requête transmise : il est appliqué dans ClickHouse après récupération de l&#39;intégralité du résultat de la requête. Pour limiter les données lues depuis MySQL, placez le filtre à l&#39;intérieur de la requête transmise. Avec [`external_table_strict_query = 1`](/fr/operations/settings/settings#external_table_strict_query), un filtre externe qui ne peut pas être déporté est rejeté avec une exception au lieu d&#39;être appliqué localement.
:::

Prend en charge plusieurs répliques, qui doivent être listées avec `|`. Par exemple :

```sql
CREATE TABLE test_replicas (id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL(`mysql{2|3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

<div id="usage-example">
  ## Exemple d’utilisation
</div>

Créez une table dans MySQL :

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

Créer une table dans ClickHouse avec de simples arguments :

```sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

Ou avec les [collections nommées](/fr/operations/named-collections.md) :

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL(creds, table='test')
```

Récupération des données d’une table MySQL :

```sql
SELECT * FROM mysql_table
```

```text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

<div id="mysql-settings">
  ## Paramètres
</div>

Les paramètres par défaut sont peu efficaces, puisqu’ils ne réutilisent même pas les connexions. Ces paramètres vous permettent d’augmenter le nombre de requêtes exécutées par le serveur par seconde.

<div id="connection-auto-close">
  ### `connection_auto_close`
</div>

Permet de fermer automatiquement la connexion après l’exécution de la requête, c’est-à-dire de désactiver sa réutilisation.

Valeurs possibles :

* 1 — La fermeture automatique de la connexion est autorisée, la réutilisation de la connexion est donc désactivée
* 0 — La fermeture automatique de la connexion n’est pas autorisée, la réutilisation de la connexion est donc activée

Valeur par défaut : `1`.

<div id="connection-max-tries">
  ### `connection_max_tries`
</div>

Définit le nombre de nouvelles tentatives pour le pool avec basculement.

Valeurs possibles :

* Entier positif.
* 0 — Il n’y a aucune nouvelle tentative pour le pool avec basculement.

Valeur par défaut : `3`.

<div id="connection-pool-size">
  ### `connection_pool_size`
</div>

Taille du pool de connexions (si toutes les connexions sont utilisées, la requête attendra qu’une connexion se libère).

Valeurs possibles :

* Entier positif.

Valeur par défaut : `16`.

<div id="connection-wait-timeout">
  ### `connection_wait_timeout`
</div>

Délai d’expiration (en secondes) d’attente d’une connexion disponible (s’il y a déjà `connection_pool_size` connexions actives) ; 0 : ne pas attendre.

Valeurs possibles :

* Entier positif.

Valeur par défaut : `5`.

<div id="connect-timeout">
  ### `connect_timeout`
</div>

Délai d’expiration de la connexion (en secondes).

Valeurs possibles :

* Entier positif.

Valeur par défaut : `10`.

<div id="read-write-timeout">
  ### `read_write_timeout`
</div>

Délai d’expiration en lecture/écriture (en secondes).

Valeurs possibles :

* Entier positif.

Valeur par défaut : `300`.

<div id="enable-compression">
  ### `enable_compression`
</div>

Active la compression pour la connexion via le protocole MySQL.

Valeur par défaut : `false`.

Ce paramètre s&#39;applique à :

* le moteur de table `MySQL` ;
* le moteur de base de données `MySQL` ;
* la fonction de table `mysql` ;
* les collections nommées utilisées par les intégrations MySQL.

Lorsqu&#39;il est activé, ClickHouse demande la compression pour cette connexion.

Exemple :

```sql
CREATE TABLE mysql_engine_compression
(
    id UInt32,
    name String,
    age UInt32,
    money UInt32
)
ENGINE = MySQL('mysql80:3306', 'clickhouse', 'test_table', 'root', 'password')
SETTINGS enable_compression = 1;
```

<div id="see-also">
  ## Voir aussi
</div>

* [La fonction de table MySQL](../../../sql-reference/table-functions/mysql.md)
* [Utiliser MySQL comme source de dictionnaire](/fr/sql-reference/statements/create/dictionary/sources/mysql)