---
description: 'Permet d’exécuter des requêtes `SELECT` et `INSERT` sur des données
  stockées sur un serveur MySQL distant.'
sidebar_label: 'mysql'
sidebar_position: 137
slug: /sql-reference/table-functions/mysql
title: 'mysql'
doc_type: 'reference'
---

Permet d’exécuter des requêtes `SELECT` et `INSERT` sur des données stockées sur un serveur MySQL distant.

<div id="syntax">
  ## Syntaxe
</div>

```sql
mysql({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## Arguments
</div>

| Argument              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`           | Adresse du serveur MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `database`            | Nom de la base de données distante.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `table`               | Nom de la table distante, ou requête transmise à MySQL telle quelle (voir [Utilisation d’une requête à la place d’un nom de table](#passing-a-query)).                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `user`                | Utilisateur MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `password`            | Mot de passe de l’utilisateur.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `replace_query`       | Indicateur qui convertit les requêtes `INSERT INTO` en `REPLACE INTO`. Valeurs possibles :<br />    - `0` - La requête est exécutée comme `INSERT INTO`.<br />    - `1` - La requête est exécutée comme `REPLACE INTO`.                                                                                                                                                                                                                                                                                                                                                |
| `on_duplicate_clause` | Expression `ON DUPLICATE KEY on_duplicate_clause` ajoutée à la requête `INSERT`. Elle ne peut être spécifiée qu’avec `replace_query = 0` (si vous transmettez simultanément `replace_query = 1` et `on_duplicate_clause`, ClickHouse génère une exception).<br />    Exemple : `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1;`<br />    Ici, `on_duplicate_clause` vaut `UPDATE c2 = c2 + 1`. Consultez la documentation MySQL pour savoir quelle valeur de `on_duplicate_clause` vous pouvez utiliser avec la clause `ON DUPLICATE KEY`. |

Les arguments peuvent aussi être transmis à l’aide de [collections nommées](/fr/operations/named-collections.md). Dans ce cas, `host` et `port` doivent être spécifiés séparément. Cette approche est recommandée pour l’environnement de production.

Les clauses `WHERE` simples telles que `=, !=, >, >=, <, <=` sont actuellement exécutées sur le serveur MySQL.

Les autres conditions ainsi que la contrainte d’échantillonnage `LIMIT` ne sont exécutées dans ClickHouse qu’une fois la requête MySQL terminée.

<div id="passing-a-query">
  ## Utilisation d’une requête à la place d’un nom de table
</div>

Au lieu d’un nom de table, le troisième argument peut être une requête `SELECT` transmise à MySQL telle quelle. La structure de la table résultante est inférée à partir du résultat de la requête. La requête peut être écrite soit sous forme de sous-requête, soit encapsulée dans la fonction `query` :

```sql
SELECT * FROM mysql('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM mysql('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Cela est utile pour déléguer à MySQL les jointures, les agrégations ou tout autre traitement. Une telle table est en lecture seule : `INSERT` n’y est pas autorisé. La même syntaxe est prise en charge par le moteur de table [`MySQL`](/fr/engines/table-engines/integrations/mysql).

:::note
La forme de sous-requête `(SELECT ...)` est analysée par ClickHouse puis re-sérialisée dans le dialecte MySQL (quotage des identifiants avec des accents graves) avant d’être envoyée au serveur. Elle doit donc être valide en ClickHouse SQL. Pour transmettre une syntaxe spécifique à MySQL que ClickHouse n’analyse pas, utilisez la forme `query('...')`, dont le texte est envoyé à MySQL tel quel.

Tout `WHERE`, `LIMIT`, toute agrégation, etc. externe de la requête ClickHouse englobante n’est **pas** délégué à la requête transmise — il est appliqué dans ClickHouse après la récupération du résultat complet de la requête. Pour limiter les données lues depuis MySQL, placez le filtre à l’intérieur de la requête transmise. Avec [`external_table_strict_query = 1`](/fr/operations/settings/settings#external_table_strict_query), un filtre externe qui ne peut pas être délégué est rejeté avec une exception au lieu d’être appliqué localement.
:::

Prend en charge plusieurs répliques, qui doivent être listées avec `|`. Par exemple :

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

or

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

<div id="returned_value">
  ## Valeur renvoyée
</div>

Un objet de table avec les mêmes colonnes que la table MySQL d’origine.

:::note
Certains types de données MySQL peuvent être associés à différents types de ClickHouse ; ce comportement est défini par le paramètre de requête [mysql&#95;datatypes&#95;support&#95;level](/fr/operations/settings/settings.md#mysql_datatypes_support_level)
:::

:::note
Dans la requête `INSERT`, pour distinguer la fonction de table `mysql(...)` d’un nom de table accompagné d’une liste de noms de colonnes, vous devez utiliser les mots-clés `FUNCTION` ou `TABLE FUNCTION`. Voir les exemples ci-dessous.
:::

<div id="examples">
  ## Exemples
</div>

Table MySQL :

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));

mysql> INSERT INTO test (`int_id`, `float`) VALUES (1,2);

mysql> SELECT * FROM test;
+--------+-------+
| int_id | float |
+--------+-------+
|      1 |     2 |
+--------+-------+
```

Sélection de données dans ClickHouse :

```sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

Ou en utilisant des [collections nommées](/fr/operations/named-collections.md) :

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
SELECT * FROM mysql(creds, table='test');
```

```text
┌─int_id─┬─float─┐
│      1 │     2 │
└────────┴───────┘
```

<div id="enable-compression">
  ### `enable_compression`
</div>

Active la compression pour la connexion via le protocole MySQL.

Valeur par défaut : `false`.

Ce paramètre s’applique à :

* la fonction de table `mysql` ;
* le moteur de table `MySQL` ;
* le moteur de base de données `MySQL` ;
* les collections nommées utilisées par les intégrations MySQL.

Lorsqu’il est activé, ClickHouse demande la compression pour la connexion.

Exemple :

```sql
SELECT *
FROM mysql(
    'mysql80:3306',
    'clickhouse',
    'test_table',
    'root',
    'password',
    SETTINGS enable_compression = 1
);
```

Remplacement et insertion :

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

```text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

Copie de données d’une table MySQL vers une table ClickHouse :

```sql
CREATE TABLE mysql_copy
(
   `id` UInt64,
   `datetime` DateTime('UTC'),
   `description` String,
)
ENGINE = MergeTree
ORDER BY (id,datetime);

INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password');
```

Ou, si vous ne copiez qu’un lot incrémental depuis MySQL à partir de l’identifiant maximal actuel :

```sql
INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password')
WHERE id > (SELECT max(id) FROM mysql_copy);
```

<div id="related">
  ## Voir aussi
</div>

* [Le moteur de table « MySQL »](../../engines/table-engines/integrations/mysql.md)
* [Utiliser MySQL comme source de dictionnaire](/fr/sql-reference/statements/create/dictionary/sources/mysql)
* [mysql&#95;datatypes&#95;support&#95;level](/fr/operations/settings/settings.md#mysql_datatypes_support_level)
* [mysql&#95;map&#95;fixed&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/fr/operations/settings/settings.md#mysql_map_fixed_string_to_text_in_show_columns)
* [mysql&#95;map&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/fr/operations/settings/settings.md#mysql_map_string_to_text_in_show_columns)
* [mysql&#95;max&#95;rows&#95;to&#95;insert](/fr/operations/settings/settings.md#mysql_max_rows_to_insert)