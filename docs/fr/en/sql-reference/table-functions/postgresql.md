---
description: 'Permet d’exécuter des requêtes `SELECT` et `INSERT` sur des données
  stockées sur un serveur PostgreSQL distant.'
sidebar_label: 'postgresql'
sidebar_position: 160
slug: /sql-reference/table-functions/postgresql
title: 'postgresql'
doc_type: 'reference'
---

Permet d’exécuter des requêtes `SELECT` et `INSERT` sur des données stockées sur un serveur PostgreSQL distant.

<div id="syntax">
  ## Syntaxe
</div>

```sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## Arguments
</div>

| Argument      | Description                                                                                                                                                 |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`   | Adresse du serveur PostgreSQL.                                                                                                                              |
| `database`    | Nom de la base de données distante.                                                                                                                         |
| `table`       | Nom de la table distante, ou requête transmise telle quelle à PostgreSQL (voir [Utilisation d’une requête à la place d’un nom de table](#passing-a-query)). |
| `user`        | Nom d’utilisateur PostgreSQL.                                                                                                                               |
| `password`    | Mot de passe de l’utilisateur.                                                                                                                              |
| `schema`      | Schéma de table différent de celui par défaut. Facultatif.                                                                                                  |
| `on_conflict` | Stratégie de résolution des conflits. Exemple : `ON CONFLICT DO NOTHING`. Facultatif.                                                                       |

Les arguments peuvent également être transmis à l’aide de [collections nommées](/fr/operations/named-collections.md). Dans ce cas, `host` et `port` doivent être indiqués séparément. Cette approche est recommandée en environnement de production.

<div id="returned_value">
  ## Valeur renvoyée
</div>

Un objet de table avec les mêmes colonnes que la table PostgreSQL d’origine.

:::note
Dans la requête `INSERT`, pour distinguer la fonction de table `postgresql(...)` d’un nom de table suivi d’une liste de noms de colonnes, vous devez utiliser les mots-clés `FUNCTION` ou `TABLE FUNCTION`. Voir les exemples ci-dessous.
:::

<div id="implementation-details">
  ## Détails d’implémentation
</div>

Les requêtes `SELECT` côté PostgreSQL s’exécutent sous la forme de `COPY (SELECT ...) TO STDOUT`, dans une transaction PostgreSQL en lecture seule, avec un commit après chaque requête `SELECT`.

Les clauses `WHERE` simples telles que `=`, `!=`, `>`, `>=`, `<`, `<=` et `IN` sont exécutées sur le serveur PostgreSQL.

Toutes les jointures, agrégations, opérations de tri, conditions `IN [ array ]` et la contrainte d’échantillonnage `LIMIT` ne sont exécutées dans ClickHouse qu’une fois la requête PostgreSQL terminée.

<div id="passing-a-query">
  ## Utilisation d’une requête à la place d’un nom de table
</div>

Au lieu d’un nom de table, le troisième argument peut être une requête `SELECT` transmise telle quelle à PostgreSQL. La structure de la table obtenue est déduite du résultat de la requête. La requête peut être écrite soit sous forme de sous-requête, soit encapsulée dans la fonction `query` :

```sql
SELECT * FROM postgresql('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM postgresql('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Cela permet de déporter les jointures, les agrégations ou tout autre traitement vers PostgreSQL. Une telle table est en lecture seule : les `INSERT` n&#39;y sont pas autorisés. La même syntaxe est prise en charge par le moteur de table [`PostgreSQL`](/fr/engines/table-engines/integrations/postgresql).

:::note
La forme de sous-requête `(SELECT ...)` est analysée par ClickHouse, puis re-sérialisée dans le dialecte PostgreSQL (mise entre guillemets des identifiants PostgreSQL et échappement des chaînes littérales) avant d&#39;être envoyée au serveur. Elle doit donc être valide en ClickHouse SQL. Pour transmettre une syntaxe spécifique à PostgreSQL que ClickHouse n&#39;analyse pas, utilisez la forme `query('...')`, dont le texte est envoyé tel quel à PostgreSQL.

Toute clause `WHERE`, `LIMIT`, agrégation, etc. externe de la requête ClickHouse englobante n&#39;est **pas** déportée dans la requête transmise — elle est appliquée dans ClickHouse après récupération du résultat complet de la requête. Pour restreindre les données lues depuis PostgreSQL, placez le filtre dans la requête transmise. Avec [`external_table_strict_query = 1`](/fr/operations/settings/settings#external_table_strict_query), un filtre externe qui ne peut pas être déporté est rejeté avec une exception au lieu d&#39;être appliqué localement.
:::

Les requêtes `INSERT` côté PostgreSQL s&#39;exécutent sous la forme `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` dans une transaction PostgreSQL, avec validation automatique après chaque instruction `INSERT`.

Les types Array de PostgreSQL sont convertis en arrays ClickHouse.

:::note
Attention : dans PostgreSQL, une colonne de type array comme Integer[] peut contenir des arrays de dimensions différentes selon les lignes, mais dans ClickHouse, les arrays multidimensionnels doivent avoir la même dimension dans toutes les lignes.
:::

Plusieurs répliques sont prises en charge et doivent être listées avec `|`. Par exemple :

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

or

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

Prend en charge la priorité des répliques pour la source de dictionnaire PostgreSQL. Plus le nombre dans la map est élevé, plus la priorité est faible. La priorité la plus élevée est `0`.

<div id="examples">
  ## Exemples
</div>

Table dans PostgreSQL :

```text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
(1 row)
```

Sélection de données dans ClickHouse à l’aide d’arguments simples :

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

Ou en utilisant les [collections nommées](/fr/operations/named-collections.md) :

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Insertion :

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Utilisation d’un schéma non par défaut :

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

<div id="related">
  ## Voir aussi
</div>

* [Le moteur de table PostgreSQL](../../engines/table-engines/integrations/postgresql.md)
* [Utiliser PostgreSQL comme source d’un dictionnaire](/fr/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="replicating-or-migrating-postgres-data-with-peerdb">
  ### Répliquer ou migrer des données Postgres avec PeerDB
</div>

> En plus des fonctions de table, vous pouvez également utiliser [PeerDB](https://docs.peerdb.io/introduction) de ClickHouse pour mettre en place un pipeline de données continu de Postgres vers ClickHouse. PeerDB est un outil spécialement conçu pour répliquer des données de Postgres vers ClickHouse à l’aide de la change data capture (CDC).