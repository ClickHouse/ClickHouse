---
description: 'Le moteur PostgreSQL permet d''exécuter des requêtes `SELECT` et `INSERT` sur des données stockées
  sur un serveur PostgreSQL distant.'
sidebar_label: 'PostgreSQL'
sidebar_position: 160
slug: /engines/table-engines/integrations/postgresql
title: 'Moteur de table PostgreSQL'
doc_type: 'guide'
---

Le moteur PostgreSQL permet d&#39;exécuter des requêtes `SELECT` et `INSERT` sur des données stockées sur un serveur PostgreSQL distant.

:::note
À l&#39;heure actuelle, seules les versions 12 et ultérieures de PostgreSQL sont prises en charge pour ce moteur de table.
:::

:::tip
Découvrez notre service [Managed Postgres](/fr/docs/cloud/managed-postgres). Basé sur un stockage NVMe physiquement co-localisé avec les ressources de calcul, il offre des performances jusqu&#39;à 10 fois supérieures pour les charges de travail limitées par le disque par rapport aux solutions reposant sur du stockage en réseau comme EBS, et vous permet de répliquer vos données Postgres vers ClickHouse à l&#39;aide du connecteur Postgres CDC dans ClickPipes.
:::

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

Consultez la description détaillée de la requête [CREATE TABLE](/fr/sql-reference/statements/create/table).

La structure de la table peut différer de celle de la table PostgreSQL d’origine :

* Les noms de colonnes doivent être les mêmes que dans la table PostgreSQL d’origine, mais vous pouvez n’utiliser qu’une partie de ces colonnes, dans n’importe quel ordre.
* Les types de colonnes peuvent différer de ceux de la table PostgreSQL d’origine. ClickHouse essaie de [convertir](../../../engines/database-engines/postgresql.md#data_types-support) les valeurs vers les types de données ClickHouse.
* Le paramètre [external&#95;table&#95;functions&#95;use&#95;nulls](/fr/operations/settings/settings#external_table_functions_use_nulls) définit comment gérer les colonnes Nullable. Valeur par défaut : 1. Si sa valeur est 0, la fonction de table ne crée pas de colonnes Nullable et insère des valeurs par défaut à la place des valeurs NULL. Cela s’applique également aux valeurs NULL dans les tableaux.

**Paramètres du moteur**

* `host:port` — Adresse du serveur PostgreSQL.
* `database` — Nom de la base de données distante.
* `table` — Nom de la table distante, ou requête transmise telle quelle à PostgreSQL (voir [Utilisation d’une requête à la place d’un nom de table](#passing-a-query)).
* `user` — Utilisateur PostgreSQL.
* `password` — Mot de passe de l’utilisateur.
* `schema` — Schéma de table non par défaut. Facultatif.
* `on_conflict` — Stratégie de résolution des conflits. Exemple : `ON CONFLICT DO NOTHING`. Facultatif. Remarque : l’ajout de cette option rendra les insertions moins efficaces.

Les [collections nommées](/fr/operations/named-collections.md) (disponibles depuis la version 21.11) sont recommandées en production. Voici un exemple :

```xml
<named_collections>
    <postgres_creds>
        <host>localhost</host>
        <port>5432</port>
        <user>postgres</user>
        <password>****</password>
        <schema>schema1</schema>
    </postgres_creds>
</named_collections>
```

Certains paramètres peuvent être remplacés par des arguments de type clé-valeur :

```sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

<div id="implementation-details">
  ## Détails d’implémentation
</div>

Les requêtes `SELECT` côté PostgreSQL s’exécutent sous la forme de `COPY (SELECT ...) TO STDOUT` dans une transaction PostgreSQL en lecture seule, avec un commit après chaque requête `SELECT`.

Les clauses `WHERE` simples comme `=`, `!=`, `>`, `>=`, `<`, `<=` et `IN` sont exécutées sur le serveur PostgreSQL.

Toutes les jointures, agrégations, opérations de tri, conditions `IN [ array ]` et la contrainte d’échantillonnage `LIMIT` ne sont exécutées dans ClickHouse qu’une fois la requête vers PostgreSQL terminée.

<div id="passing-a-query">
  ## Utilisation d’une requête à la place d’un nom de table
</div>

Au lieu d’un nom de table, l’argument `table` peut être une requête `SELECT` transmise telle quelle à PostgreSQL. La structure de la table est inférée à partir du résultat de la requête. La requête peut être écrite soit sous forme de sous-requête, soit encapsulée dans la fonction `query` :

```sql
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Ceci est utile pour déporter les jointures, les agrégations ou tout autre traitement vers PostgreSQL. Une telle table est en lecture seule : les requêtes `INSERT` n&#39;y sont pas autorisées. La même syntaxe est prise en charge par la fonction de table [`postgresql`](/fr/sql-reference/table-functions/postgresql).

:::note
La forme de sous-requête `(SELECT ...)` est analysée par ClickHouse, puis re-sérialisée dans le dialecte PostgreSQL (quotage des identifiants PostgreSQL et échappement des chaînes littérales) avant d&#39;être envoyée au serveur. Elle doit donc être valide en ClickHouse SQL. Pour transmettre une syntaxe spécifique à PostgreSQL que ClickHouse n&#39;analyse pas, utilisez la forme `query('...')`, dont le texte est envoyé tel quel à PostgreSQL.

Tout `WHERE`, `LIMIT`, toute agrégation, etc. placés à l&#39;extérieur dans la requête ClickHouse englobante ne sont **pas** déportés dans la requête transmise — ils sont appliqués dans ClickHouse après récupération du résultat complet de la requête. Pour limiter les données lues depuis PostgreSQL, placez le filtre dans la requête transmise. Avec [`external_table_strict_query = 1`](/fr/operations/settings/settings#external_table_strict_query), un filtre externe qui ne peut pas être déporté est rejeté avec une exception au lieu d&#39;être appliqué localement.
:::

Les requêtes `INSERT` côté PostgreSQL s&#39;exécutent sous la forme `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` dans une transaction PostgreSQL, avec validation automatique après chaque instruction `INSERT`.

Les types `Array` de PostgreSQL sont convertis en types Array de ClickHouse.

:::note
Attention : dans PostgreSQL, les données de type tableau créées sous la forme `type_name[]` peuvent contenir des tableaux multidimensionnels avec un nombre de dimensions différent selon les lignes d&#39;une même colonne. En revanche, dans ClickHouse, seuls les tableaux multidimensionnels ayant le même nombre de dimensions dans toutes les lignes d&#39;une même colonne sont autorisés.
:::

Prend en charge plusieurs répliques, qui doivent être séparées par `|`. Par exemple :

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

La définition d’une priorité pour les répliques d’une source de dictionnaire PostgreSQL est prise en charge. Plus le nombre dans la map est élevé, plus la priorité est faible. La priorité la plus élevée est `0`.

Dans l’exemple ci-dessous, la réplique `example01-1` a la priorité la plus élevée :

```xml
<postgresql>
    <port>5432</port>
    <user>clickhouse</user>
    <password>qwerty</password>
    <replica>
        <host>example01-1</host>
        <priority>1</priority>
    </replica>
    <replica>
        <host>example01-2</host>
        <priority>2</priority>
    </replica>
    <db>db_name</db>
    <table>table_name</table>
    <where>id=10</where>
    <invalidate_query>SQL_QUERY</invalidate_query>
</postgresql>
</source>
```

<div id="usage-example">
  ## Exemple d’utilisation
</div>

<div id="table-in-postgresql">
  ### Table dans PostgreSQL
</div>

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

<div id="creating-table-in-clickhouse-and-connecting-to--postgresql-table-created-above">
  ### Création d’une table dans ClickHouse et connexion à la table PostgreSQL créée ci-dessus
</div>

Cet exemple utilise le [moteur de table PostgreSQL](/fr/engines/table-engines/integrations/postgresql.md) pour connecter la table ClickHouse à la table PostgreSQL et exécuter des instructions SELECT et INSERT sur la base de données PostgreSQL :

```sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-initial-data-from-postgresql-table-into-clickhouse-table-using-a-select-query">
  ### Insertion des données initiales d&#39;une table PostgreSQL dans une table ClickHouse à l&#39;aide d&#39;une requête SELECT
</div>

La [fonction de table postgresql](/fr/sql-reference/table-functions/postgresql.md) copie les données de PostgreSQL vers ClickHouse. Elle est souvent utilisée pour améliorer les performances des requêtes sur ces données en les interrogeant ou en effectuant des analyses dans ClickHouse plutôt que dans PostgreSQL, et peut également servir à migrer des données de PostgreSQL vers ClickHouse. Comme nous allons copier les données de PostgreSQL vers ClickHouse, nous utiliserons un moteur de table MergeTree dans ClickHouse, que nous appellerons postgresql&#95;copy :

```sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-incremental-data-from-postgresql-table-into-clickhouse-table">
  ### Insertion de données incrémentielles d&#39;une table PostgreSQL dans une table ClickHouse
</div>

Si vous effectuez ensuite une synchronisation continue entre la table PostgreSQL et la table ClickHouse après l&#39;insertion initiale, vous pouvez utiliser une clause WHERE dans ClickHouse pour insérer uniquement les données ajoutées à PostgreSQL en vous basant sur un timestamp ou un identifiant de séquence unique.

Cela nécessite de suivre l&#39;ID maximal ou le timestamp précédemment ajouté, comme suit :

```sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

Puis, insertion des valeurs de la table PostgreSQL dépassant la valeur maximale

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password')
WHERE int_id > (SELECT max(int_id) FROM default.postgresql_copy);
```

<div id="selecting-data-from-the-resulting-clickhouse-table">
  ### Sélection des données dans la table ClickHouse obtenue
</div>

```sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

```text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

<div id="using-non-default-schema">
  ### Utiliser un schéma autre que celui par défaut
</div>

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

**Voir aussi**

* [La fonction de table `postgresql`](../../../sql-reference/table-functions/postgresql.md)
* [Utiliser PostgreSQL comme source d&#39;un dictionnaire](/fr/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [ClickHouse and PostgreSQL - un mariage parfait au paradis des données - partie 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* Blog : [ClickHouse and PostgreSQL - un mariage parfait au paradis des données - partie 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)