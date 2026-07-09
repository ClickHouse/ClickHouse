---
description: 'Documentation sur la gestion des projections'
sidebar_label: 'PROJECTION'
sidebar_position: 49
slug: /sql-reference/statements/alter/projection
title: 'Projections'
doc_type: 'référence'
---

Cette page présente ce que sont les projections, comment les utiliser et les différentes options permettant de les gérer.

<div id="overview">
  ## Vue d’ensemble des projections
</div>

Les projections stockent les données dans un format qui optimise l’exécution des requêtes. Cette fonctionnalité est utile pour :

* Exécuter des requêtes sur une colonne qui ne fait pas partie de la clé primaire
* Pré-agréger des colonnes, ce qui réduit à la fois les calculs et les IO

Vous pouvez définir une ou plusieurs projections pour une table et, lors de l’analyse de la requête, ClickHouse sélectionnera celle qui contient le moins de données à analyser, sans modifier la requête fournie par l’utilisateur.

:::note[Utilisation du disque]
Les projections créent en interne une nouvelle table masquée, ce qui signifie que davantage d’IO et d’espace disque seront nécessaires.
Par exemple, si la projection définit une clé primaire différente, toutes les données de la table d’origine seront dupliquées.
:::

Vous trouverez plus de détails techniques sur le fonctionnement interne des projections sur cette [page](/fr/guides/best-practices/sparse-primary-indexes.md/#option-3-projections).

<div id="examples">
  ## Utilisation des projections
</div>

<div id="example-filtering-without-using-primary-keys">
  ### Exemple de filtrage sans utiliser de clés primaires
</div>

Création de la table :

```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```

Avec `ALTER TABLE`, nous pouvons ajouter la projection à une table existante :

```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
    SELECT *
    ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```

Insertion des données :

```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

La projection nous permettra de filtrer rapidement sur `user_name`, même si, dans la table d’origine, `user_name` n’a pas été défini comme `PRIMARY_KEY`.
Au moment de la requête, ClickHouse détermine que moins de données seront traitées si la projection est utilisée, car les données sont ordonnées par `user_name`.

```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

Pour vérifier qu’une requête utilise bien la projection, nous pouvons consulter la table `system.query_log`. Dans le champ `projections`, nous trouvons le nom de la projection utilisée, ou une valeur vide si aucune n’a été utilisée :

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="example-pre-aggregation-query">
  ### Exemple de requête de pré-agrégation
</div>

Créez la table avec la projection `projection_visits_by_user` :

```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```

Insérez les données :

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```

Exécutez une première requête avec `GROUP BY` en utilisant le champ `user_agent`.
Cette requête n&#39;utilisera pas la projection définie, car la pré-agrégation ne correspond pas.

```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

Pour exploiter la projection, vous pouvez exécuter des requêtes qui sélectionnent tout ou partie des champs de pré-agrégation et de `GROUP BY` :

```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```

```sql
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

Comme indiqué précédemment, vous pouvez consulter la table `system.query_log` pour vérifier si une projection a été utilisée.
Le champ `projections` indique le nom de la projection utilisée.
Il sera vide si aucune projection n’a été utilisée :

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="projection-indexes">
  ### Créer et utiliser des index de projection
</div>

Création d’un [index de projection](../../../engines/table-engines/mergetree-family/mergetree.md#projection-index) :

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id INDEX user_id TYPE basic
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

<details markdown="1">
  <summary>Création d’une projection avec le champ `_part_offset` explicite</summary>

  Les index de projection peuvent aussi être créés à l’aide de la syntaxe suivante (non recommandée) :

  ```sql
  CREATE TABLE events
  (
      `event_time` DateTime,
      `event_id` UInt64,
      `user_id` UInt64,
      `huge_string` String,
      PROJECTION order_by_user_id
      (
          SELECT
              _part_offset
          ORDER BY user_id
      )
  )
  ENGINE = MergeTree()
  ORDER BY (event_id);
  ```
</details>

Insertion de quelques données d’exemple :

```sql
INSERT INTO events SELECT * FROM generateRandom() LIMIT 100000;
```

Le champ `_part_offset` conserve sa valeur lors des fusions et des mutations, ce qui le rend utile pour l’indexation secondaire. Nous pouvons nous en servir dans les requêtes :

```sql
SELECT
    count()
FROM events
WHERE _part_starting_offset + _part_offset IN (
    SELECT _part_starting_offset + _part_offset
    FROM events
    WHERE user_id = 42
)
SETTINGS enable_shared_storage_snapshot_in_query = 1
```

<div id="example-projection-with-where">
  ### Exemple de projection avec une clause WHERE
</div>

Les projections peuvent inclure une clause `WHERE` afin de ne stocker qu’un sous-ensemble de lignes. Cela est utile lorsque les requêtes filtrent fréquemment selon un prédicat connu — la projection matérialise uniquement les lignes correspondantes, ce qui réduit l’espace de stockage et améliore les performances des requêtes.

Création d’une table et ajout d’une projection filtrée :

```sql
CREATE TABLE events
(
    `event_type` String,
    `time` DateTime,
    `message` String
)
ENGINE = MergeTree()
ORDER BY time;

ALTER TABLE events ADD PROJECTION proj_pageview (
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

ALTER TABLE events MATERIALIZE PROJECTION proj_pageview;
```

Insertion de données :

```sql
INSERT INTO events VALUES
    ('pageview', '2024-01-01', 'homepage'),
    ('click', '2024-01-02', 'button'),
    ('pageview', '2024-01-03', 'about');
```

Lorsque la clause `WHERE` d’une requête **implique** la clause `WHERE` de la projection (c.-à-d. que chaque condition du filtre de la projection est également présente dans le filtre de la requête), l’optimiseur peut automatiquement utiliser la projection s’il juge que cela est bénéfique :

```sql
-- This query implies the projection's WHERE, so the projection may be used:
SELECT time, message FROM events WHERE event_type = 'pageview';

-- A stricter query also implies the projection's WHERE:
SELECT time, message FROM events WHERE event_type = 'pageview' AND time > '2024-01-01';

-- This query does NOT imply the projection, so the base table is scanned:
SELECT time, message FROM events WHERE event_type = 'click';
```

La vérification d’implication est conservatrice : elle repose sur une correspondance exacte des conjonctions dans la forme canonique de l’expression. Elle peut passer à côté de certaines optimisations pourtant valides (par exemple, des implications d’intervalles), mais elle ne produira jamais de résultats incorrects.

<div id="manipulating-projections">
  ## Manipulation des projections
</div>

Les opérations suivantes sont disponibles pour les [projections](/fr/engines/table-engines/mergetree-family/mergetree.md/#projections) :

<div id="add-projection">
  ### ADD PROJECTION
</div>

Utilisez l’instruction ci-dessous pour ajouter une description de projection dans les métadonnées d’une table :

```sql
-- Normal projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]

-- Aggregate projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]
```

:::note
Lorsqu’une projection définit une clause `WHERE`, seules les lignes correspondant au prédicat sont matérialisées. L’optimiseur peut utiliser une telle projection lorsque le `WHERE` de la requête implique logiquement le `WHERE` de la projection et que celle-ci est avantageuse pour le plan d’exécution de la requête. Cela s’applique aussi bien aux projections ordinaires qu’aux projections agrégées.
:::

<div id="with-settings">
  #### Clause `WITH SETTINGS`
</div>

`WITH SETTINGS` définit des **paramètres de projection** qui personnalisent la façon dont la projection stocke les données (par exemple, `index_granularity` ou `index_granularity_bytes`).
Ils correspondent directement aux **paramètres de table MergeTree**, mais s&#39;appliquent **uniquement à cette projection**.

Exemple :

```sql
ALTER TABLE t
ADD PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 4096,
    index_granularity_bytes = 1048576
);
```

Les paramètres de projection priment sur les paramètres effectifs de la table pour la projection, sous réserve des règles de validation (par exemple, les remplacements invalides ou incompatibles seront rejetés).

<div id="drop-projection">
  ### DROP PROJECTION
</div>

Utilisez l’instruction ci-dessous pour supprimer la description de projection des métadonnées d’une table et supprimer les fichiers de projection du disque.
Cette opération est implémentée sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
```

<div id="materialize-projection">
  ### MATERIALIZE PROJECTION
</div>

Utilisez l’instruction ci-dessous pour reconstruire la projection `name` dans la partition `partition_name`.
Cette opération est implémentée sous la forme d’une [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

<div id="clear-projection">
  ### CLEAR PROJECTION
</div>

Utilisez l’instruction ci-dessous pour supprimer du disque les fichiers de projection sans en supprimer la description.
Cette opération est implémentée sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

Les commandes `ADD`, `DROP` et `CLEAR` sont légères, au sens où elles se contentent de modifier les métadonnées ou de supprimer des fichiers.
De plus, elles sont répliquées et synchronisent les métadonnées des projections via ClickHouse Keeper ou ZooKeeper.

:::note
La manipulation des projections est prise en charge uniquement pour les tables utilisant le moteur [`*MergeTree`](/fr/engines/table-engines/mergetree-family/mergetree.md) (y compris les variantes [répliquées](/fr/engines/table-engines/mergetree-family/replication.md)).
:::

<div id="control-projections-merges">
  ### Contrôler le comportement de fusion des projections
</div>

Lorsque vous exécutez une requête, ClickHouse choisit de lire soit la table d’origine, soit l’une de ses projections.
La décision de lire depuis la table d’origine ou depuis l’une de ses projections est prise individuellement pour chaque part de la table.
ClickHouse cherche généralement à lire le moins de données possible et utilise plusieurs techniques pour identifier la meilleure part à lire, par exemple en échantillonnant la clé primaire d’une part.
Dans certains cas, les parts de la table source n’ont pas de parts de projection correspondantes.
Cela peut arriver, par exemple, parce que la création d’une projection pour une table en SQL est « paresseuse » par défaut : elle n’affecte que les données nouvellement insérées, mais laisse les parts existantes inchangées.

Comme l’une des projections contient déjà les valeurs d’agrégation précalculées, ClickHouse essaie de lire à partir des parts de projection correspondantes pour éviter de refaire l’agrégation à l’exécution de la requête. Si une part donnée ne possède pas la part de projection correspondante, l’exécution de la requête revient à la part d’origine.

Mais que se passe-t-il si les lignes de la table d’origine changent de manière non triviale en raison d’opérations de fusion en arrière-plan non triviales sur les parts de données ?
Par exemple, supposons que la table utilise le table engine `ReplacingMergeTree`.
Si la même ligne est détectée dans plusieurs parts d’entrée pendant la fusion, seule la version la plus récente de la ligne (issue de la part insérée le plus récemment) est conservée, tandis que toutes les versions plus anciennes sont supprimées.

De même, si la table utilise le table engine `AggregatingMergeTree`, l’opération de fusion peut regrouper les mêmes lignes dans les parts d’entrée (sur la base des valeurs de la clé primaire) en une seule ligne afin de mettre à jour les états d’agrégation partiels.

Avant ClickHouse v24.8, les parts de projection se désynchronisaient soit silencieusement des données principales, soit certaines opérations comme les mises à jour et les suppressions ne pouvaient pas du tout être exécutées, car la base de données levait automatiquement une exception si la table comportait des projections.

Depuis la v24.8, un nouveau paramètre au niveau de la table, [`deduplicate_merge_projection_mode`](/fr/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode), contrôle le comportement lorsque les opérations de fusion en arrière-plan non triviales mentionnées ci-dessus se produisent dans des parts de la table d’origine.

Les mutations de suppression constituent un autre exemple d’opérations de fusion de parts qui suppriment des lignes dans les parts de la table d’origine. Depuis la v24.7, nous disposons également d’un paramètre pour contrôler le comportement concernant les mutations de suppression déclenchées par les lightweight deletes : [`lightweight_mutation_projection_mode`](/fr/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode).

Vous trouverez ci-dessous les valeurs possibles pour `deduplicate_merge_projection_mode` et `lightweight_mutation_projection_mode` :

* `throw` (par défaut) : une exception est levée, ce qui empêche les parts de projection de se désynchroniser.
* `drop` : les parts de table de projection concernées sont supprimées. Les requêtes reviennent alors à la part de la table d’origine pour les parts de projection concernées.
* `rebuild` : la part de projection concernée est reconstruite afin de rester cohérente avec les données de la part de la table d’origine.

<div id="limitations">
  ## Limitations
</div>

Il n’est pas possible d’utiliser une colonne `ALIAS` dans la clause `ORDER BY` d’une projection. Par exemple :

```sql
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 ALIAS a + 1,
--highlight-next-line
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;
-- Fails with UNKNOWN_IDENTIFIER
```

Les colonnes `ALIAS` ne sont pas stockées physiquement et sont calculées à la volée à l’exécution de la requête. Elles ne sont donc pas disponibles lors de l’écriture de la part de projection, au moment où l’expression de tri est évaluée.

Utilisez plutôt des colonnes `MATERIALIZED` ou intégrez directement l’expression :

```sql
-- using MATERIALIZED column
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 MATERIALIZED a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

-- using an inline expression
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    PROJECTION p (SELECT a ORDER BY a + 1)
)
ENGINE = MergeTree ORDER BY id;
```

<div id="see-also">
  ## Voir aussi
</div>

* [&quot;Contrôle des projections lors des fusions&quot; (article de blog)](https://clickhouse.com/blog/clickhouse-release-24-08#control-of-projections-during-merges)
* [&quot;Projections&quot; (guide)](/fr/data-modeling/projections#using-projections-to-speed-up-UK-price-paid)
* [&quot;Vues matérialisées ou projections&quot;](https://clickhouse.com/docs/managing-data/materialized-views-versus-projections)