---
description: 'Remplace toutes les lignes ayant la même clé primaire (ou, plus précisément, la même [clé de tri](../../../engines/table-engines/mergetree-family/mergetree.md))
  par une seule ligne (au sein d''une même part de données) qui stocke une combinaison d''états
  de fonctions d''agrégation.'
sidebar_label: 'AggregatingMergeTree'
sidebar_position: 60
slug: /engines/table-engines/mergetree-family/aggregatingmergetree
title: 'Moteur de table AggregatingMergeTree'
doc_type: 'référence'
---

Le moteur hérite de [MergeTree](/fr/engines/table-engines/mergetree-family/mergetree) et en modifie la logique de fusion des parts de données. ClickHouse remplace toutes les lignes ayant la même clé primaire (ou, plus précisément, la même [clé de tri](../../../engines/table-engines/mergetree-family/mergetree.md)) par une seule ligne (au sein d’une même part de données) qui stocke une combinaison d’états de fonctions d’agrégation.

Vous pouvez utiliser les tables `AggregatingMergeTree` pour l’agrégation incrémentielle des données, notamment pour les vues matérialisées agrégées.

Vous trouverez ci-dessous une vidéo montrant comment utiliser AggregatingMergeTree et les fonctions d’agrégation :

<div class="vimeo-container">
  <iframe width="1030" height="579" src="https://www.youtube.com/embed/pryhI4F_zqQ" title="États d’agrégation dans ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />
</div>

Le moteur traite toutes les colonnes des types suivants :

* [`AggregateFunction`](../../../sql-reference/data-types/aggregatefunction.md)
* [`SimpleAggregateFunction`](../../../sql-reference/data-types/simpleaggregatefunction.md)

Il est pertinent d’utiliser `AggregatingMergeTree` si cela réduit le nombre de lignes de plusieurs ordres de grandeur.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

Pour une description des paramètres de la requête, consultez la [description de la requête](../../../sql-reference/statements/create/table.md).

**Clauses de requête**

Lors de la création d’une table `AggregatingMergeTree`, les mêmes [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) sont requises que pour la création d’une table `MergeTree`.

<details markdown="1">
  <summary>Méthode obsolète de création d’une table</summary>

  :::note
  N’utilisez pas cette méthode dans de nouveaux projets et, si possible, migrez les anciens projets vers la méthode décrite ci-dessus.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  Tous les paramètres ont la même signification que dans `MergeTree`.
</details>

<div id="select-and-insert">
  ## SELECT and INSERT
</div>

Pour insérer des données, utilisez une requête [INSERT SELECT](../../../sql-reference/statements/insert-into.md) avec des fonctions d’agrégation en `-State`.
Lors de la sélection de données dans une table `AggregatingMergeTree`, utilisez la clause `GROUP BY` et les mêmes fonctions d’agrégation que pour l’insertion des données, mais avec le suffixe `-Merge`.

Dans les résultats d’une requête `SELECT`, les valeurs de type `AggregateFunction` ont une représentation binaire propre à l’implémentation dans tous les formats de sortie de ClickHouse. Par exemple, si vous exportez des données au format `TabSeparated` avec une requête `SELECT`, cet export peut ensuite être rechargé à l’aide d’une requête `INSERT`.

<div id="example-of-an-aggregated-materialized-view">
  ## Exemple de vue matérialisée agrégée
</div>

L’exemple suivant suppose que vous disposez d’une base de données appelée `test`. Créez-la si elle n’existe pas déjà à l’aide de la commande ci-dessous :

```sql
CREATE DATABASE test;
```

Créez maintenant la table `test.visits`, qui contient les données brutes :

```sql
CREATE TABLE test.visits
 (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Sign Nullable(Int32),
    UserID Nullable(Int32)
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);
```

Ensuite, vous avez besoin d&#39;une table `AggregatingMergeTree` qui stockera des `AggregationFunction` permettant de suivre le nombre total de visites et le nombre d&#39;utilisateurs uniques.

Créez une vue matérialisée `AggregatingMergeTree` qui surveille la table `test.visits` et utilise le type [`AggregateFunction`](/fr/sql-reference/data-types/aggregatefunction) :

```sql
CREATE TABLE test.agg_visits (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Visits AggregateFunction(sum, Nullable(Int32)),
    Users AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID);
```

Créez une vue matérialisée qui alimente `test.agg_visits` à partir de `test.visits` :

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;
```

Insérez des données dans la table `test.visits` :

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1667446031000, 1, 3, 4), (1667446031000, 1, 6, 3);
```

Les données sont insérées dans `test.visits` et `test.agg_visits`.

Pour obtenir les données agrégées, exécutez une requête telle que `SELECT ... GROUP BY ...` à partir de la vue matérialisée `test.visits_mv` :

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.visits_mv
GROUP BY StartDate
ORDER BY StartDate;
```

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │      9 │     2 │
└─────────────────────────┴────────┴───────┘
```

Ajoutez encore deux enregistrements à `test.visits`, mais cette fois, essayez d’utiliser un timestamp différent pour l’un des enregistrements :

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1669446031000, 2, 5, 10), (1667446031000, 3, 7, 5);
```

Exécutez à nouveau la requête `SELECT`, qui renverra la sortie suivante :

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │     16 │     3 │
│ 2022-11-26 07:00:31.000 │      5 │     1 │
└─────────────────────────┴────────┴───────┘
```

Dans certains cas, vous pouvez vouloir éviter de pré-agréger les lignes au moment de l’insertion afin de reporter le coût de l’agrégation du moment de l’insertion
au moment de la fusion. Normalement, il faut inclure dans la clause `GROUP BY` les colonnes qui ne font pas partie de l’agrégation
dans la définition de la vue matérialisée afin d’éviter une erreur. Toutefois, vous pouvez utiliser la fonction [`initializeAggregation`](/fr/sql-reference/functions/other-functions#initializeAggregation)
avec le paramètre `optimize_on_insert = 0` (activé par défaut) pour y parvenir. Dans ce cas, l’utilisation de `GROUP BY`
n’est plus nécessaire :

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    initializeAggregation('sumState', Sign) AS Visits,
    initializeAggregation('uniqState', UserID) AS Users
FROM test.visits;
```

:::note
Lors de l’utilisation de `initializeAggregation`, un état d’agrégation est créé pour chaque ligne, sans regroupement.
Chaque ligne source produit une ligne dans la vue matérialisée, et l’agrégation proprement dite a lieu plus tard, lorsque
`AggregatingMergeTree` fusionne les parts. Cela n’est vrai que si `optimize_on_insert = 0`.
:::

<div id="tuple-element-aggregation">
  ## Agrégation des éléments de Tuple
</div>

Lorsque le paramètre `allow_tuple_element_aggregation` est activé, les colonnes `Tuple` sont aplaties de manière récursive afin que chaque élément terminal participe indépendamment à l’agrégation. Cela signifie que les sous-colonnes `AggregateFunction` ou `SimpleAggregateFunction` à l’intérieur d’un `Tuple` sont agrégées selon leurs fonctions respectives, comme si elles étaient des colonnes de premier niveau.

Les sous-colonnes appartenant à un `Tuple` dans la clé de tri sont exclues de l’agrégation. Les sous-colonnes non agrégées sont traitées comme des colonnes ordinaires (leur première valeur est conservée).

:::note
Ce paramètre est immuable et doit être spécifié lors de la création de la table.
:::

```sql
CREATE TABLE agg_tuples
(
    key UInt32,
    metrics Tuple(
        total_visits SimpleAggregateFunction(sum, UInt64),
        unique_users SimpleAggregateFunction(max, UInt64)
    )
) ENGINE = AggregatingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO agg_tuples VALUES (1, (100, 5));
INSERT INTO agg_tuples VALUES (1, (200, 8));
INSERT INTO agg_tuples VALUES (2, (50, 3));

OPTIMIZE TABLE agg_tuples FINAL;

SELECT key, metrics.total_visits, metrics.unique_users FROM agg_tuples ORDER BY key;
```

```text
┌─key─┬─metrics.total_visits─┬─metrics.unique_users─┐
│   1 │                  300 │                    8 │
│   2 │                   50 │                    3 │
└─────┴──────────────────────┴──────────────────────┘
```

`total_visits` est agrégé à l’aide de `sum` (100 + 200 = 300), tandis que `unique_users` est agrégé avec `max` (max(5, 8) = 8).

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Utiliser les combinateurs d’agrégation dans ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)