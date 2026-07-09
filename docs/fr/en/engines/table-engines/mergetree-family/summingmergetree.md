---
description: "SummingMergeTree hérite du moteur MergeTree. Sa principale caractéristique
  est sa capacité à additionner automatiquement les données numériques lors des fusions de parties de données."
sidebar_label: 'SummingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/summingmergetree
title: 'Moteur de table SummingMergeTree'
doc_type: 'reference'
---

Le moteur hérite de [MergeTree](/fr/engines/table-engines/mergetree-family/mergetree). La différence est que, lors de la fusion des parties de données des tables `SummingMergeTree`, ClickHouse remplace toutes les lignes ayant la même clé primaire (ou, plus précisément, la même [clé de tri](../../../engines/table-engines/mergetree-family/mergetree.md)) par une seule ligne contenant les valeurs additionnées des colonnes de type de données numérique. Si la clé de tri est définie de sorte qu&#39;une même valeur de clé corresponde à un grand nombre de lignes, cela réduit considérablement le volume de stockage et accélère la lecture des données.

Nous recommandons d&#39;utiliser ce moteur avec `MergeTree`. Stockez les données complètes dans une table `MergeTree` et utilisez `SummingMergeTree` pour stocker des données agrégées, par exemple lors de la préparation de rapports. Cette approche vous évitera de perdre des données précieuses à cause d&#39;une clé primaire mal définie.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Pour une description des paramètres de la requête, consultez la [description de la requête](../../../sql-reference/statements/create/table.md).

<div id="parameters-of-summingmergetree">
  ### Paramètres de SummingMergeTree
</div>

<div id="columns">
  #### Colonnes
</div>

`columns` - un tuple contenant les noms des colonnes dont les valeurs seront additionnées. Paramètre facultatif.
Les colonnes doivent être de type numérique et ne doivent figurer ni dans la partition ni dans la clé de tri.

Si `columns` n&#39;est pas spécifié, ClickHouse additionne les valeurs de toutes les colonnes de type numérique qui ne figurent pas dans la clé de tri.

<div id="query-clauses">
  ### Clauses de requête
</div>

Lors de la création d&#39;une table `SummingMergeTree`, les mêmes [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) sont requises que pour la création d&#39;une table `MergeTree`.

<details markdown="1">
  <summary>Méthode obsolète de création d&#39;une table</summary>

  :::note
  N&#39;utilisez pas cette méthode pour de nouveaux projets et, si possible, migrez les anciens projets vers la méthode décrite ci-dessus.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  Tous les paramètres, à l&#39;exception de `columns`, ont le même sens que dans `MergeTree`.

  * `columns` — tuple contenant les noms des colonnes dont les valeurs seront additionnées. Paramètre facultatif. Pour une description, voir le texte ci-dessus.
</details>

<div id="usage-example">
  ## Exemple d’utilisation
</div>

Prenons la table suivante :

```sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

Insérez-y des données :

```sql
INSERT INTO summtt VALUES(1,1),(1,2),(2,1)
```

ClickHouse peut ne pas additionner complètement toutes les lignes ([voir ci-dessous](#data-processing)) ; nous utilisons donc une fonction d’agrégation `sum` et la clause `GROUP BY` dans la requête.

```sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

```text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

<div id="data-processing">
  ## Traitement des données
</div>

Lorsque des données sont insérées dans une table, elles sont enregistrées telles quelles. ClickHouse fusionne périodiquement les parties de données insérées, et c’est à ce moment-là que les lignes ayant la même clé primaire sont additionnées puis remplacées par une seule ligne dans chaque partie de données résultante.

ClickHouse peut fusionner les parties de données de telle sorte que différentes parties de données résultantes puissent contenir des lignes ayant la même clé primaire ; autrement dit, la sommation sera incomplète. Par conséquent, (`SELECT`) une fonction d’agrégation [sum()](/fr/sql-reference/aggregate-functions/reference/sum) et la clause `GROUP BY` doivent être utilisées dans une requête, comme décrit dans l’exemple ci-dessus.

<div id="common-rules-for-summation">
  ### Règles communes de sommation
</div>

Les valeurs des colonnes de type de données numérique sont additionnées. L’ensemble des colonnes est défini par le paramètre `columns`.

Si les valeurs sont égales à 0 dans toutes les colonnes à sommer, la ligne est supprimée.

Si une colonne ne fait pas partie de la clé primaire et n’est pas additionnée, une valeur arbitraire est choisie parmi les valeurs existantes.

Les valeurs des colonnes de la clé primaire ne sont pas additionnées.

<div id="the-summation-in-the-aggregatefunction-columns">
  ### La sommation des colonnes AggregateFunction
</div>

Pour les colonnes de [type AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md), ClickHouse se comporte comme le moteur [AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md) et agrège les données selon la fonction.

<div id="nested-structures">
  ### Structures imbriquées
</div>

Une table peut comporter des structures de données imbriquées, traitées d’une manière particulière.

Si le nom d’une table imbriquée se termine par `Map` et qu’elle contient au moins deux colonnes répondant aux critères suivants :

* la première colonne est numérique `(*Int*, Date, DateTime)` ou de type chaîne `(String, FixedString)` ; appelons-la `key`,
* les autres colonnes sont arithmétiques `(*Int*, Float32/64)` ; appelons-les `(values...)`,

alors cette table imbriquée est interprétée comme une association `key => (values...)` et, lors de la fusion de ses lignes, les éléments de deux ensembles de données sont fusionnés par `key` en additionnant les `(values...)` correspondantes.

Exemples :

```text
DROP TABLE IF EXISTS nested_sum;
CREATE TABLE nested_sum
(
    date Date,
    site UInt32,
    hitsMap Nested(
        browser String,
        imps UInt32,
        clicks UInt32
    )
) ENGINE = SummingMergeTree
PRIMARY KEY (date, site);

INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Firefox', 'Opera'], [10, 5], [2, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Chrome', 'Firefox'], [20, 1], [1, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['IE'], [22], [0]);
INSERT INTO nested_sum VALUES ('2020-01-01', 10, ['Chrome'], [4], [3]);

OPTIMIZE TABLE nested_sum FINAL; -- emulate merge 

SELECT * FROM nested_sum;
┌───────date─┬─site─┬─hitsMap.browser───────────────────┬─hitsMap.imps─┬─hitsMap.clicks─┐
│ 2020-01-01 │   10 │ ['Chrome']                        │ [4]          │ [3]            │
│ 2020-01-01 │   12 │ ['Chrome','Firefox','IE','Opera'] │ [20,11,22,5] │ [1,3,0,1]      │
└────────────┴──────┴───────────────────────────────────┴──────────────┴────────────────┘

SELECT
    site,
    browser,
    impressions,
    clicks
FROM
(
    SELECT
        site,
        sumMap(hitsMap.browser, hitsMap.imps, hitsMap.clicks) AS imps_map
    FROM nested_sum
    GROUP BY site
)
ARRAY JOIN
    imps_map.1 AS browser,
    imps_map.2 AS impressions,
    imps_map.3 AS clicks;

┌─site─┬─browser─┬─impressions─┬─clicks─┐
│   12 │ Chrome  │          20 │      1 │
│   12 │ Firefox │          11 │      3 │
│   12 │ IE      │          22 │      0 │
│   12 │ Opera   │           5 │      1 │
│   10 │ Chrome  │           4 │      3 │
└──────┴─────────┴─────────────┴────────┘
```

Lors de la requête des données, utilisez la fonction [sumMap(key, value)](../../../sql-reference/aggregate-functions/reference/sumMappedArrays.md) pour agréger `Map`.

Pour une structure de données imbriquée, vous n’avez pas besoin de spécifier ses colonnes dans le tuple de colonnes pour la sommation.

<div id="tuple-element-aggregation">
  ### Agrégation des éléments de Tuple
</div>

Lorsque le paramètre `allow_tuple_element_aggregation` est activé, les colonnes `Tuple` sont aplaties récursivement afin que chaque élément terminal participe indépendamment à la somme. Cela permet de stocker plusieurs métriques dans une seule colonne `Tuple` et de les additionner élément par élément lors des fusions.

Les mêmes règles s&#39;appliquent aux sous-colonnes aplaties qu&#39;aux colonnes ordinaires :

* Seules les sous-colonnes numériques sont additionnées.
* Les sous-colonnes appartenant à un `Tuple` présent dans la clé de tri ou la clé de partition sont exclues de la somme.
* Si `columns` est spécifié, seules les sous-colonnes des colonnes `Tuple` listées sont additionnées.
* Si toutes les sous-colonnes numériques d&#39;une ligne valent zéro après la somme, la ligne est supprimée.

:::note
Ce paramètre est immuable et doit être spécifié lors de la création de la table.
:::

```sql
CREATE TABLE summing_tuples
(
    key UInt32,
    metrics Tuple(
        impressions UInt64,
        clicks UInt64,
        nested Tuple(
            conversions UInt64
        )
    )
) ENGINE = SummingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO summing_tuples VALUES (1, (100, 10, (1)));
INSERT INTO summing_tuples VALUES (1, (200, 20, (3)));

OPTIMIZE TABLE summing_tuples FINAL;

SELECT key, metrics.impressions, metrics.clicks, metrics.nested.conversions FROM summing_tuples;
```

```text
┌─key─┬─metrics.impressions─┬─metrics.clicks─┬─metrics.nested.conversions─┐
│   1 │                 300 │             30 │                          4 │
└─────┴─────────────────────┴────────────────┴────────────────────────────┘
```

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Utiliser les combinateurs d’agrégation dans ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)