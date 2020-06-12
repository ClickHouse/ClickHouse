---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: AggregatingMergeTree
---

# Aggregatingmergetree {#aggregatingmergetree}

Le moteur hérite de [MergeTree](mergetree.md#table_engines-mergetree), modifier la logique pour les parties de données Fusion. ClickHouse remplace toutes les lignes avec la même clé primaire (ou, plus précisément, avec la même [clé de tri](mergetree.md)) avec une seule ligne (dans un rayon d'une partie des données) qui stocke une combinaison d'états de fonctions d'agrégation.

Vous pouvez utiliser `AggregatingMergeTree` tables pour l'agrégation incrémentielle des données, y compris pour les vues matérialisées agrégées.

Le moteur traite toutes les colonnes avec les types suivants:

-   [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md)
-   [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md)

Il est approprié d'utiliser `AggregatingMergeTree` si elle réduit le nombre de lignes par commande.

## Création d'une Table {#creating-a-table}

``` sql
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

Pour une description des paramètres de requête, voir [demande de description](../../../sql-reference/statements/create.md).

**Les clauses de requête**

Lors de la création d'un `AggregatingMergeTree` la table de la même [clause](mergetree.md) sont nécessaires, comme lors de la création d'un `MergeTree` table.

<details markdown="1">

<summary>Méthode obsolète pour créer une Table</summary>

!!! attention "Attention"
    N'utilisez pas cette méthode dans les nouveaux projets et, si possible, remplacez les anciens projets par la méthode décrite ci-dessus.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

Tous les paramètres ont la même signification que dans `MergeTree`.
</details>

## Sélectionner et insérer {#select-and-insert}

Pour insérer des données, utilisez [INSERT SELECT](../../../sql-reference/statements/insert-into.md) requête avec l'ensemble-l'État des fonctions.
Lors de la sélection des données `AggregatingMergeTree` table, utilisez `GROUP BY` et les mêmes fonctions d'agrégat que lors de l'insertion de données, mais en utilisant `-Merge` suffixe.

Dans les résultats de `SELECT` requête, les valeurs de `AggregateFunction` type ont une représentation binaire spécifique à l'implémentation pour tous les formats de sortie ClickHouse. Si les données de vidage dans, par exemple, `TabSeparated` format avec `SELECT` requête alors ce vidage peut être chargé en utilisant `INSERT` requête.

## Exemple D'une vue matérialisée agrégée {#example-of-an-aggregated-materialized-view}

`AggregatingMergeTree` vue matérialisée qui regarde le `test.visits` table:

``` sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

Insertion de données dans la `test.visits` table.

``` sql
INSERT INTO test.visits ...
```

Les données sont insérées dans la table et la vue `test.basic` que va effectuer l'agrégation.

Pour obtenir les données agrégées, nous devons exécuter une requête telle que `SELECT ... GROUP BY ...` à partir de la vue `test.basic`:

``` sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/aggregatingmergetree/) <!--hide-->
