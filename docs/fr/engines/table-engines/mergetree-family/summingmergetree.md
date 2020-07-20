---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: SummingMergeTree
---

# SummingMergeTree {#summingmergetree}

Le moteur hérite de [MergeTree](mergetree.md#table_engines-mergetree). La différence est que lors de la fusion de parties de données pour `SummingMergeTree` tables ClickHouse remplace toutes les lignes avec la même clé primaire (ou, plus précisément, avec la même [clé de tri](mergetree.md)) avec une ligne qui contient des valeurs résumées pour les colonnes avec le type de données numériques. Si la clé de tri est composée de telle sorte qu'une seule valeur de clé correspond à un grand nombre de lignes, cela réduit considérablement le volume de stockage et accélère la sélection des données.

Nous vous recommandons d'utiliser le moteur avec `MergeTree`. Stocker des données complètes dans `MergeTree` table, et l'utilisation `SummingMergeTree` pour le stockage de données agrégées, par exemple, lors de la préparation de rapports. Une telle approche vous empêchera de perdre des données précieuses en raison d'une clé primaire mal composée.

## Création d'une Table {#creating-a-table}

``` sql
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

Pour une description des paramètres de requête, voir [demande de description](../../../sql-reference/statements/create.md).

**Paramètres de SummingMergeTree**

-   `columns` - un n-uplet avec les noms de colonnes où les valeurs seront résumées. Paramètre facultatif.
    Les colonnes doivent être d'un type numérique et ne doit pas être dans la clé primaire.

    Si `columns` non spécifié, ClickHouse résume les valeurs dans toutes les colonnes avec un type de données numérique qui ne sont pas dans la clé primaire.

**Les clauses de requête**

Lors de la création d'un `SummingMergeTree` la table de la même [clause](mergetree.md) sont nécessaires, comme lors de la création d'un `MergeTree` table.

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
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

Tous les paramètres excepté `columns` ont la même signification que dans `MergeTree`.

-   `columns` — tuple with names of columns values of which will be summarized. Optional parameter. For a description, see the text above.

</details>

## Exemple D'Utilisation {#usage-example}

Considérons le tableau suivant:

``` sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

Insérer des données:

``` sql
INSERT INTO summtt Values(1,1),(1,2),(2,1)
```

ClickHouse peut résumer toutes les lignes pas complètement ([voir ci-dessous](#data-processing)), nous utilisons donc une fonction d'agrégation `sum` et `GROUP BY` la clause dans la requête.

``` sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

``` text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

## Le Traitement Des Données {#data-processing}

Lorsque les données sont insérées dans une table, elles sont enregistrées telles quelles. Clickhouse fusionne périodiquement les parties de données insérées et c'est à ce moment que les lignes avec la même clé primaire sont additionnées et remplacées par une pour chaque partie de données résultante.

ClickHouse can merge the data parts so that different resulting parts of data cat consist rows with the same primary key, i.e. the summation will be incomplete. Therefore (`SELECT`) une fonction d'agrégation [somme()](../../../sql-reference/aggregate-functions/reference.md#agg_function-sum) et `GROUP BY` la clause doit être utilisé dans une requête comme décrit dans l'exemple ci-dessus.

### Règles communes pour la sommation {#common-rules-for-summation}

Les valeurs dans les colonnes avec le type de données numériques sont résumées. L'ensemble des colonnes est défini par le paramètre `columns`.

Si les valeurs étaient 0 dans toutes les colonnes pour la sommation, la ligne est supprimée.

Si la colonne n'est pas dans la clé primaire et n'est pas résumée, une valeur arbitraire est sélectionnée parmi celles existantes.

Les valeurs ne sont pas résumés des colonnes de la clé primaire.

### La somme dans les colonnes Aggregatefunction {#the-summation-in-the-aggregatefunction-columns}

Pour les colonnes de [Type AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md) ClickHouse se comporte comme [AggregatingMergeTree](aggregatingmergetree.md) moteur d'agrégation selon la fonction.

### Structures Imbriquées {#nested-structures}

Table peut avoir des structures de données imbriquées qui sont traitées d'une manière spéciale.

Si le nom d'une table imbriquée se termine avec `Map` et il contient au moins deux colonnes qui répondent aux critères suivants:

-   la première colonne est numérique `(*Int*, Date, DateTime)` ou une chaîne de caractères `(String, FixedString)`, nous allons l'appeler `key`,
-   les autres colonnes sont arithmétique `(*Int*, Float32/64)`, nous allons l'appeler `(values...)`,

ensuite, cette table imbriquée est interprétée comme un mappage de `key => (values...)` et lors de la fusion de ses lignes, les éléments de deux ensembles de données sont regroupées par `key` avec une sommation du correspondant `(values...)`.

Exemple:

``` text
[(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
[(1, 100)] + [(1, 150)] -> [(1, 250)]
[(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
[(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
```

Lorsque vous demandez des données, utilisez [sumMap (clé, valeur)](../../../sql-reference/aggregate-functions/reference.md) fonction pour l'agrégation de `Map`.

Pour la structure de données imbriquée, vous n'avez pas besoin de spécifier ses colonnes dans le tuple de colonnes pour la sommation.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/summingmergetree/) <!--hide-->
