---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "Imbriqu\xE9e(Type1 Nom1, Nom2 Type2, ...)"
---

# Nested(name1 Type1, Name2 Type2, …) {#nestedname1-type1-name2-type2}

A nested data structure is like a table inside a cell. The parameters of a nested data structure – the column names and types – are specified the same way as in a [CREATE TABLE](../../../sql-reference/statements/create.md) requête. Chaque ligne de table peut correspondre à n'importe quel nombre de lignes dans une structure de données imbriquée.

Exemple:

``` sql
CREATE TABLE test.visits
(
    CounterID UInt32,
    StartDate Date,
    Sign Int8,
    IsNew UInt8,
    VisitID UInt64,
    UserID UInt64,
    ...
    Goals Nested
    (
        ID UInt32,
        Serial UInt32,
        EventTime DateTime,
        Price Int64,
        OrderID String,
        CurrencyID UInt32
    ),
    ...
) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192, Sign)
```

Cet exemple déclare le `Goals` structure de données imbriquée, qui contient des données sur les conversions (objectifs atteints). Chaque ligne de la ‘visits’ table peut correspondre à zéro ou n'importe quel nombre de conversions.

Un seul niveau d'imbrication est pris en charge. Les colonnes de structures imbriquées contenant des tableaux sont équivalentes à des tableaux multidimensionnels, elles ont donc un support limité (il n'y a pas de support pour stocker ces colonnes dans des tables avec le moteur MergeTree).

Dans la plupart des cas, lorsque vous travaillez avec une structure de données imbriquée, ses colonnes sont spécifiées avec des noms de colonnes séparés par un point. Ces colonnes constituent un tableau de types correspondants. Tous les tableaux de colonnes d'une structure de données imbriquée unique ont la même longueur.

Exemple:

``` sql
SELECT
    Goals.ID,
    Goals.EventTime
FROM test.visits
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

``` text
┌─Goals.ID───────────────────────┬─Goals.EventTime───────────────────────────────────────────────────────────────────────────┐
│ [1073752,591325,591325]        │ ['2014-03-17 16:38:10','2014-03-17 16:38:48','2014-03-17 16:42:27']                       │
│ [1073752]                      │ ['2014-03-17 00:28:25']                                                                   │
│ [1073752]                      │ ['2014-03-17 10:46:20']                                                                   │
│ [1073752,591325,591325,591325] │ ['2014-03-17 13:59:20','2014-03-17 22:17:55','2014-03-17 22:18:07','2014-03-17 22:18:51'] │
│ []                             │ []                                                                                        │
│ [1073752,591325,591325]        │ ['2014-03-17 11:37:06','2014-03-17 14:07:47','2014-03-17 14:36:21']                       │
│ []                             │ []                                                                                        │
│ []                             │ []                                                                                        │
│ [591325,1073752]               │ ['2014-03-17 00:46:05','2014-03-17 00:46:05']                                             │
│ [1073752,591325,591325,591325] │ ['2014-03-17 13:28:33','2014-03-17 13:30:26','2014-03-17 18:51:21','2014-03-17 18:51:45'] │
└────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```

Il est plus facile de penser à une structure de données imbriquée comme un ensemble de plusieurs tableaux de colonnes de la même longueur.

Le seul endroit où une requête SELECT peut spécifier le nom d'une structure de données imbriquée entière au lieu de colonnes individuelles est la clause de jointure de tableau. Pour plus d'informations, voir “ARRAY JOIN clause”. Exemple:

``` sql
SELECT
    Goal.ID,
    Goal.EventTime
FROM test.visits
ARRAY JOIN Goals AS Goal
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

``` text
┌─Goal.ID─┬──────Goal.EventTime─┐
│ 1073752 │ 2014-03-17 16:38:10 │
│  591325 │ 2014-03-17 16:38:48 │
│  591325 │ 2014-03-17 16:42:27 │
│ 1073752 │ 2014-03-17 00:28:25 │
│ 1073752 │ 2014-03-17 10:46:20 │
│ 1073752 │ 2014-03-17 13:59:20 │
│  591325 │ 2014-03-17 22:17:55 │
│  591325 │ 2014-03-17 22:18:07 │
│  591325 │ 2014-03-17 22:18:51 │
│ 1073752 │ 2014-03-17 11:37:06 │
└─────────┴─────────────────────┘
```

Vous ne pouvez pas effectuer SELECT pour une structure de données imbriquée entière. Vous ne pouvez lister explicitement que les colonnes individuelles qui en font partie.

Pour une requête INSERT, vous devez passer tous les tableaux de colonnes composant d'une structure de données imbriquée séparément (comme s'il s'agissait de tableaux de colonnes individuels). Au cours de l'insertion, le système vérifie qu'ils ont la même longueur.

Pour une requête DESCRIBE, les colonnes d'une structure de données imbriquée sont répertoriées séparément de la même manière.

La requête ALTER pour les éléments d'une structure de données imbriquée a des limites.

[Article Original](https://clickhouse.tech/docs/en/data_types/nested_data_structures/nested/) <!--hide-->
