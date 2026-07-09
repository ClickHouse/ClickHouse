---
description: 'Vue d’ensemble des structures de données imbriquées dans ClickHouse'
sidebar_label: 'Nested(Name1 Type1, Name2 Type2, ...)'
sidebar_position: 57
slug: /sql-reference/data-types/nested-data-structures/nested
title: 'Nested(name1 Type1, Name2 Type2, ...)'
doc_type: 'guide'
---

Une structure de données imbriquée s’apparente à une table à l’intérieur d’une cellule. Les paramètres d’une structure de données imbriquée — les noms et les types de colonnes — sont spécifiés de la même manière que dans une requête [CREATE TABLE](../../../sql-reference/statements/create/table.md). Chaque ligne de table peut correspondre à un nombre quelconque de lignes dans une structure de données imbriquée.

:::tip[Évitez d’utiliser des points dans les noms de colonnes]
Les noms de colonnes contenant des points, les colonnes partageant un préfixe commun suivi d’un point, et les colonnes de type `Array` peuvent être interprétés comme faisant partie d’une structure Nested aplatie lorsque `flatten_nested = 1` (valeur par défaut). Cela peut entraîner une validation inattendue de la longueur des tableaux lors des insertions, ainsi que des restrictions de renommage.

Évitez si possible d’utiliser des points dans les noms de colonnes.
Utilisez des underscores (`_`) ou un autre séparateur à la place des points dans les noms de colonnes, sauf si vous avez explicitement besoin de la sémantique `Nested`.
:::

Exemple :

```sql
CREATE TABLE test.visits(
  CounterID UInt32,
  StartDate Date,
  Sign Int8,
  IsNew UInt8,
  VisitID UInt64,
  UserID UInt64,
--highlight-start
  Goals Nested(
    ID UInt32,
    Serial UInt32,
    EventTime DateTime,
    Price Int64,
    OrderID String,
    CurrencyID UInt32
  )
--highlight-end
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY (StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID));

INSERT INTO test.visits
(CounterID, StartDate, Sign, IsNew, VisitID, UserID, Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    (101500, '2014-03-17', 1, 1, 1001, 100001, [1073752, 591325, 591325], [1, 2, 3], ['2014-03-17 16:38:10', '2014-03-17 16:38:48', '2014-03-17 16:42:27'], [0, 0, 0], ['', '', ''], [0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1002, 100002, [1073752], [1], ['2014-03-17 00:28:25'], [0], [''], [0]),
    (101500, '2014-03-17', 1, 0, 1003, 100003, [1073752], [1], ['2014-03-17 10:46:20'], [0], [''], [0]),
    (101500, '2014-03-17', 1, 1, 1004, 100004, [1073752, 591325, 591325, 591325], [1, 2, 3, 4], ['2014-03-17 13:59:20', '2014-03-17 22:17:55', '2014-03-17 22:18:07', '2014-03-17 22:18:51'], [0, 0, 0, 0], ['', '', '', ''], [0, 0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1005, 100005, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 0, 1006, 100006, [1073752, 591325, 591325], [1, 2, 3], ['2014-03-17 11:37:06', '2014-03-17 14:07:47', '2014-03-17 14:36:21'], [0, 0, 0], ['', '', ''], [0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1007, 100007, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 0, 1008, 100008, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 1, 1009, 100009, [591325, 1073752], [1, 2], ['2014-03-17 00:46:05', '2014-03-17 00:46:05'], [0, 0], ['', ''], [0, 0]),
    (101500, '2014-03-17', 1, 1, 1010, 100010, [1073752, 591325, 591325, 591325], [1, 2, 3, 4], ['2014-03-17 13:28:33', '2014-03-17 13:30:26', '2014-03-17 18:51:21', '2014-03-17 18:51:45'], [0, 0, 0, 0], ['', '', '', ''], [0, 0, 0, 0]);
```

L’instruction DDL `CREATE TABLE` ci-dessus déclare la structure de données imbriquée `Goals`, qui contient des données sur les conversions, c’est-à-dire les objectifs atteints.
Chaque ligne de la table &#39;visits&#39; correspond à aucune, une ou plusieurs conversions.

Lorsque le paramètre [`flatten_nested`](/fr/operations/settings/settings#flatten_nested) est défini sur `0` (`flatten_nested=1` par défaut), n’importe quel niveau d’imbrication est pris en charge.

Dans la plupart des cas, lorsque vous travaillez avec une structure de données imbriquée, ses colonnes sont spécifiées à l’aide de noms de colonnes séparés par un point.
Ces colonnes forment un tableau de types correspondants.
Tous les tableaux de colonnes d’une même structure de données imbriquée ont la même longueur.

Par exemple :

```sql
SELECT
    Goals.ID,
    Goals.EventTime
FROM test.visits
WHERE CounterID = 101500 AND length(Goals.ID) < 5
ORDER BY VisitID
LIMIT 10
```

```text
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Goals.ID                       ┃ Goals.EventTime                                                                           ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
 1. │ [1073752,591325,591325]        │ ['2014-03-17 16:38:10','2014-03-17 16:38:48','2014-03-17 16:42:27']                       │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 2. │ [1073752]                      │ ['2014-03-17 00:28:25']                                                                   │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 3. │ [1073752]                      │ ['2014-03-17 10:46:20']                                                                   │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 4. │ [1073752,591325,591325,591325] │ ['2014-03-17 13:59:20','2014-03-17 22:17:55','2014-03-17 22:18:07','2014-03-17 22:18:51'] │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 5. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 6. │ [1073752,591325,591325]        │ ['2014-03-17 11:37:06','2014-03-17 14:07:47','2014-03-17 14:36:21']                       │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 7. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 8. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 9. │ [591325,1073752]               │ ['2014-03-17 00:46:05','2014-03-17 00:46:05']                                             │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
10. │ [1073752,591325,591325,591325] │ ['2014-03-17 13:28:33','2014-03-17 13:30:26','2014-03-17 18:51:21','2014-03-17 18:51:45'] │
    └────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```

:::tip
Le plus simple est de voir une structure de données imbriquée comme un ensemble de plusieurs tableaux de colonnes de même longueur.
:::

<div id="filtering-nested-columns-in-where">
  ### Filtrage des colonnes Nested dans WHERE
</div>

Comme chaque colonne d&#39;une structure `Nested` est stockée sous forme d&#39;`Array`, la référencer dans une clause `WHERE` renvoie le tableau entier pour chaque ligne, et non un élément individuel. Vous ne pouvez pas comparer directement une colonne imbriquée à une valeur scalaire ; vous devez donc utiliser des [fonctions sur les tableaux](/fr/sql-reference/functions/array-functions).

Par exemple, cette requête ne se contente **pas** de ne renvoyer aucune ligne de façon silencieuse — elle génère une exception, car `Goals.ID` est de type `Array(UInt32)` et `equals(Array(UInt32), UInt32)` n&#39;est pas une comparaison valide :

```sql
-- WRONG: compares the entire Array to a scalar
SELECT * FROM test.visits
WHERE Goals.ID = 591325;
```

```text
Code: 43. DB::Exception: Illegal types of arguments (`Array(UInt32)`, `UInt32`)
of function `equals`. (ILLEGAL_TYPE_OF_ARGUMENT)
```

Utilisez [`has`](/fr/sql-reference/functions/array-functions#has) pour vérifier si un tableau contient une valeur donnée :

```sql
-- Find visits that have at least one goal with ID 591325
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE has(Goals.ID, 591325);
```

Utilisez [`arrayExists`](/fr/sql-reference/functions/array-functions#arrayExists) si la condition est plus complexe :

```sql
-- Find visits that have at least one goal with ID greater than 1000000
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE arrayExists(id -> id > 1000000, Goals.ID);
```

Vous pouvez filtrer sur la longueur du tableau avec `length` ou exclure les tableaux vides avec `notEmpty` :

```sql
-- Visits with at least 3 goals
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE length(Goals.ID) >= 3;

-- Visits with at least one goal (non-empty array)
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE notEmpty(Goals.ID);
```

Pour filtrer des éléments individuels d&#39;une structure imbriquée plutôt que des lignes entières, utilisez `ARRAY JOIN` pour dérouler d&#39;abord les tableaux.
Après `ARRAY JOIN`, chaque élément devient une ligne distincte, de sorte que la clause `WHERE` s&#39;applique à des valeurs scalaires.
Pour plus d&#39;informations, voir la [clause `ARRAY JOIN`](/fr/sql-reference/statements/select/array-join). Exemple :

```sql
SELECT
    Goal.ID,
    Goal.EventTime
FROM test.visits
ARRAY JOIN Goals AS Goal
WHERE CounterID = 101500 AND length(Goals.ID) < 5
ORDER BY VisitID, Goal.Serial
LIMIT 10
```

```text
    ┏━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Goal.ID ┃      Goal.EventTime ┃
    ┡━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
 1. │ 1073752 │ 2014-03-17 16:38:10 │
    ├─────────┼─────────────────────┤
 2. │  591325 │ 2014-03-17 16:38:48 │
    ├─────────┼─────────────────────┤
 3. │  591325 │ 2014-03-17 16:42:27 │
    ├─────────┼─────────────────────┤
 4. │ 1073752 │ 2014-03-17 00:28:25 │
    ├─────────┼─────────────────────┤
 5. │ 1073752 │ 2014-03-17 10:46:20 │
    ├─────────┼─────────────────────┤
 6. │ 1073752 │ 2014-03-17 13:59:20 │
    ├─────────┼─────────────────────┤
 7. │  591325 │ 2014-03-17 22:17:55 │
    ├─────────┼─────────────────────┤
 8. │  591325 │ 2014-03-17 22:18:07 │
    ├─────────┼─────────────────────┤
 9. │  591325 │ 2014-03-17 22:18:51 │
    ├─────────┼─────────────────────┤
10. │ 1073752 │ 2014-03-17 11:37:06 │
    └─────────┴─────────────────────┘
```

Vous ne pouvez pas exécuter `SELECT` sur une structure de données imbriquée dans son ensemble. Vous pouvez uniquement lister explicitement les colonnes individuelles qui en font partie.

<div id="inserting-data">
  ### Insertion de données
</div>

Pour une requête `INSERT`, vous devez transmettre séparément tous les tableaux des colonnes composantes d&#39;une structure de données imbriquée (comme s&#39;il s&#39;agissait de tableaux de colonnes individuels). Lors de l&#39;insertion, le système vérifie qu&#39;ils ont la même longueur.

Chaque sous-colonne imbriquée est répertoriée dans la liste des colonnes à l&#39;aide de la notation pointée (`Goals.ID`, `Goals.Serial`, ...), et les valeurs correspondantes sont des tableaux :

```sql
INSERT INTO test.visits
    (CounterID, StartDate, Sign, IsNew, VisitID, UserID,
     Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    -- A visit with two goals: each nested sub-column gets an array of length 2
    (101500, '2014-03-18', 1, 1, 2001, 200001,
     [1073752, 591325], [1, 2],
     ['2014-03-18 10:00:00', '2014-03-18 10:05:00'],
     [100, 200], ['order_a', 'order_b'], [1, 2]),
    -- A visit with no goals: all nested sub-columns get empty arrays
    (101500, '2014-03-18', 1, 0, 2002, 200002,
     [], [], [], [], [], []);
```

Tous les tableaux de sous-colonnes imbriquées d’une même ligne doivent avoir la même longueur. Des longueurs différentes entraînent une erreur :

```sql
-- ERROR: Goals.ID has 2 elements, but Goals.Serial has 1
INSERT INTO test.visits
    (CounterID, StartDate, Sign, IsNew, VisitID, UserID,
     Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    (101500, '2014-03-18', 1, 1, 2003, 200003,
     [1073752, 591325], [1],
     ['2014-03-18 12:00:00'], [0], [''], [0]);
```

Pour une requête `DESCRIBE`, les colonnes d’une structure de données imbriquée sont listées séparément, de la même façon.

<div id="alter-limitations">
  ### Limitations d’ALTER
</div>

Les requêtes `ALTER` sur les structures de données imbriquées présentent les limitations suivantes :

**L’ajout de sous-colonnes** fonctionne normalement. Vous pouvez ajouter une nouvelle sous-colonne à une structure `Nested` existante :

```sql
ALTER TABLE test.visits ADD COLUMN Goals.Revenue Float64;
```

**La suppression de sous-colonnes** s’applique aux sous-colonnes individuelles :

```sql
ALTER TABLE test.visits DROP COLUMN Goals.Revenue;
```

**Le changement de type** d’une sous-colonne est possible et déclenche une mutation (réécriture des données) :

```sql
ALTER TABLE test.visits MODIFY COLUMN Goals.Price Int32;
```

Le **renommage** présente des restrictions. Vous pouvez renommer une sous-colonne au sein de la même structure imbriquée :

```sql
-- OK: stays within the Goals structure
ALTER TABLE test.visits RENAME COLUMN Goals.Price TO Goals.Amount;
```

Cependant, vous **ne pouvez pas** :

* Renommer la structure imbriquée elle-même dans son ensemble (par exemple, `Goals` en `Conversions`).
* Déplacer une sous-colonne vers une autre structure imbriquée (par exemple, `Goals.ID` vers `OtherNested.ID`).
* Déplacer une sous-colonne hors d’une structure imbriquée ou dans une structure imbriquée (par exemple, `Goals.ID` vers `GoalID`, ou inversement).