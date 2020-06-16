---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Clause de jointure de tableau {#select-array-join-clause}

C'est une opération courante pour les tables qui contiennent une colonne de tableau pour produire une nouvelle table qui a une colonne avec chaque élément de tableau individuel de cette colonne initiale, tandis que les valeurs des autres colonnes sont dupliquées. C'est le cas de fond de ce `ARRAY JOIN` la clause le fait.

Son nom vient du fait qu'il peut être regardé comme l'exécution de `JOIN` avec un tableau ou une structure de données imbriquée. L'intention est similaire à la [arrayJoin](../../functions/array-join.md#functions_arrayjoin) fonction, mais la fonctionnalité de la clause est plus large.

Syntaxe:

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

Vous ne pouvez en spécifier qu'un `ARRAY JOIN` la clause dans un `SELECT` requête.

Types pris en charge de `ARRAY JOIN` sont énumérés ci-dessous:

-   `ARRAY JOIN` - Dans le cas de base, les tableaux vides ne sont pas inclus dans le résultat de `JOIN`.
-   `LEFT ARRAY JOIN` - Le résultat de `JOIN` contient des lignes avec des tableaux vides. La valeur d'un tableau vide est définie sur la valeur par défaut pour le type d'élément de tableau (généralement 0, chaîne vide ou NULL).

## Exemples de jointure de tableau de base {#basic-array-join-examples}

Les exemples ci-dessous illustrent l'utilisation de la `ARRAY JOIN` et `LEFT ARRAY JOIN` clause. Créons une table avec un [Tableau](../../../sql-reference/data-types/array.md) tapez colonne et insérez des valeurs dedans:

``` sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

``` text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

L'exemple ci-dessous utilise la `ARRAY JOIN` clause:

``` sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

``` text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

L'exemple suivant utilise l' `LEFT ARRAY JOIN` clause:

``` sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

``` text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

## À L'Aide D'Alias {#using-aliases}

Un alias peut être spécifié pour un tableau `ARRAY JOIN` clause. Dans ce cas, un élément de tableau peut être consulté par ce pseudonyme, mais le tableau lui-même est accessible par le nom d'origine. Exemple:

``` sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

``` text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

En utilisant des alias, vous pouvez effectuer `ARRAY JOIN` avec un groupe externe. Exemple:

``` sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

``` text
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

Plusieurs tableaux peuvent être séparés par des virgules `ARRAY JOIN` clause. Dans ce cas, `JOIN` est effectuée avec eux simultanément (la somme directe, pas le produit cartésien). Notez que tous les tableaux doivent avoir la même taille. Exemple:

``` sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

L'exemple ci-dessous utilise la [arrayEnumerate](../../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) fonction:

``` sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

## Jointure de tableau avec la Structure de données imbriquée {#array-join-with-nested-data-structure}

`ARRAY JOIN` fonctionne également avec [structures de données imbriquées](../../../sql-reference/data-types/nested-data-structures/nested.md):

``` sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

``` text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Lorsque vous spécifiez des noms de structures de données imbriquées dans `ARRAY JOIN` le sens est le même que `ARRAY JOIN` avec tous les éléments du tableau qui la compose. Des exemples sont énumérés ci-dessous:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Cette variation a également du sens:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

Un alias peut être utilisé pour une structure de données imbriquée, afin de sélectionner `JOIN` le résultat ou le tableau source. Exemple:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

Exemple d'utilisation de l' [arrayEnumerate](../../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) fonction:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

## Détails De Mise En Œuvre {#implementation-details}

L'ordre d'exécution de la requête est optimisé lors de l'exécution `ARRAY JOIN`. Bien `ARRAY JOIN` doit toujours être spécifié avant l' [WHERE](where.md)/[PREWHERE](prewhere.md) dans une requête, techniquement, ils peuvent être exécutés dans n'importe quel ordre, sauf résultat de `ARRAY JOIN` est utilisé pour le filtrage. L'ordre de traitement est contrôlée par l'optimiseur de requête.
