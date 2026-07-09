---
description: 'Documentation de la clause ARRAY JOIN'
sidebar_label: 'ARRAY JOIN'
slug: /sql-reference/statements/select/array-join
title: 'Clause ARRAY JOIN'
doc_type: 'reference'
---

Pour les tables qui contiennent une colonne Array, il est courant de produire une nouvelle table comportant une ligne pour chaque élément du tableau de cette colonne initiale, tandis que les valeurs des autres colonnes sont dupliquées. C’est le cas de base de ce que fait la clause `ARRAY JOIN`.

Son nom vient du fait qu’on peut la considérer comme l’exécution d’un `JOIN` avec un tableau ou une structure de données imbriquée. Le principe est similaire à celui de la fonction [arrayJoin](/fr/sql-reference/functions/array-join), mais la clause offre des fonctionnalités plus étendues.

Syntaxe :

```sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

Les types de `ARRAY JOIN` pris en charge sont indiqués ci-dessous :

* `ARRAY JOIN` - Dans le cas de base, les tableaux vides ne sont pas inclus dans le résultat du `JOIN`.
* `LEFT ARRAY JOIN` - Le résultat du `JOIN` contient des lignes avec des tableaux vides. La valeur d’un tableau vide est définie sur la valeur par défaut du type d’élément du tableau (généralement 0, une chaîne vide ou NULL).

<div id="basic-array-join-examples">
  ## Exemples de base d&#39;ARRAY JOIN
</div>

<div id="array-join-left-array-join-examples">
  ### ARRAY JOIN et LEFT ARRAY JOIN
</div>

Les exemples ci-dessous illustrent l’utilisation des clauses `ARRAY JOIN` et `LEFT ARRAY JOIN`. Créons une table avec une colonne de type [Array](../../../sql-reference/data-types/array.md) et insérons-y des valeurs :

```sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

```response
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

L’exemple ci-dessous utilise la clause `ARRAY JOIN` :

```sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

```response
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

L’exemple suivant utilise la clause `LEFT ARRAY JOIN` :

```sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

```response
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

<div id="array-join-arrayEnumerate">
  ### ARRAY JOIN et la fonction arrayEnumerate
</div>

Cette fonction est généralement utilisée avec `ARRAY JOIN`. Elle permet de ne compter quelque chose qu’une seule fois pour chaque tableau après un `ARRAY JOIN`. Exemple :

```sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

Dans cet exemple, Reaches correspond au nombre de conversions (les chaînes obtenues après application de `ARRAY JOIN`) et Hits au nombre de pages vues (les chaînes avant `ARRAY JOIN`). Dans ce cas précis, vous pouvez obtenir le même résultat plus facilement :

```sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

<div id="array_join_arrayEnumerateUniq">
  ### ARRAY JOIN et arrayEnumerateUniq
</div>

Cette fonction est utile lors de l’utilisation de `ARRAY JOIN` et de l’agrégation d’éléments de tableau.

Dans cet exemple, chaque ID d’objectif inclut un calcul du nombre de conversions (chaque élément de la structure de données imbriquée Goals correspond à un objectif atteint, que nous appelons une conversion) ainsi que du nombre de sessions. Sans `ARRAY JOIN`, nous aurions compté le nombre de sessions avec sum(Sign). Mais dans ce cas particulier, les lignes ont été multipliées par la structure imbriquée Goals. Afin de ne compter chaque session qu’une seule fois ensuite, nous appliquons une condition à la valeur de la fonction `arrayEnumerateUniq(Goals.ID)`.

```sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

```text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

<div id="using-aliases">
  ## Utilisation des alias
</div>

Un alias peut être défini pour un tableau dans la clause `ARRAY JOIN`. Dans ce cas, on peut accéder à un élément du tableau via cet alias, mais au tableau lui-même via son nom d’origine. Exemple :

```sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

```response
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

À l’aide d’alias, vous pouvez effectuer `ARRAY JOIN` avec un tableau externe. Par exemple :

```sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

```response
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

Plusieurs tableaux peuvent être séparés par des virgules dans la clause `ARRAY JOIN`. Dans ce cas, l’opération `JOIN` est effectuée simultanément sur ceux-ci (somme directe, et non produit cartésien). Notez que, par défaut, tous les tableaux doivent avoir la même taille. Exemple :

```sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

L’exemple ci-dessous utilise la fonction [arrayEnumerate](/fr/sql-reference/functions/array-functions#arrayEnumerate) :

```sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

Plusieurs tableaux de tailles différentes peuvent être joints à l’aide de : `SETTINGS enable_unaligned_array_join = 1`. Exemple :

```sql
SELECT s, arr, a, b
FROM arrays_test ARRAY JOIN arr AS a, [['a','b'],['c']] AS b
SETTINGS enable_unaligned_array_join = 1;
```

```response
┌─s───────┬─arr─────┬─a─┬─b─────────┐
│ Hello   │ [1,2]   │ 1 │ ['a','b'] │
│ Hello   │ [1,2]   │ 2 │ ['c']     │
│ World   │ [3,4,5] │ 3 │ ['a','b'] │
│ World   │ [3,4,5] │ 4 │ ['c']     │
│ World   │ [3,4,5] │ 5 │ []        │
│ Goodbye │ []      │ 0 │ ['a','b'] │
│ Goodbye │ []      │ 0 │ ['c']     │
└─────────┴─────────┴───┴───────────┘
```

<div id="array-join-with-nested-data-structure">
  ## ARRAY JOIN avec une structure de données imbriquée
</div>

`ARRAY JOIN` fonctionne également avec les [structures de données imbriquées](../../../sql-reference/data-types/nested-data-structures/index.md):

```sql
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

```response
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Lorsque l’on spécifie les noms de structures de données imbriquées dans `ARRAY JOIN`, cela a le même sens que `ARRAY JOIN` avec tous les éléments de tableau qui les composent. Des exemples sont donnés ci-dessous :

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Cette variante est également pertinente :

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

Un alias peut être utilisé pour une structure de données imbriquée, pour sélectionner soit le résultat du `JOIN`, soit le tableau source. Exemple :

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

Exemple d’utilisation de la fonction [arrayEnumerate](/fr/sql-reference/functions/array-functions#arrayEnumerate) :

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

<div id="implementation-details">
  ## Détails d’implémentation
</div>

L’ordre d’exécution de la requête est optimisé lors de l’utilisation de `ARRAY JOIN`. Bien que `ARRAY JOIN` doive toujours être spécifié avant la clause [WHERE](../../../sql-reference/statements/select/where.md)/[PREWHERE](../../../sql-reference/statements/select/prewhere.md) dans une requête, leur exécution peut techniquement avoir lieu dans n’importe quel ordre, sauf si le résultat de `ARRAY JOIN` est utilisé pour le filtrage. L’ordre de traitement est contrôlé par l’optimiseur de requêtes.

<div id="incompatibility-with-short-circuit-function-evaluation">
  ### Incompatibilité avec l’évaluation des fonctions en court-circuit
</div>

[L’évaluation des fonctions en court-circuit](/fr/operations/settings/settings#short_circuit_function_evaluation) est une fonctionnalité qui optimise l’exécution d’expressions complexes dans certaines fonctions, telles que `if`, `multiIf`, `and` et `or`. Elle évite que des exceptions potentielles, comme une division par zéro, ne se produisent lors de l’exécution de ces fonctions.

`arrayJoin` est toujours exécutée et n’est pas compatible avec l’évaluation des fonctions en court-circuit. Cela s’explique par le fait qu’il s’agit d’une fonction particulière, traitée séparément de toutes les autres lors de l’analyse et de l’exécution des requêtes, et qu’elle nécessite une logique supplémentaire incompatible avec ce mode d’exécution. En effet, le nombre de lignes dans le résultat dépend du résultat de `arrayJoin`, et il serait trop complexe et coûteux d’implémenter une exécution différée de `arrayJoin`.

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Travailler avec des données de séries temporelles dans ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)