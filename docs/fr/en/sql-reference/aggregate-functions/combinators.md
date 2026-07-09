---
description: "Documentation sur les combinateurs de fonctions d’agrégation"
sidebar_label: 'Combinateurs'
sidebar_position: 37
slug: /sql-reference/aggregate-functions/combinators
title: "Combinateurs de fonctions d’agrégation"
doc_type: 'reference'
---

Le nom d’une fonction d’agrégation peut être suivi d’un suffixe. Celui-ci modifie le comportement de la fonction d’agrégation.

<div id="-if">
  ## -If
</div>

Le suffixe -If peut être ajouté au nom de n’importe quelle fonction d’agrégation. Dans ce cas, la fonction d’agrégation accepte un argument supplémentaire – une condition (de type Uint8). La fonction d’agrégation traite uniquement les lignes pour lesquelles la condition est remplie. Si la condition n’est jamais remplie, elle renvoie une valeur par défaut (généralement des zéros ou des chaînes vides).

Exemples : `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` et ainsi de suite.

Avec les fonctions d’agrégation conditionnelles, vous pouvez calculer des agrégats pour plusieurs conditions à la fois, sans utiliser de sous-requêtes ni de `JOIN`s. Par exemple, les fonctions d’agrégation conditionnelles peuvent être utilisées pour implémenter la fonctionnalité de comparaison de segments.

<div id="-array">
  ## -Array
</div>

Le suffixe -Array peut être ajouté à n&#39;importe quelle fonction d&#39;agrégation. Dans ce cas, la fonction d&#39;agrégation prend des arguments de type &#39;Array(T)&#39; (tableaux) au lieu d&#39;arguments de type &#39;T&#39;. Si la fonction d&#39;agrégation accepte plusieurs arguments, ceux-ci doivent être des tableaux de même longueur. Lorsqu&#39;elle traite des tableaux, la fonction d&#39;agrégation se comporte comme la fonction d&#39;agrégation d&#39;origine sur l&#39;ensemble des éléments des tableaux.

Exemple 1 : `sumArray(arr)` - Calcule le total de tous les éléments de tous les tableaux &#39;arr&#39;. Dans cet exemple, on aurait pu l&#39;écrire plus simplement : `sum(arraySum(arr))`.

Exemple 2 : `uniqArray(arr)` – Compte le nombre d&#39;éléments uniques dans tous les tableaux &#39;arr&#39;. On pourrait aussi le faire plus simplement : `uniq(arrayJoin(arr))`, mais il n&#39;est pas toujours possible d&#39;ajouter &#39;arrayJoin&#39; à une requête.

-If et -Array peuvent être combinés. Cependant, &#39;Array&#39; doit venir en premier, puis &#39;If&#39;. Exemples : `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. En raison de cet ordre, l&#39;argument &#39;cond&#39; ne sera pas un tableau.

<div id="-map">
  ## -Map
</div>

Le suffixe -Map peut être ajouté à n’importe quelle fonction d’agrégation. Cela crée une fonction d’agrégation qui prend un type Map comme argument et agrège séparément les valeurs de chaque clé de la map à l’aide de la fonction d’agrégation spécifiée. Le résultat est également de type Map.

**Exemple**

```sql
CREATE TABLE map_map(
    date Date,
    timeslot DateTime,
    status Map(String, UInt64)
) ENGINE = MergeTree
ORDER BY ();

INSERT INTO map_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', (['a', 'b', 'c'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', (['c', 'd', 'e'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['d', 'e', 'f'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['f', 'g', 'g'], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(status),
    avgMap(status),
    minMap(status)
FROM map_map
GROUP BY timeslot;

┌────────────timeslot─┬─sumMap(status)───────────────────────┬─avgMap(status)───────────────────────┬─minMap(status)───────────────────────┐
│ 2000-01-01 00:00:00 │ {'a':10,'b':10,'c':20,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │
│ 2000-01-01 00:01:00 │ {'d':10,'e':10,'f':20,'g':20}        │ {'d':10,'e':10,'f':10,'g':10}        │ {'d':10,'e':10,'f':10,'g':10}        │
└─────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┘
```

<div id="-simplestate">
  ## -SimpleState
</div>

Si vous appliquez ce combinateur, la fonction d’agrégation renvoie la même valeur, mais avec un type différent. Il s’agit d’une [SimpleAggregateFunction(...)](../../sql-reference/data-types/simpleaggregatefunction.md) qui peut être stockée dans une table pour être utilisée avec les tables [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).

**Syntaxe**

```sql
<aggFunction>SimpleState(x)
```

**Arguments**

* `x` — Paramètres de la fonction d’agrégation.

**Valeurs renvoyées**

La valeur d’une fonction d’agrégation de type `SimpleAggregateFunction(...)`.

**Exemple**

```sql title="Query"
WITH anySimpleState(number) AS c SELECT toTypeName(c), c FROM numbers(1);
```

```text title="Response"
┌─toTypeName(c)────────────────────────┬─c─┐
│ SimpleAggregateFunction(any, UInt64) │ 0 │
└──────────────────────────────────────┴───┘
```

<div id="-state">
  ## -State
</div>

Si vous appliquez ce combinateur, la fonction d’agrégation ne renvoie pas la valeur finale (comme le nombre de valeurs uniques pour la fonction [uniq](/fr/sql-reference/aggregate-functions/reference/uniq)), mais un état intermédiaire de l’agrégation (pour `uniq`, il s’agit de la table de hachage utilisée pour calculer le nombre de valeurs uniques). Il s’agit d’une `AggregateFunction(...)` qui peut être utilisée pour un traitement ultérieur ou stockée dans une table afin de finaliser l’agrégation plus tard.

:::note
Veuillez noter que -MapState n’est pas invariant pour un même jeu de données, car l’ordre des données dans l’état intermédiaire peut changer, même si cela n’a pas d’impact sur l’ingestion de ces données.
:::

Pour travailler avec ces états, utilisez :

* le moteur de table [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).
* la fonction [finalizeAggregation](/fr/sql-reference/functions/other-functions#finalizeAggregation).
* la fonction [runningAccumulate](../../sql-reference/functions/other-functions.md#runningAccumulate).
* le combinateur [-Merge](#-merge).
* le combinateur [-MergeState](#-mergestate).

<div id="-merge">
  ## -Merge
</div>

Si vous appliquez ce combinateur, la fonction d’agrégation prend l’état d’agrégation intermédiaire en argument, combine les états pour finaliser l’agrégation et renvoie la valeur obtenue.

<div id="-mergestate">
  ## -MergeState
</div>

Fusionne les états d’agrégation intermédiaires de la même manière que le combinateur -Merge. Cependant, il ne renvoie pas la valeur obtenue, mais un état d’agrégation intermédiaire, à l’instar du combinateur -State.

<div id="-foreach">
  ## -ForEach
</div>

Convertit une fonction d’agrégation pour les tables en une fonction d’agrégation pour les tableaux, qui agrège les éléments correspondants des tableaux et renvoie un tableau de résultats. Par exemple, `sumForEach` pour les tableaux `[1, 2]`, `[3, 4, 5]` et `[6, 7]` renvoie le résultat `[10, 13, 5]` après addition des éléments correspondants.

<div id="-tuple">
  ## -Tuple
</div>

Le suffixe `-Tuple` peut être ajouté à n’importe quelle fonction d’agrégation. La fonction combinée prend un argument de type `Tuple` pour chaque argument de la fonction d’agrégation sous-jacente ; tous les tuples doivent avoir le même nombre d’éléments. L’agrégation est appliquée indépendamment à chaque position d’élément : elle reçoit l’élément correspondant de chaque `Tuple` et renvoie un `Tuple` de résultats.

Si le premier `Tuple` en entrée comporte des noms d’élément explicites, ils sont conservés dans le résultat.

Les fonctions d’agrégation qui gèrent elles-mêmes les valeurs `NULL` (`anyRespectNulls`, `anyLastRespectNulls`, le modificateur `RESPECT NULLS`) ne prennent pas en charge le type `Nullable(Tuple(...))` comme argument ; utilisez plutôt des éléments `Nullable`.

**Syntaxe**

```sql
<aggFunction>Tuple(tuple1[, tuple2, ...])
```

**Arguments**

* `tuple1[, tuple2, ...]` — Colonnes de type `Tuple`, une par argument de la fonction d&#39;agrégation sous-jacente, avec toutes le même nombre d&#39;éléments. Chaque élément doit être d&#39;un type pris en charge par la fonction d&#39;agrégation sous-jacente à cette position d&#39;argument.

**Valeurs renvoyées**

* Un `Tuple` contenant le résultat de l&#39;application de la fonction d&#39;agrégation à chaque élément, indépendamment des autres.

Type : `Tuple(aggFunction(element1), aggFunction(element2), ...)`.

**Exemple**

Requête :

```sql
SELECT sumTuple(t) FROM
(
    SELECT tuple(toInt64(1), toFloat64(2.5)) AS t
    UNION ALL
    SELECT tuple(toInt64(3), toFloat64(4.5))
    UNION ALL
    SELECT tuple(toInt64(5), toFloat64(6.5))
);
```

Résultat :

```text
┌─sumTuple(t)─┐
│ (9,13.5)    │
└─────────────┘
```

Avec `GROUP BY` :

```sql
SELECT
    k,
    avgTuple(t)
FROM
(
    SELECT
        number % 2 AS k,
        tuple(toInt64(number), toFloat64(number) * 1.5) AS t
    FROM numbers(6)
)
GROUP BY k
ORDER BY k;
```

```text
┌─k─┬─avgTuple(t)─┐
│ 0 │ (2,3)       │
│ 1 │ (3,4.5)     │
└───┴─────────────┘
```

Avec une fonction d’agrégation à plusieurs arguments : chaque argument `Tuple` fournit un argument à la fonction sous-jacente, et les éléments sont appariés selon leur position :

```text
corrTuple((a1, a2), (b1, b2)) = (corr(a1, b1), corr(a2, b2))
```

```sql
SELECT corrTuple((a1, a2), (b1, b2))
FROM
(
    SELECT
        toFloat64(number) AS a1,
        toFloat64(number * 2) AS a2,
        toFloat64(100 - number) AS b1,
        toFloat64(number * 3) AS b2
    FROM numbers(10)
);
```

```text
┌─corrTuple((a1, a2), (b1, b2))─┐
│ (-1,1)                        │
└───────────────────────────────┘
```

`a1` et `b1` sont anticorrélés, tandis que `a2` et `b2` sont proportionnels, de sorte que le résultat est `(-1, 1)`.

`-Tuple` peut être combiné avec d’autres combinateurs, comme `-If`. Par exemple : `sumTupleIf(tuple_column, cond)`.

<div id="-distinct">
  ## -Distinct
</div>

Chaque combinaison unique d’arguments n’est agrégée qu’une seule fois. Les valeurs répétées sont ignorées.
Exemples : `sum(DISTINCT x)` (ou `sumDistinct(x)`), `groupArray(DISTINCT x)` (ou `groupArrayDistinct(x)`), `corrStable(DISTINCT x, y)` (ou `corrStableDistinct(x, y)`) et ainsi de suite.

<div id="-ordefault">
  ## -OrDefault
</div>

Modifie le comportement d’une fonction d’agrégation.

Si une fonction d’agrégation n’a pas de valeur d’entrée, ce combinateur renvoie la valeur par défaut du type de données de retour. S’applique aux fonctions d’agrégation qui peuvent accepter des données d’entrée vides.

`-OrDefault` peut être utilisé avec d’autres combinateurs.

**Syntaxe**

```sql
<aggFunction>OrDefault(x)
```

**Arguments**

* `x` — Paramètres de la fonction d’agrégation.

**Valeurs renvoyées**

Renvoie la valeur par défaut du type de retour de la fonction d’agrégation s’il n’y a rien à agréger.

Le type dépend de la fonction d’agrégation utilisée.

**Exemple**

```sql title="Query"
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

```text title="Response"
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

Le suffixe `-OrDefault` peut également être utilisé avec d’autres combinateurs. Cela est utile lorsque la fonction d’agrégation n’accepte pas d’entrée vide.

```sql title="Query"
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

<div id="-ornull">
  ## -OrNull
</div>

Modifie le comportement d&#39;une fonction d&#39;agrégation.

Ce combinateur convertit le résultat d&#39;une fonction d&#39;agrégation en type de données [Nullable](../../sql-reference/data-types/nullable.md). Si la fonction d&#39;agrégation n&#39;a aucune valeur à calculer, elle renvoie [NULL](/fr/operations/settings/formats#input_format_null_as_default).

`-OrNull` peut être utilisé avec d&#39;autres combinateurs.

**Syntaxe**

```sql
<aggFunction>OrNull(x)
```

**Arguments**

* `x` — Paramètres de la fonction d’agrégation.

**Valeurs renvoyées**

* Le résultat de la fonction d’agrégation, converti en type de données `Nullable`.
* `NULL`, s’il n’y a rien à agréger.

Type : `Nullable(aggregate function return type)`.

**Exemple**

Ajoutez `-orNull` à la fin du nom de la fonction d’agrégation.

```sql title="Query"
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

```text title="Response"
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

En outre, `-OrNull` peut aussi être utilisé avec d&#39;autres combinateurs. C&#39;est utile lorsque la fonction d&#39;agrégation n&#39;accepte pas d&#39;entrée vide.

```sql title="Query"
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

<div id="-resample">
  ## -Resample
</div>

Permet de répartir les données en groupes, puis d’agréger séparément les données de chaque groupe. Les groupes sont créés en répartissant les valeurs d’une colonne par intervalles.

```sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**Arguments**

* `start` — Valeur de début de l’intervalle complet requis pour les valeurs de `resampling_key`.
* `stop` — Valeur de fin de l’intervalle complet requis pour les valeurs de `resampling_key`. L’intervalle complet n’inclut pas la valeur `stop` `[start, stop)`.
* `step` — Pas servant à diviser l’intervalle complet en sous-intervalles. `aggFunction` est exécutée indépendamment sur chacun de ces sous-intervalles.
* `resampling_key` — Colonne dont les valeurs sont utilisées pour répartir les données en intervalles.
* `aggFunction_params` — Paramètres de `aggFunction`.

**Valeurs renvoyées**

* Array des résultats de `aggFunction` pour chaque sous-intervalle.

**Exemple**

Prenons la table `people` avec les données suivantes :

```text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

Récupérons les noms des personnes dont l’âge se situe dans les intervalles `[30,60)` et `[60,75)`. Comme nous utilisons une représentation entière pour l’âge, nous obtenons des âges dans les intervalles `[30, 59]` et `[60,74]`.

Pour agréger des noms dans un tableau, nous utilisons la fonction d’agrégation [groupArray](/fr/sql-reference/aggregate-functions/reference/grouparray). Elle prend un argument. Dans notre cas, il s’agit de la colonne `name`. La fonction `groupArrayResample` doit utiliser la colonne `age` pour agréger les noms par âge. Pour définir les intervalles requis, nous passons les arguments `30, 75, 30` à la fonction `groupArrayResample`.

```sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

```text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

Examinons les résultats.

`John` ne fait pas partie de l’échantillon parce qu&#39;il est trop jeune. Les autres personnes sont réparties selon les intervalles d’âge spécifiés.

Comptons maintenant le nombre total de personnes ainsi que leur salaire moyen dans les intervalles d’âge spécifiés.

```sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

```text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

<div id="-argmin">
  ## -ArgMin
</div>

Le suffixe -ArgMin peut être ajouté au nom de n’importe quelle fonction d’agrégation. Dans ce cas, la fonction d’agrégation accepte un argument supplémentaire, qui doit être une expression comparable. La fonction d’agrégation ne traite que les lignes ayant la valeur minimale pour l’expression supplémentaire spécifiée.

Exemples : `sumArgMin(column, expr)`, `countArgMin(expr)`, `avgArgMin(x, expr)`, etc.

<div id="-argmax">
  ## -ArgMax
</div>

Semblable au suffixe -ArgMin, mais traite uniquement les lignes ayant la valeur maximale pour l’expression supplémentaire spécifiée.

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Utilisation des combinateurs d&#39;agrégation dans ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)