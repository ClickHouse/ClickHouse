---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: Combinators
---

# Combinateurs De Fonction D'Agrégat {#aggregate_functions_combinators}

Le nom d'une fonction d'agrégat peut avoir un suffixe ajouté. Cela change la façon dont la fonction d'agrégation fonctionne.

## -Si {#agg-functions-combinator-if}

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

Exemple: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` et ainsi de suite.

Avec les fonctions d'agrégat conditionnel, vous pouvez calculer des agrégats pour plusieurs conditions à la fois, sans utiliser de sous-requêtes et `JOIN`s. Par exemple, dans Yandex.Metrica, les fonctions d'agrégat conditionnel sont utilisées pour implémenter la fonctionnalité de comparaison de segment.

## -Tableau {#agg-functions-combinator-array}

Le Tableau suffixe peut être ajouté à toute fonction d'agrégation. Dans ce cas, la fonction d'agrégation des arguments de la ‘Array(T)’ type (tableaux) au lieu de ‘T’ tapez les arguments. Si la fonction aggregate accepte plusieurs arguments, il doit s'agir de tableaux de longueurs égales. Lors du traitement des tableaux, la fonction d'agrégation fonctionne comme la fonction d'agrégation d'origine sur tous les éléments du tableau.

Exemple 1: `sumArray(arr)` - Totalise tous les éléments de tous ‘arr’ tableau. Dans cet exemple, il aurait pu être écrit plus simplement: `sum(arraySum(arr))`.

Exemple 2: `uniqArray(arr)` – Counts the number of unique elements in all ‘arr’ tableau. Cela pourrait être fait d'une manière plus facile: `uniq(arrayJoin(arr))` mais ce n'est pas toujours possible d'ajouter des ‘arrayJoin’ pour une requête.

\- Si et-tableau peut être combiné. Cependant, ‘Array’ doit venir en premier, puis ‘If’. Exemple: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. En raison de cet ordre, le ‘cond’ argument ne sera pas un tableau.

## -État {#agg-functions-combinator-state}

Si vous appliquez ce combinateur, la fonction d'agrégation ne renvoie pas la valeur résultante (par exemple le nombre de valeurs uniques pour [uniq](reference.md#agg_function-uniq) la fonction), mais un état intermédiaire de l'agrégation (pour `uniq`, c'est la table de hachage pour calculer le nombre de valeurs uniques). C'est un `AggregateFunction(...)` qui peuvent être utilisés pour un traitement ultérieur ou stockés dans un tableau pour terminer l'agrégation plus tard.

Pour travailler avec ces états, utilisez:

-   [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) tableau moteur.
-   [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) fonction.
-   [runningAccumulate](../../sql-reference/functions/other-functions.md#function-runningaccumulate) fonction.
-   [-Fusionner](#aggregate_functions_combinators-merge) combinator.
-   [-MergeState](#aggregate_functions_combinators-mergestate) combinator.

## -Fusionner {#aggregate_functions_combinators-merge}

Si vous appliquez ce combinateur, la fonction d'agrégation prend l'état d'agrégation intermédiaire comme argument, combine les États pour terminer l'agrégation et renvoie la valeur résultante.

## -MergeState {#aggregate_functions_combinators-mergestate}

Fusionne les États d'agrégation intermédiaires de la même manière que le combinateur-Merge. Cependant, il ne renvoie pas la valeur résultante, mais un État d'agrégation intermédiaire, similaire au combinateur-State.

## - ForEach {#agg-functions-combinator-foreach}

Convertit une fonction d'agrégation pour les tables en une fonction d'agrégation pour les tableaux qui agrège les éléments de tableau correspondants et renvoie un tableau de résultats. Exemple, `sumForEach` pour les tableaux `[1, 2]`, `[3, 4, 5]`et`[6, 7]`renvoie le résultat `[10, 13, 5]` après avoir additionné les éléments de tableau correspondants.

## - OrDefault {#agg-functions-combinator-ordefault}

Modifie le comportement d'une fonction d'agrégat.

Si une fonction d'agrégation n'a pas de valeurs d'entrée, avec ce combinateur, elle renvoie la valeur par défaut pour son type de données de retour. S'applique aux fonctions d'agrégation qui peuvent prendre des données d'entrée vides.

`-OrDefault` peut être utilisé avec d'autres combinators.

**Syntaxe**

``` sql
<aggFunction>OrDefault(x)
```

**Paramètre**

-   `x` — Aggregate function parameters.

**Valeurs renvoyées**

Renvoie la valeur par défaut du type de retour d'une fonction d'agrégation s'il n'y a rien à agréger.

Le Type dépend de la fonction d'agrégation utilisée.

**Exemple**

Requête:

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

Résultat:

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

Également `-OrDefault` peut être utilisé avec un autre combinateur. Il est utile lorsque la fonction d'agrégation n'accepte pas l'entrée vide.

Requête:

``` sql
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

Résultat:

``` text
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

## - OrNull {#agg-functions-combinator-ornull}

Modifie le comportement d'une fonction d'agrégat.

Ce combinateur convertit un résultat d'une fonction d'agrégation à l' [Nullable](../data-types/nullable.md) type de données. Si la fonction d'agrégation n'a pas de valeurs à calculer elle renvoie [NULL](../syntax.md#null-literal).

`-OrNull` peut être utilisé avec d'autres combinators.

**Syntaxe**

``` sql
<aggFunction>OrNull(x)
```

**Paramètre**

-   `x` — Aggregate function parameters.

**Valeurs renvoyées**

-   Le résultat de la fonction d'agrégat, converti en `Nullable` type de données.
-   `NULL` si il n'y a rien à s'agréger.

Type: `Nullable(aggregate function return type)`.

**Exemple**

Ajouter `-orNull` à la fin de la fonction d'agrégation.

Requête:

``` sql
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

Résultat:

``` text
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

Également `-OrNull` peut être utilisé avec un autre combinateur. Il est utile lorsque la fonction d'agrégation n'accepte pas l'entrée vide.

Requête:

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

Résultat:

``` text
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

## -Resample {#agg-functions-combinator-resample}

Permet de diviser les données en groupes, puis séparément agrège les données de ces groupes. Les groupes sont créés en divisant les valeurs d'une colonne en intervalles.

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**Paramètre**

-   `start` — Starting value of the whole required interval for `resampling_key` valeur.
-   `stop` — Ending value of the whole required interval for `resampling_key` valeur. L'ensemble de l'intervalle ne comprend pas les `stop` valeur `[start, stop)`.
-   `step` — Step for separating the whole interval into subintervals. The `aggFunction` est exécuté sur chacun de ces sous-intervalles indépendamment.
-   `resampling_key` — Column whose values are used for separating data into intervals.
-   `aggFunction_params` — `aggFunction` paramètre.

**Valeurs renvoyées**

-   Tableau de `aggFunction` les résultats pour chaque subinterval.

**Exemple**

Envisager l' `people` le tableau avec les données suivantes:

``` text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

Obtenons les noms des personnes dont l'âge se trouve dans les intervalles de `[30,60)` et `[60,75)`. Puisque nous utilisons la représentation entière pour l'âge, nous obtenons des âges dans le `[30, 59]` et `[60,74]` intervalle.

Pour agréger des noms dans un tableau, nous utilisons [grouperay](reference.md#agg_function-grouparray) fonction d'agrégation. Il faut un argument. Dans notre cas, c'est l' `name` colonne. Le `groupArrayResample` fonction devrait utiliser le `age` colonne pour agréger les noms par âge. Pour définir les intervalles requis, nous passons le `30, 75, 30` des arguments dans la `groupArrayResample` fonction.

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

Considérez les résultats.

`Jonh` est hors de l'échantillon parce qu'il est trop jeune. D'autres personnes sont distribués selon les intervalles d'âge.

Maintenant, nous allons compter le nombre total de personnes et leur salaire moyen dans les intervalles d'âge.

``` sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

``` text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/agg_functions/combinators/) <!--hide-->
