---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "D'Ordre Sup\xE9rieur"
---

# Fonctions d'ordre supérieur {#higher-order-functions}

## `->` opérateur, fonction lambda (params, expr) {#operator-lambdaparams-expr-function}

Allows describing a lambda function for passing to a higher-order function. The left side of the arrow has a formal parameter, which is any ID, or multiple formal parameters – any IDs in a tuple. The right side of the arrow has an expression that can use these formal parameters, as well as any table columns.

Exemple: `x -> 2 * x, str -> str != Referer.`

Les fonctions d'ordre supérieur ne peuvent accepter que les fonctions lambda comme argument fonctionnel.

Une fonction lambda qui accepte plusieurs arguments peuvent être passés à une fonction d'ordre supérieur. Dans ce cas, la fonction d'ordre supérieur est passé plusieurs tableaux de longueur identique que ces arguments correspondent.

Pour certaines fonctions, telles que [arrayCount](#higher_order_functions-array-count) ou [arraySum](#higher_order_functions-array-count) le premier argument (la fonction lambda) peut être omis. Dans ce cas, un mappage identique est supposé.

Une fonction lambda ne peut pas être omise pour les fonctions suivantes:

-   [arrayMap](#higher_order_functions-array-map)
-   [arrayFilter](#higher_order_functions-array-filter)
-   [arrayFill](#higher_order_functions-array-fill)
-   [arrayReverseFill](#higher_order_functions-array-reverse-fill)
-   [arraySplit](#higher_order_functions-array-split)
-   [arrayReverseSplit](#higher_order_functions-array-reverse-split)
-   [arrayFirst](#higher_order_functions-array-first)
-   [arrayFirstIndex](#higher_order_functions-array-first-index)

### arrayMap(func, arr1, …) {#higher_order_functions-array-map}

Renvoie un tableau obtenu à partir de l'application d'origine `func` fonction à chaque élément dans le `arr` tableau.

Exemple:

``` sql
SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,4,5] │
└─────────┘
```

L'exemple suivant montre comment créer un n-uplet d'éléments de différents tableaux:

``` sql
SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res
```

``` text
┌─res─────────────────┐
│ [(1,4),(2,5),(3,6)] │
└─────────────────────┘
```

Notez que le premier argument (fonction lambda) ne peut pas être omis dans le `arrayMap` fonction.

### arrayFilter(func, arr1, …) {#higher_order_functions-array-filter}

Renvoie un tableau contenant uniquement les éléments `arr1` pour ce qui `func` retourne autre chose que 0.

Exemple:

``` sql
SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res
```

``` text
┌─res───────────┐
│ ['abc World'] │
└───────────────┘
```

``` sql
SELECT
    arrayFilter(
        (i, x) -> x LIKE '%World%',
        arrayEnumerate(arr),
        ['Hello', 'abc World'] AS arr)
    AS res
```

``` text
┌─res─┐
│ [2] │
└─────┘
```

Notez que le premier argument (fonction lambda) ne peut pas être omis dans le `arrayFilter` fonction.

### arrayFill(func, arr1, …) {#higher_order_functions-array-fill}

Analyse par le biais de `arr1` du premier élément au dernier élément et remplacer `arr1[i]` par `arr1[i - 1]` si `func` renvoie 0. Le premier élément de `arr1` ne sera pas remplacé.

Exemple:

``` sql
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res──────────────────────────────┐
│ [1,1,3,11,12,12,12,5,6,14,14,14] │
└──────────────────────────────────┘
```

Notez que le premier argument (fonction lambda) ne peut pas être omis dans le `arrayFill` fonction.

### arrayReverseFill(func, arr1, …) {#higher_order_functions-array-reverse-fill}

Analyse par le biais de `arr1` du dernier élément au premier élément et remplacer `arr1[i]` par `arr1[i + 1]` si `func` renvoie 0. Le dernier élément de `arr1` ne sera pas remplacé.

Exemple:

``` sql
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res────────────────────────────────┐
│ [1,3,3,11,12,5,5,5,6,14,NULL,NULL] │
└────────────────────────────────────┘
```

Notez que le premier argument (fonction lambda) ne peut pas être omis dans le `arrayReverseFill` fonction.

### arraySplit(func, arr1, …) {#higher_order_functions-array-split}

Split `arr1` en plusieurs tableaux. Lorsque `func` retourne autre chose que 0, la matrice sera de split sur le côté gauche de l'élément. Le tableau ne sera pas partagé avant le premier élément.

Exemple:

``` sql
SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res─────────────┐
│ [[1,2,3],[4,5]] │
└─────────────────┘
```

Notez que le premier argument (fonction lambda) ne peut pas être omis dans le `arraySplit` fonction.

### arrayReverseSplit(func, arr1, …) {#higher_order_functions-array-reverse-split}

Split `arr1` en plusieurs tableaux. Lorsque `func` retourne autre chose que 0, la matrice sera de split sur le côté droit de l'élément. Le tableau ne sera pas divisé après le dernier élément.

Exemple:

``` sql
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res───────────────┐
│ [[1],[2,3,4],[5]] │
└───────────────────┘
```

Notez que le premier argument (fonction lambda) ne peut pas être omis dans le `arraySplit` fonction.

### arrayCount(\[func,\] arr1, …) {#higher_order_functions-array-count}

Renvoie le nombre d'éléments dans l'arr tableau pour lequel func renvoie autre chose que 0. Si ‘func’ n'est pas spécifié, il renvoie le nombre d'éléments non nuls dans le tableau.

### arrayExists(\[func,\] arr1, …) {#arrayexistsfunc-arr1}

Renvoie 1 s'il existe au moins un élément ‘arr’ pour ce qui ‘func’ retourne autre chose que 0. Sinon, il renvoie 0.

### arrayAll(\[func,\] arr1, …) {#arrayallfunc-arr1}

Renvoie 1 si ‘func’ retourne autre chose que 0 pour tous les éléments de ‘arr’. Sinon, il renvoie 0.

### arraySum(\[func,\] arr1, …) {#higher-order-functions-array-sum}

Renvoie la somme de la ‘func’ valeur. Si la fonction est omise, elle retourne la somme des éléments du tableau.

### arrayFirst(func, arr1, …) {#higher_order_functions-array-first}

Renvoie le premier élément du ‘arr1’ tableau pour lequel ‘func’ retourne autre chose que 0.

Notez que le premier argument (fonction lambda) ne peut pas être omis dans le `arrayFirst` fonction.

### arrayFirstIndex(func, arr1, …) {#higher_order_functions-array-first-index}

Renvoie l'index du premier élément de la ‘arr1’ tableau pour lequel ‘func’ retourne autre chose que 0.

Notez que le premier argument (fonction lambda) ne peut pas être omis dans le `arrayFirstIndex` fonction.

### arrayCumSum(\[func,\] arr1, …) {#arraycumsumfunc-arr1}

Retourne un tableau des sommes partielles d'éléments dans le tableau source (une somme). Si l' `func` la fonction est spécifiée, les valeurs des éléments du tableau sont convertis par cette fonction avant l'addition.

Exemple:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

``` text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

### arrayCumSumNonNegative (arr) {#arraycumsumnonnegativearr}

Même que `arrayCumSum`, renvoie un tableau des sommes partielles d'éléments dans le tableau source (une somme). Différent `arrayCumSum`, lorsque la valeur renvoyée contient une valeur inférieure à zéro, la valeur est remplacée par zéro et le calcul ultérieur est effectué avec des paramètres zéro. Exemple:

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

``` text
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```

### arraySort(\[func,\] arr1, …) {#arraysortfunc-arr1}

Renvoie un tableau à la suite du tri des éléments de `arr1` dans l'ordre croissant. Si l' `func` la fonction est spécifiée, l'ordre de classement est déterminé par le résultat de la fonction `func` appliquée aux éléments du tableau (tableaux)

Le [Transformation schwartzienne](https://en.wikipedia.org/wiki/Schwartzian_transform) est utilisé pour améliorer l'efficacité du tri.

Exemple:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Pour plus d'informations sur la `arraySort` la méthode, voir l' [Fonctions pour travailler avec des tableaux](array-functions.md#array_functions-sort) section.

### arrayReverseSort(\[func,\] arr1, …) {#arrayreversesortfunc-arr1}

Renvoie un tableau à la suite du tri des éléments de `arr1` dans l'ordre décroissant. Si l' `func` la fonction est spécifiée, l'ordre de classement est déterminé par le résultat de la fonction `func` appliquée aux éléments du tableau (tableaux).

Exemple:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

Pour plus d'informations sur la `arrayReverseSort` la méthode, voir l' [Fonctions pour travailler avec des tableaux](array-functions.md#array_functions-reverse-sort) section.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/higher_order_functions/) <!--hide-->
