---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: Travailler avec des tableaux
---

# Fonctions pour travailler avec des tableaux {#functions-for-working-with-arrays}

## vide {#function-empty}

Retourne 1 pour un tableau vide, ou 0 pour un non-vide.
Le type de résultat est UInt8.
La fonction fonctionne également pour les chaînes.

## notEmpty {#function-notempty}

Retourne 0 pour un tableau vide, ou 1 pour un non-vide.
Le type de résultat est UInt8.
La fonction fonctionne également pour les chaînes.

## longueur {#array_functions-length}

Retourne le nombre d'éléments dans le tableau.
Le type de résultat est UInt64.
La fonction fonctionne également pour les chaînes.

## emptyArrayUInt8, emptyArrayUInt16, emptyArrayUInt32, emptyArrayUInt64 {#emptyarrayuint8-emptyarrayuint16-emptyarrayuint32-emptyarrayuint64}

## emptyArrayInt8, emptyArrayInt16, emptyArrayInt32, emptyArrayInt64 {#emptyarrayint8-emptyarrayint16-emptyarrayint32-emptyarrayint64}

## emptyArrayFloat32, emptyArrayFloat64 {#emptyarrayfloat32-emptyarrayfloat64}

## emptyArrayDate, emptyArrayDateTime {#emptyarraydate-emptyarraydatetime}

## emptyArrayString {#emptyarraystring}

Accepte zéro argument et renvoie un tableau vide du type approprié.

## emptyArrayToSingle {#emptyarraytosingle}

Accepte un tableau vide et renvoie un élément de tableau qui est égal à la valeur par défaut.

## plage (fin), Plage(début, fin \[, étape\]) {#rangeend-rangestart-end-step}

Retourne un tableau de nombres du début à la fin-1 par étape.
Si l'argument `start` n'est pas spécifié, la valeur par défaut est 0.
Si l'argument `step` n'est pas spécifié, la valeur par défaut est 1.
Il se comporte presque comme pythonic `range`. Mais la différence est que tous les types d'arguments doivent être `UInt` nombre.
Juste au cas où, une exception est levée si des tableaux d'une longueur totale de plus de 100 000 000 d'éléments sont créés dans un bloc de données.

## array(x1, …), operator \[x1, …\] {#arrayx1-operator-x1}

Crée un tableau à partir des arguments de la fonction.
Les arguments doivent être des constantes et avoir des types qui ont le plus petit type commun. Au moins un argument doit être passé, sinon il n'est pas clair quel type de tableau créer. Qui est, vous ne pouvez pas utiliser cette fonction pour créer un tableau vide (pour ce faire, utilisez la ‘emptyArray\*’ la fonction décrite ci-dessus).
Retourne un ‘Array(T)’ type de résultat, où ‘T’ est le plus petit type commun parmi les arguments passés.

## arrayConcat {#arrayconcat}

Combine des tableaux passés comme arguments.

``` sql
arrayConcat(arrays)
```

**Paramètre**

-   `arrays` – Arbitrary number of arguments of [Tableau](../../sql-reference/data-types/array.md) type.
    **Exemple**

<!-- -->

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

``` text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## arrayElement(arr, n), opérateur arr\[n\] {#arrayelementarr-n-operator-arrn}

Récupérer l'élément avec l'index `n` à partir du tableau `arr`. `n` doit être n'importe quel type entier.
Les index dans un tableau commencent à partir d'un.
Les index négatifs sont pris en charge. Dans ce cas, il sélectionne l'élément correspondant numérotées à partir de la fin. Exemple, `arr[-1]` est le dernier élément du tableau.

Si l'index est en dehors des limites d'un tableau, il renvoie une valeur (0 pour les nombres, une chaîne vide pour les cordes, etc.), sauf pour le cas avec un tableau non constant et un index constant 0 (dans ce cas, il y aura une erreur `Array indices are 1-based`).

## a (arr, elem) {#hasarr-elem}

Vérifie si le ‘arr’ tableau a la ‘elem’ élément.
Retourne 0 si l'élément n'est pas dans le tableau, ou 1 si elle l'est.

`NULL` est traitée comme une valeur.

``` sql
SELECT has([1, 2, NULL], NULL)
```

``` text
┌─has([1, 2, NULL], NULL)─┐
│                       1 │
└─────────────────────────┘
```

## hasAll {#hasall}

Vérifie si un tableau est un sous-ensemble de l'autre.

``` sql
hasAll(set, subset)
```

**Paramètre**

-   `set` – Array of any type with a set of elements.
-   `subset` – Array of any type with elements that should be tested to be a subset of `set`.

**Les valeurs de retour**

-   `1`, si `set` contient tous les éléments de `subset`.
-   `0`, autrement.

**Propriétés particulières**

-   Un tableau vide est un sous-ensemble d'un tableau quelconque.
-   `Null` traitée comme une valeur.
-   Ordre des valeurs dans les deux tableaux n'a pas d'importance.

**Exemple**

`SELECT hasAll([], [])` retours 1.

`SELECT hasAll([1, Null], [Null])` retours 1.

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` retours 1.

`SELECT hasAll(['a', 'b'], ['a'])` retours 1.

`SELECT hasAll([1], ['a'])` renvoie 0.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` renvoie 0.

## hasAny {#hasany}

Vérifie si deux tableaux ont une intersection par certains éléments.

``` sql
hasAny(array1, array2)
```

**Paramètre**

-   `array1` – Array of any type with a set of elements.
-   `array2` – Array of any type with a set of elements.

**Les valeurs de retour**

-   `1`, si `array1` et `array2` avoir un élément similaire au moins.
-   `0`, autrement.

**Propriétés particulières**

-   `Null` traitée comme une valeur.
-   Ordre des valeurs dans les deux tableaux n'a pas d'importance.

**Exemple**

`SELECT hasAny([1], [])` retourner `0`.

`SELECT hasAny([Null], [Null, 1])` retourner `1`.

`SELECT hasAny([-128, 1., 512], [1])` retourner `1`.

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` retourner `0`.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` retourner `1`.

## indexOf (arr, x) {#indexofarr-x}

Renvoie l'index de la première ‘x’ élément (à partir de 1) s'il est dans le tableau, ou 0 s'il ne l'est pas.

Exemple:

``` sql
SELECT indexOf([1, 3, NULL, NULL], NULL)
```

``` text
┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│                                 3 │
└───────────────────────────────────┘
```

Ensemble d'éléments de `NULL` sont traités comme des valeurs normales.

## countEqual (arr, x) {#countequalarr-x}

Renvoie le nombre d'éléments dans le tableau égal à X. équivalent à arrayCount (elem - \> elem = x, arr).

`NULL` les éléments sont traités comme des valeurs distinctes.

Exemple:

``` sql
SELECT countEqual([1, 2, NULL, NULL], NULL)
```

``` text
┌─countEqual([1, 2, NULL, NULL], NULL)─┐
│                                    2 │
└──────────────────────────────────────┘
```

## arrayEnumerate (arr) {#array_functions-arrayenumerate}

Returns the array \[1, 2, 3, …, length (arr) \]

Cette fonction est normalement utilisée avec ARRAY JOIN. Il permet de compter quelque chose une seule fois pour chaque tableau après l'application de la jointure de tableau. Exemple:

``` sql
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

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

Dans cet exemple, Reaches est le nombre de conversions (les chaînes reçues après l'application de la jointure de tableau), et Hits est le nombre de pages vues (chaînes avant la jointure de tableau). Dans ce cas particulier, vous pouvez obtenir le même résultat dans une voie plus facile:

``` sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

Cette fonction peut également être utilisée dans les fonctions d'ordre supérieur. Par exemple, vous pouvez l'utiliser pour obtenir les indices de tableau pour les éléments qui correspondent à une condition.

## arrayEnumerateUniq(arr, …) {#arrayenumerateuniqarr}

Renvoie un tableau de la même taille que le tableau source, indiquant pour chaque élément Quelle est sa position parmi les éléments de même valeur.
Par exemple: arrayEnumerateUniq(\[10, 20, 10, 30\]) = \[1, 1, 2, 1\].

Cette fonction est utile lors de L'utilisation de la jointure de tableau et de l'agrégation d'éléments de tableau.
Exemple:

``` sql
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

``` text
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

Dans cet exemple, chaque ID d'objectif a un calcul du nombre de conversions (chaque élément de la structure de données imbriquées objectifs est un objectif atteint, que nous appelons une conversion) et le nombre de sessions. Sans array JOIN, nous aurions compté le nombre de sessions comme sum(signe). Mais dans ce cas particulier, les lignes ont été multipliées par la structure des objectifs imbriqués, donc pour compter chaque session une fois après cela, nous appliquons une condition à la valeur de arrayEnumerateUniq(Goals.ID) fonction.

La fonction arrayEnumerateUniq peut prendre plusieurs tableaux de la même taille que les arguments. Dans ce cas, l'unicité est considérée pour les tuples d'éléments dans les mêmes positions dans tous les tableaux.

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

``` text
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

Ceci est nécessaire lors de L'utilisation de Array JOIN avec une structure de données imbriquée et une agrégation supplémentaire entre plusieurs éléments de cette structure.

## arrayPopBack {#arraypopback}

Supprime le dernier élément du tableau.

``` sql
arrayPopBack(array)
```

**Paramètre**

-   `array` – Array.

**Exemple**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## arrayPopFront {#arraypopfront}

Supprime le premier élément de la matrice.

``` sql
arrayPopFront(array)
```

**Paramètre**

-   `array` – Array.

**Exemple**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack {#arraypushback}

Ajoute un élément à la fin du tableau.

``` sql
arrayPushBack(array, single_value)
```

**Paramètre**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` type pour le type de données du tableau. Pour plus d'informations sur les types de données dans ClickHouse, voir “[Types de données](../../sql-reference/data-types/index.md#data_types)”. Peut être `NULL`. La fonction ajoute un `NULL` tableau, et le type d'éléments de tableau convertit en `Nullable`.

**Exemple**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayPushFront {#arraypushfront}

Ajoute un élément au début du tableau.

``` sql
arrayPushFront(array, single_value)
```

**Paramètre**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` type pour le type de données du tableau. Pour plus d'informations sur les types de données dans ClickHouse, voir “[Types de données](../../sql-reference/data-types/index.md#data_types)”. Peut être `NULL`. La fonction ajoute un `NULL` tableau, et le type d'éléments de tableau convertit en `Nullable`.

**Exemple**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayResize {#arrayresize}

Les changements de la longueur du tableau.

``` sql
arrayResize(array, size[, extender])
```

**Paramètre:**

-   `array` — Array.
-   `size` — Required length of the array.
    -   Si `size` est inférieure à la taille d'origine du tableau, le tableau est tronqué à partir de la droite.
-   Si `size` est plus grande que la taille initiale du tableau, le tableau est étendu vers la droite avec `extender` valeurs ou valeurs par défaut pour le type de données des éléments du tableau.
-   `extender` — Value for extending an array. Can be `NULL`.

**Valeur renvoyée:**

Un tableau de longueur `size`.

**Exemples d'appels**

``` sql
SELECT arrayResize([1], 3)
```

``` text
┌─arrayResize([1], 3)─┐
│ [1,0,0]             │
└─────────────────────┘
```

``` sql
SELECT arrayResize([1], 3, NULL)
```

``` text
┌─arrayResize([1], 3, NULL)─┐
│ [1,NULL,NULL]             │
└───────────────────────────┘
```

## arraySlice {#arrayslice}

Retourne une tranche du tableau.

``` sql
arraySlice(array, offset[, length])
```

**Paramètre**

-   `array` – Array of data.
-   `offset` – Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with 1.
-   `length` - La longueur de la nécessaire tranche. Si vous spécifiez une valeur négative, la fonction renvoie un ouvert tranche `[offset, array_length - length)`. Si vous omettez la valeur, la fonction renvoie la tranche `[offset, the_end_of_array]`.

**Exemple**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res
```

``` text
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

Éléments de tableau définis sur `NULL` sont traités comme des valeurs normales.

## arraySort(\[func,\] arr, …) {#array_functions-sort}

Trie les éléments de la `arr` tableau dans l'ordre croissant. Si l' `func` fonction est spécifiée, l'ordre de tri est déterminé par le résultat de la `func` fonction appliquée aux éléments du tableau. Si `func` accepte plusieurs arguments, le `arraySort` la fonction est passé plusieurs tableaux que les arguments de `func` correspond à. Des exemples détaillés sont présentés à la fin de `arraySort` Description.

Exemple de tri de valeurs entières:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```

``` text
┌─arraySort([1, 3, 3, 0])─┐
│ [0,1,3,3]               │
└─────────────────────────┘
```

Exemple de tri des valeurs de chaîne:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```

``` text
┌─arraySort(['hello', 'world', '!'])─┐
│ ['!','hello','world']              │
└────────────────────────────────────┘
```

Considérez l'ordre de tri suivant pour le `NULL`, `NaN` et `Inf` valeur:

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

``` text
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
│ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
└───────────────────────────────────────────────────────────┘
```

-   `-Inf` les valeurs sont d'abord dans le tableau.
-   `NULL` les valeurs sont les derniers dans le tableau.
-   `NaN` les valeurs sont juste avant `NULL`.
-   `Inf` les valeurs sont juste avant `NaN`.

Notez que `arraySort` est un [fonction d'ordre supérieur](higher-order-functions.md). Vous pouvez passer d'une fonction lambda comme premier argument. Dans ce cas, l'ordre de classement est déterminé par le résultat de la fonction lambda appliquée aux éléments de la matrice.

Considérons l'exemple suivant:

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

For each element of the source array, the lambda function returns the sorting key, that is, \[1 –\> -1, 2 –\> -2, 3 –\> -3\]. Since the `arraySort` fonction trie les touches dans l'ordre croissant, le résultat est \[3, 2, 1\]. Ainsi, l' `(x) –> -x` fonction lambda définit le [l'ordre décroissant](#array_functions-reverse-sort) dans un tri.

La fonction lambda peut accepter plusieurs arguments. Dans ce cas, vous avez besoin de passer l' `arraySort` fonction plusieurs tableaux de longueur identique à laquelle correspondront les arguments de la fonction lambda. Le tableau résultant sera composé d'éléments du premier tableau d'entrée; les éléments du(des) Tableau (s) d'entrée suivant (s) spécifient les clés de tri. Exemple:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Ici, les éléments qui sont passés dans le deuxième tableau (\[2, 1\]) définissent une clé de tri pour l'élément correspondant à partir du tableau source (\[‘hello’, ‘world’\]), qui est, \[‘hello’ –\> 2, ‘world’ –\> 1\]. Since the lambda function doesn't use `x`, les valeurs réelles du tableau source n'affectent pas l'ordre dans le résultat. Si, ‘hello’ sera le deuxième élément du résultat, et ‘world’ sera le premier.

D'autres exemples sont présentés ci-dessous.

``` sql
SELECT arraySort((x, y) -> y, [0, 1, 2], ['c', 'b', 'a']) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

``` sql
SELECT arraySort((x, y) -> -y, [0, 1, 2], [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

!!! note "Note"
    Pour améliorer l'efficacité du tri, de la [Transformation schwartzienne](https://en.wikipedia.org/wiki/Schwartzian_transform) est utilisée.

## arrayReverseSort(\[func,\] arr, …) {#array_functions-reverse-sort}

Trie les éléments de la `arr` tableau dans l'ordre décroissant. Si l' `func` la fonction est spécifiée, `arr` est trié en fonction du résultat de la `func` fonction appliquée aux éléments du tableau, puis le tableau trié est inversé. Si `func` accepte plusieurs arguments, le `arrayReverseSort` la fonction est passé plusieurs tableaux que les arguments de `func` correspond à. Des exemples détaillés sont présentés à la fin de `arrayReverseSort` Description.

Exemple de tri de valeurs entières:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

``` text
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3,3,1,0]                      │
└────────────────────────────────┘
```

Exemple de tri des valeurs de chaîne:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

``` text
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ ['world','hello','!']                     │
└───────────────────────────────────────────┘
```

Considérez l'ordre de tri suivant pour le `NULL`, `NaN` et `Inf` valeur:

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

``` text
┌─res───────────────────────────────────┐
│ [inf,3,2,1,-4,-inf,nan,nan,NULL,NULL] │
└───────────────────────────────────────┘
```

-   `Inf` les valeurs sont d'abord dans le tableau.
-   `NULL` les valeurs sont les derniers dans le tableau.
-   `NaN` les valeurs sont juste avant `NULL`.
-   `-Inf` les valeurs sont juste avant `NaN`.

Notez que l' `arrayReverseSort` est un [fonction d'ordre supérieur](higher-order-functions.md). Vous pouvez passer d'une fonction lambda comme premier argument. Exemple est montré ci-dessous.

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

Le tableau est trié de la façon suivante:

1.  Dans un premier temps, le tableau source (\[1, 2, 3\]) est trié en fonction du résultat de la fonction lambda appliquée aux éléments du tableau. Le résultat est un tableau \[3, 2, 1\].
2.  Tableau qui est obtenu à l'étape précédente, est renversé. Donc, le résultat final est \[1, 2, 3\].

La fonction lambda peut accepter plusieurs arguments. Dans ce cas, vous avez besoin de passer l' `arrayReverseSort` fonction plusieurs tableaux de longueur identique à laquelle correspondront les arguments de la fonction lambda. Le tableau résultant sera composé d'éléments du premier tableau d'entrée; les éléments du(des) Tableau (s) d'entrée suivant (s) spécifient les clés de tri. Exemple:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

Dans cet exemple, le tableau est trié de la façon suivante:

1.  Au début, le tableau source (\[‘hello’, ‘world’\]) est triée selon le résultat de la fonction lambda appliquée aux éléments de tableaux. Les éléments qui sont passés dans le deuxième tableau (\[2, 1\]), définissent les clés de tri pour les éléments correspondants du tableau source. Le résultat est un tableau \[‘world’, ‘hello’\].
2.  Tableau trié lors de l'étape précédente, est renversé. Donc, le résultat final est \[‘hello’, ‘world’\].

D'autres exemples sont présentés ci-dessous.

``` sql
SELECT arrayReverseSort((x, y) -> y, [4, 3, 5], ['a', 'b', 'c']) AS res;
```

``` text
┌─res─────┐
│ [5,3,4] │
└─────────┘
```

``` sql
SELECT arrayReverseSort((x, y) -> -y, [4, 3, 5], [1, 2, 3]) AS res;
```

``` text
┌─res─────┐
│ [4,3,5] │
└─────────┘
```

## arrayUniq(arr, …) {#arrayuniqarr}

Si un argument est passé, il compte le nombre de différents éléments dans le tableau.
Si plusieurs arguments sont passés, il compte le nombre de tuples différents d'éléments aux positions correspondantes dans plusieurs tableaux.

Si vous souhaitez obtenir une liste des éléments dans un tableau, vous pouvez utiliser arrayReduce(‘groupUniqArray’, arr).

## arrayJoin (arr) {#array-functions-join}

Une fonction spéciale. Voir la section [“ArrayJoin function”](array-join.md#functions_arrayjoin).

## tableaudifférence {#arraydifference}

Calcule la différence entre les éléments de tableau adjacents. Renvoie un tableau où le premier élément sera 0, le second est la différence entre `a[1] - a[0]`, etc. The type of elements in the resulting array is determined by the type inference rules for subtraction (e.g. `UInt8` - `UInt8` = `Int16`).

**Syntaxe**

``` sql
arrayDifference(array)
```

**Paramètre**

-   `array` – [Tableau](https://clickhouse.tech/docs/en/data_types/array/).

**Valeurs renvoyées**

Renvoie un tableau de différences entre les éléments adjacents.

Type: [UInt\*](https://clickhouse.tech/docs/en/data_types/int_uint/#uint-ranges), [Int\*](https://clickhouse.tech/docs/en/data_types/int_uint/#int-ranges), [Flottant\*](https://clickhouse.tech/docs/en/data_types/float/).

**Exemple**

Requête:

``` sql
SELECT arrayDifference([1, 2, 3, 4])
```

Résultat:

``` text
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0,1,1,1]                     │
└───────────────────────────────┘
```

Exemple de débordement dû au type de résultat Int64:

Requête:

``` sql
SELECT arrayDifference([0, 10000000000000000000])
```

Résultat:

``` text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0,-8446744073709551616]                   │
└────────────────────────────────────────────┘
```

## arrayDistinct {#arraydistinct}

Prend un tableau, retourne un tableau contenant les différents éléments seulement.

**Syntaxe**

``` sql
arrayDistinct(array)
```

**Paramètre**

-   `array` – [Tableau](https://clickhouse.tech/docs/en/data_types/array/).

**Valeurs renvoyées**

Retourne un tableau contenant les éléments distincts.

**Exemple**

Requête:

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1])
```

Résultat:

``` text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## arrayEnumerateDense(arr) {#array_functions-arrayenumeratedense}

Renvoie un tableau de la même taille que le tableau source, indiquant où chaque élément apparaît en premier dans le tableau source.

Exemple:

``` sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

``` text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1,2,1,3]                             │
└───────────────────────────────────────┘
```

## arrayIntersect (arr) {#array-functions-arrayintersect}

Prend plusieurs tableaux, retourne un tableau avec des éléments présents dans tous les tableaux source. L'ordre des éléments dans le tableau résultant est le même que dans le premier tableau.

Exemple:

``` sql
SELECT
    arrayIntersect([1, 2], [1, 3], [2, 3]) AS no_intersect,
    arrayIntersect([1, 2], [1, 3], [1, 4]) AS intersect
```

``` text
┌─no_intersect─┬─intersect─┐
│ []           │ [1]       │
└──────────────┴───────────┘
```

## arrayReduce {#arrayreduce}

Applique une fonction d'agrégation aux éléments du tableau et renvoie son résultat. Le nom de la fonction d'agrégation est passé sous forme de chaîne entre guillemets simples `'max'`, `'sum'`. Lorsque vous utilisez des fonctions d'agrégat paramétriques, le paramètre est indiqué après le nom de la fonction entre parenthèses `'uniqUpTo(6)'`.

**Syntaxe**

``` sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**Paramètre**

-   `agg_func` — The name of an aggregate function which should be a constant [chaîne](../../sql-reference/data-types/string.md).
-   `arr` — Any number of [tableau](../../sql-reference/data-types/array.md) tapez les colonnes comme paramètres de la fonction d'agrégation.

**Valeur renvoyée**

**Exemple**

``` sql
SELECT arrayReduce('max', [1, 2, 3])
```

``` text
┌─arrayReduce('max', [1, 2, 3])─┐
│                             3 │
└───────────────────────────────┘
```

Si une fonction d'agrégation prend plusieurs arguments, cette fonction doit être appliqué à plusieurs ensembles de même taille.

``` sql
SELECT arrayReduce('maxIf', [3, 5], [1, 0])
```

``` text
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐
│                                    3 │
└──────────────────────────────────────┘
```

Exemple avec une fonction d'agrégat paramétrique:

``` sql
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
```

``` text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
```

## arrayReduceInRanges {#arrayreduceinranges}

Applique une fonction d'agrégation d'éléments de tableau dans des plages et retourne un tableau contenant le résultat correspondant à chaque gamme. La fonction retourne le même résultat que plusieurs `arrayReduce(agg_func, arraySlice(arr1, index, length), ...)`.

**Syntaxe**

``` sql
arrayReduceInRanges(agg_func, ranges, arr1, arr2, ..., arrN)
```

**Paramètre**

-   `agg_func` — The name of an aggregate function which should be a constant [chaîne](../../sql-reference/data-types/string.md).
-   `ranges` — The ranges to aggretate which should be an [tableau](../../sql-reference/data-types/array.md) de [tuple](../../sql-reference/data-types/tuple.md) qui contient l'indice et la longueur de chaque plage.
-   `arr` — Any number of [tableau](../../sql-reference/data-types/array.md) tapez les colonnes comme paramètres de la fonction d'agrégation.

**Valeur renvoyée**

**Exemple**

``` sql
SELECT arrayReduceInRanges(
    'sum',
    [(1, 5), (2, 3), (3, 4), (4, 4)],
    [1000000, 200000, 30000, 4000, 500, 60, 7]
) AS res
```

``` text
┌─res─────────────────────────┐
│ [1234500,234000,34560,4567] │
└─────────────────────────────┘
```

## arrayReverse(arr) {#arrayreverse}

Retourne un tableau de la même taille que l'original tableau contenant les éléments dans l'ordre inverse.

Exemple:

``` sql
SELECT arrayReverse([1, 2, 3])
```

``` text
┌─arrayReverse([1, 2, 3])─┐
│ [3,2,1]                 │
└─────────────────────────┘
```

## inverse (arr) {#array-functions-reverse}

Synonyme de [“arrayReverse”](#arrayreverse)

## arrayFlatten {#arrayflatten}

Convertit un tableau de tableaux dans un tableau associatif.

Fonction:

-   S'applique à toute profondeur de tableaux imbriqués.
-   Ne change pas les tableaux qui sont déjà plats.

Le tableau aplati contient tous les éléments de tous les tableaux source.

**Syntaxe**

``` sql
flatten(array_of_arrays)
```

Alias: `flatten`.

**Paramètre**

-   `array_of_arrays` — [Tableau](../../sql-reference/data-types/array.md) de tableaux. Exemple, `[[1,2,3], [4,5]]`.

**Exemple**

``` sql
SELECT flatten([[[1]], [[2], [3]]])
```

``` text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1,2,3]                                     │
└─────────────────────────────────────────────┘
```

## arrayCompact {#arraycompact}

Supprime les éléments en double consécutifs d'un tableau. L'ordre des valeurs de résultat est déterminée par l'ordre dans le tableau source.

**Syntaxe**

``` sql
arrayCompact(arr)
```

**Paramètre**

`arr` — The [tableau](../../sql-reference/data-types/array.md) inspecter.

**Valeur renvoyée**

Le tableau sans doublon.

Type: `Array`.

**Exemple**

Requête:

``` sql
SELECT arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])
```

Résultat:

``` text
┌─arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])─┐
│ [1,nan,nan,2,3]                            │
└────────────────────────────────────────────┘
```

## arrayZip {#arrayzip}

Combine plusieurs tableaux en un seul tableau. Le tableau résultant contient les éléments correspondants des tableaux source regroupés en tuples dans l'ordre des arguments listés.

**Syntaxe**

``` sql
arrayZip(arr1, arr2, ..., arrN)
```

**Paramètre**

-   `arrN` — [Tableau](../data-types/array.md).

La fonction peut prendre n'importe quel nombre de tableaux de différents types. Tous les tableaux doivent être de taille égale.

**Valeur renvoyée**

-   Tableau avec des éléments des tableaux source regroupés en [tuple](../data-types/tuple.md). Types de données dans le tuple sont les mêmes que les types de l'entrée des tableaux et dans le même ordre que les tableaux sont passés.

Type: [Tableau](../data-types/array.md).

**Exemple**

Requête:

``` sql
SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1])
```

Résultat:

``` text
┌─arrayZip(['a', 'b', 'c'], [5, 2, 1])─┐
│ [('a',5),('b',2),('c',1)]            │
└──────────────────────────────────────┘
```

## arrayAUC {#arrayauc}

Calculer AUC (zone sous la courbe, qui est un concept dans l'apprentissage automatique, voir plus de détails: https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve).

**Syntaxe**

``` sql
arrayAUC(arr_scores, arr_labels)
```

**Paramètre**
- `arr_scores` — scores prediction model gives.
- `arr_labels` — labels of samples, usually 1 for positive sample and 0 for negtive sample.

**Valeur renvoyée**
Renvoie la valeur AUC avec le type Float64.

**Exemple**
Requête:

``` sql
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
```

Résultat:

``` text
┌─arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])─┐
│                                          0.75 │
└────────────────────────────────────────---──┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/array_functions/) <!--hide-->
