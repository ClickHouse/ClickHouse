---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: "R\xE9f\xE9rence"
---

# Référence De La Fonction Agrégée {#aggregate-functions-reference}

## compter {#agg_function-count}

Compte le nombre de lignes ou de valeurs non NULL.

ClickHouse prend en charge les syntaxes suivantes pour `count`:
- `count(expr)` ou `COUNT(DISTINCT expr)`.
- `count()` ou `COUNT(*)`. Le `count()` la syntaxe est spécifique à ClickHouse.

**Paramètre**

La fonction peut prendre:

-   Zéro des paramètres.
-   Un [expression](../syntax.md#syntax-expressions).

**Valeur renvoyée**

-   Si la fonction est appelée sans paramètres, il compte le nombre de lignes.
-   Si l' [expression](../syntax.md#syntax-expressions) est passé, alors la fonction compte combien de fois cette expression retournée not null. Si l'expression renvoie un [Nullable](../../sql-reference/data-types/nullable.md)- tapez la valeur, puis le résultat de `count` séjours pas `Nullable`. La fonction renvoie 0 si l'expression est retournée `NULL` pour toutes les lignes.

Dans les deux cas le type de la valeur renvoyée est [UInt64](../../sql-reference/data-types/int-uint.md).

**Détail**

Clickhouse soutient le `COUNT(DISTINCT ...)` syntaxe. Le comportement de cette construction dépend de la [count_distinct_implementation](../../operations/settings/settings.md#settings-count_distinct_implementation) paramètre. Il définit lequel des [uniq\*](#agg_function-uniq) fonctions est utilisée pour effectuer l'opération. La valeur par défaut est la [uniqExact](#agg_function-uniqexact) fonction.

Le `SELECT count() FROM table` la requête n'est pas optimisé, car le nombre d'entrées dans la table n'est pas stockée séparément. Il choisit une petite colonne de la table et compte le nombre de valeurs qu'il contient.

**Exemple**

Exemple 1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

Exemple 2:

``` sql
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation'
```

``` text
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘
```

``` sql
SELECT count(DISTINCT num) FROM t
```

``` text
┌─uniqExact(num)─┐
│              3 │
└────────────────┘
```

Cet exemple montre que `count(DISTINCT num)` est effectuée par le `uniqExact` en fonction de l' `count_distinct_implementation` valeur de réglage.

## tout(x) {#agg_function-any}

Sélectionne la première valeur rencontrée.
La requête peut être exécutée dans n'importe quel ordre, et même dans un ordre différent à chaque fois, de sorte que le résultat de cette fonction est indéterminée.
Pour obtenir un résultat déterminé, vous pouvez utiliser le ‘min’ ou ‘max’ fonction au lieu de ‘any’.

Dans certains cas, vous pouvez compter sur l'ordre de l'exécution. Cela s'applique aux cas où SELECT provient d'une sous-requête qui utilise ORDER BY.

Lorsqu'un `SELECT` la requête a l' `GROUP BY` ou au moins une fonction d'agrégat, ClickHouse (contrairement à MySQL) exige que toutes les expressions du `SELECT`, `HAVING`, et `ORDER BY` clauses être calculée à partir de clés ou de fonctions d'agrégation. En d'autres termes, chaque colonne sélectionnée dans la table doit être utilisée soit dans les clés, soit dans les fonctions d'agrégation. Pour obtenir un comportement comme dans MySQL, vous pouvez mettre les autres colonnes dans le `any` fonction d'agrégation.

## anyHeavy (x) {#anyheavyx}

Sélectionne une valeur fréquente à l'aide [poids lourds](http://www.cs.umd.edu/~samir/498/karp.pdf) algorithme. S'il y a une valeur qui se produit plus de la moitié des cas dans chacun des threads d'exécution de la requête, cette valeur est renvoyée. Normalement, le résultat est non déterministe.

``` sql
anyHeavy(column)
```

**Argument**

-   `column` – The column name.

**Exemple**

Prendre la [OnTime](../../getting-started/example-datasets/ontime.md) ensemble de données et sélectionnez n'importe quelle valeur `AirlineID` colonne.

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```

## anyLast (x) {#anylastx}

Sélectionne la dernière valeur rencontrés.
Le résultat est tout aussi indéterminé que pour le `any` fonction.

## groupBitAnd {#groupbitand}

S'applique au niveau du BIT `AND` pour les séries de nombres.

``` sql
groupBitAnd(expr)
```

**Paramètre**

`expr` – An expression that results in `UInt*` type.

**Valeur de retour**

La valeur de la `UInt*` type.

**Exemple**

Des données de Test:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Requête:

``` sql
SELECT groupBitAnd(num) FROM t
```

Où `num` est la colonne avec les données de test.

Résultat:

``` text
binary     decimal
00000100 = 4
```

## groupBitOr {#groupbitor}

S'applique au niveau du BIT `OR` pour les séries de nombres.

``` sql
groupBitOr(expr)
```

**Paramètre**

`expr` – An expression that results in `UInt*` type.

**Valeur de retour**

La valeur de la `UInt*` type.

**Exemple**

Des données de Test:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Requête:

``` sql
SELECT groupBitOr(num) FROM t
```

Où `num` est la colonne avec les données de test.

Résultat:

``` text
binary     decimal
01111101 = 125
```

## groupBitXor {#groupbitxor}

S'applique au niveau du BIT `XOR` pour les séries de nombres.

``` sql
groupBitXor(expr)
```

**Paramètre**

`expr` – An expression that results in `UInt*` type.

**Valeur de retour**

La valeur de la `UInt*` type.

**Exemple**

Des données de Test:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Requête:

``` sql
SELECT groupBitXor(num) FROM t
```

Où `num` est la colonne avec les données de test.

Résultat:

``` text
binary     decimal
01101000 = 104
```

## groupBitmap {#groupbitmap}

Calculs Bitmap ou agrégés à partir d'une colonne entière non signée, retour cardinalité de type UInt64, si Ajouter suffixe-State, puis retour [objet bitmap](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**Paramètre**

`expr` – An expression that results in `UInt*` type.

**Valeur de retour**

La valeur de la `UInt64` type.

**Exemple**

Des données de Test:

``` text
UserID
1
1
2
3
```

Requête:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

Résultat:

``` text
num
3
```

## min (x) {#agg_function-min}

Calcule le minimum.

## max (x) {#agg_function-max}

Calcule le maximum.

## argMin (arg, val) {#agg-function-argmin}

Calcule la ‘arg’ valeur pour un minimum ‘val’ valeur. S'il y a plusieurs valeurs différentes de ‘arg’ pour des valeurs minimales de ‘val’ la première de ces valeurs rencontrées est de sortie.

**Exemple:**

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

``` sql
SELECT argMin(user, salary) FROM salary
```

``` text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```

## argMax(arg, val) {#agg-function-argmax}

Calcule la ‘arg’ valeur pour un maximum ‘val’ valeur. S'il y a plusieurs valeurs différentes de ‘arg’ pour les valeurs maximales de ‘val’ la première de ces valeurs rencontrées est de sortie.

## sum(x) {#agg_function-sum}

Calcule la somme.
Ne fonctionne que pour les numéros.

## sumWithOverflow (x) {#sumwithoverflowx}

Calcule la somme des nombres, en utilisant le même type de données pour le résultat que pour les paramètres d'entrée. Si la somme dépasse la valeur maximale pour ce type de données, la fonction renvoie une erreur.

Ne fonctionne que pour les numéros.

## sumMap(clé, valeur), sumMap(Tuple(clé, valeur)) {#agg_functions-summap}

Les totaux de la ‘value’ tableau selon les clés spécifiés dans le ‘key’ tableau.
Le passage du tuple des tableaux de clés et de valeurs est synonyme du passage de deux tableaux de clés et de valeurs.
Le nombre d'éléments dans ‘key’ et ‘value’ doit être identique pour chaque ligne totalisée.
Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

Exemple:

``` sql
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    ),
    statusMapTuple Tuple(Array(Int32), Array(Int32))
) ENGINE = Log;
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests),
    sumMap(statusMapTuple)
FROM sum_map
GROUP BY timeslot
```

``` text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┬─sumMap(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │ ([1,2,3,4,5],[10,10,20,10,10]) │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │ ([4,5,6,7,8],[10,10,20,10,10]) │
└─────────────────────┴──────────────────────────────────────────────┴────────────────────────────────┘
```

## skewPop {#skewpop}

Calcule la [asymétrie](https://en.wikipedia.org/wiki/Skewness) d'une séquence.

``` sql
skewPop(expr)
```

**Paramètre**

`expr` — [Expression](../syntax.md#syntax-expressions) retour d'un nombre.

**Valeur renvoyée**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**Exemple**

``` sql
SELECT skewPop(value) FROM series_with_value_column
```

## skewSamp {#skewsamp}

Calcule la [asymétrie de l'échantillon](https://en.wikipedia.org/wiki/Skewness) d'une séquence.

Il représente une estimation non biaisée de l'asymétrie d'une variable aléatoire si les valeurs passées forme de son échantillon.

``` sql
skewSamp(expr)
```

**Paramètre**

`expr` — [Expression](../syntax.md#syntax-expressions) retour d'un nombre.

**Valeur renvoyée**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). Si `n <= 1` (`n` est la taille de l'échantillon), alors la fonction renvoie `nan`.

**Exemple**

``` sql
SELECT skewSamp(value) FROM series_with_value_column
```

## kurtPop {#kurtpop}

Calcule la [kurtosis](https://en.wikipedia.org/wiki/Kurtosis) d'une séquence.

``` sql
kurtPop(expr)
```

**Paramètre**

`expr` — [Expression](../syntax.md#syntax-expressions) retour d'un nombre.

**Valeur renvoyée**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**Exemple**

``` sql
SELECT kurtPop(value) FROM series_with_value_column
```

## kurtSamp {#kurtsamp}

Calcule la [l'échantillon le coefficient d'aplatissement](https://en.wikipedia.org/wiki/Kurtosis) d'une séquence.

Il représente une estimation non biaisée de la kurtose d'une variable aléatoire si les valeurs passées forment son échantillon.

``` sql
kurtSamp(expr)
```

**Paramètre**

`expr` — [Expression](../syntax.md#syntax-expressions) retour d'un nombre.

**Valeur renvoyée**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). Si `n <= 1` (`n` la taille de l'échantillon), alors la fonction renvoie `nan`.

**Exemple**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column
```

## avg (x) {#agg_function-avg}

Calcule la moyenne.
Ne fonctionne que pour les numéros.
Le résultat est toujours Float64.

## avgWeighted {#avgweighted}

Calcule la [moyenne arithmétique pondérée](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean).

**Syntaxe**

``` sql
avgWeighted(x, weight)
```

**Paramètre**

-   `x` — Values. [Entier](../data-types/int-uint.md) ou [virgule flottante](../data-types/float.md).
-   `weight` — Weights of the values. [Entier](../data-types/int-uint.md) ou [virgule flottante](../data-types/float.md).

Type de `x` et `weight` doit être le même.

**Valeur renvoyée**

-   Moyenne pondérée.
-   `NaN`. Si tous les poids sont égaux à 0.

Type: [Float64](../data-types/float.md).

**Exemple**

Requête:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

Résultat:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

## uniq {#agg_function-uniq}

Calcule le nombre approximatif des différentes valeurs de l'argument.

``` sql
uniq(x[, ...])
```

**Paramètre**

La fonction prend un nombre variable de paramètres. Les paramètres peuvent être `Tuple`, `Array`, `Date`, `DateTime`, `String` ou des types numériques.

**Valeur renvoyée**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)numéro de type.

**Détails de mise en œuvre**

Fonction:

-   Calcule un hachage pour tous les paramètres de l'agrégat, puis l'utilise dans les calculs.

-   Utilise un algorithme d'échantillonnage adaptatif. Pour l'état de calcul, La fonction utilise un échantillon de valeurs de hachage d'éléments jusqu'à 65536.

        This algorithm is very accurate and very efficient on the CPU. When the query contains several of these functions, using `uniq` is almost as fast as using other aggregate functions.

-   Fournit le résultat de manière déterministe (cela ne dépend pas de l'ordre de traitement de la requête).

Nous vous recommandons d'utiliser cette fonction dans presque tous les scénarios.

**Voir Aussi**

-   [uniqcombiné](#agg_function-uniqcombined)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqcombiné {#agg_function-uniqcombined}

Calcule le nombre approximatif de différentes valeurs d'argument.

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

Le `uniqCombined` la fonction est un bon choix pour calculer le nombre de valeurs différentes.

**Paramètre**

La fonction prend un nombre variable de paramètres. Les paramètres peuvent être `Tuple`, `Array`, `Date`, `DateTime`, `String` ou des types numériques.

`HLL_precision` est le logarithme en base 2 du nombre de cellules dans [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Facultatif, vous pouvez utiliser la fonction comme `uniqCombined(x[, ...])`. La valeur par défaut pour `HLL_precision` est 17, qui est effectivement 96 Ko d'espace(2 ^ 17 cellules, 6 bits chacune).

**Valeur renvoyée**

-   Nombre [UInt64](../../sql-reference/data-types/int-uint.md)numéro de type.

**Détails de mise en œuvre**

Fonction:

-   Calcule un hachage (hachage 64 bits pour `String` et 32 bits sinon) pour tous les paramètres dans l'agrégat, puis l'utilise dans les calculs.

-   Utilise une combinaison de trois algorithmes: tableau, table de hachage et HyperLogLog avec une table de correction d'erreur.

        For a small number of distinct elements, an array is used. When the set size is larger, a hash table is used. For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.

-   Fournit le résultat de manière déterministe (cela ne dépend pas de l'ordre de traitement de la requête).

!!! note "Note"
    Comme il utilise le hachage 32 bits pour non-`String` type, le résultat aura une erreur très élevée pour les cardinalités significativement plus grandes que `UINT_MAX` (erreur va augmenter rapidement après quelques dizaines de milliards de valeurs distinctes), donc dans ce cas, vous devez utiliser [uniqCombined64](#agg_function-uniqcombined64)

Par rapport à la [uniq](#agg_function-uniq) la fonction, la `uniqCombined`:

-   Consomme plusieurs fois moins de mémoire.
-   Calcule avec plusieurs fois plus de précision.
-   A généralement des performances légèrement inférieures. Dans certains scénarios, `uniqCombined` peut faire mieux que `uniq` par exemple, avec des requêtes distribuées qui transmettent un grand nombre d'agrégation des états sur le réseau.

**Voir Aussi**

-   [uniq](#agg_function-uniq)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined64 {#agg_function-uniqcombined64}

Même que [uniqcombiné](#agg_function-uniqcombined), mais utilise le hachage 64 bits pour tous les types de données.

## uniqHLL12 {#agg_function-uniqhll12}

Calcule le nombre approximatif de différentes valeurs d'argument, en utilisant [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithme.

``` sql
uniqHLL12(x[, ...])
```

**Paramètre**

La fonction prend un nombre variable de paramètres. Les paramètres peuvent être `Tuple`, `Array`, `Date`, `DateTime`, `String` ou des types numériques.

**Valeur renvoyée**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)numéro de type.

**Détails de mise en œuvre**

Fonction:

-   Calcule un hachage pour tous les paramètres de l'agrégat, puis l'utilise dans les calculs.

-   Utilise L'algorithme HyperLogLog pour approximer le nombre de valeurs d'argument différentes.

        212 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

-   Fournit le résultat déterminé (il ne dépend pas de l'ordre de traitement de la requête).

Nous ne recommandons pas d'utiliser cette fonction. Dans la plupart des cas, l'utilisation de la [uniq](#agg_function-uniq) ou [uniqcombiné](#agg_function-uniqcombined) fonction.

**Voir Aussi**

-   [uniq](#agg_function-uniq)
-   [uniqcombiné](#agg_function-uniqcombined)
-   [uniqExact](#agg_function-uniqexact)

## uniqExact {#agg_function-uniqexact}

Calcule le nombre exact de différentes valeurs d'argument.

``` sql
uniqExact(x[, ...])
```

L'utilisation de la `uniqExact` fonction si vous avez absolument besoin d'un résultat exact. Sinon l'utilisation de la [uniq](#agg_function-uniq) fonction.

Le `uniqExact` la fonction utilise plus de mémoire que `uniq` parce que la taille de l'état a surabondance de croissance que le nombre de valeurs différentes augmente.

**Paramètre**

La fonction prend un nombre variable de paramètres. Les paramètres peuvent être `Tuple`, `Array`, `Date`, `DateTime`, `String` ou des types numériques.

**Voir Aussi**

-   [uniq](#agg_function-uniq)
-   [uniqcombiné](#agg_function-uniqcombined)
-   [uniqHLL12](#agg_function-uniqhll12)

## groupArray(x), groupArray (max_size) (x) {#agg_function-grouparray}

Crée un tableau de valeurs de l'argument.
Les valeurs peuvent être ajoutées au tableau dans une (indéterminée) de commande.

La deuxième version (avec le `max_size` paramètre) limite la taille du tableau résultant à `max_size` élément.
Exemple, `groupArray (1) (x)` est équivalent à `[any (x)]`.

Dans certains cas, vous pouvez toujours compter sur l'ordre de l'exécution. Cela s'applique aux cas où `SELECT` provient d'une sous-requête qui utilise `ORDER BY`.

## groupeparrayinsertat {#grouparrayinsertat}

Insère une valeur dans le tableau à la position spécifiée.

**Syntaxe**

``` sql
groupArrayInsertAt(default_x, size)(x, pos);
```

Si dans une requête plusieurs valeurs sont insérées dans la même position, la fonction se comporte de la manière suivante:

-   Si une requête est exécutée dans un seul thread, la première des valeurs insérées est utilisée.
-   Si une requête est exécutée dans plusieurs threads, le résultat est indéterminé l'une des valeurs insérées.

**Paramètre**

-   `x` — Value to be inserted. [Expression](../syntax.md#syntax-expressions) résultant dans l'un des [types de données pris en charge](../../sql-reference/data-types/index.md).
-   `pos` — Position at which the specified element `x` doit être inséré. L'indice de numérotation dans le tableau commence à partir de zéro. [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).
-   `default_x`— Default value for substituting in empty positions. Optional parameter. [Expression](../syntax.md#syntax-expressions) résultant dans le type de données configuré pour le `x` paramètre. Si `default_x` n'est pas définie, la [les valeurs par défaut](../../sql-reference/statements/create.md#create-default-values) sont utilisés.
-   `size`— Length of the resulting array. Optional parameter. When using this parameter, the default value `default_x` doit être spécifié. [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Valeur renvoyée**

-   Tableau avec des valeurs insérées.

Type: [Tableau](../../sql-reference/data-types/array.md#data-type-array).

**Exemple**

Requête:

``` sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

Résultat:

``` text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐
│ ['0','','1','','2','','3','','4']                         │
└───────────────────────────────────────────────────────────┘
```

Requête:

``` sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

Résultat:

``` text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

Requête:

``` sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

Résultat:

``` text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

Insertion multi-thread d'éléments dans une position.

Requête:

``` sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

Comme un résultat de cette requête, vous obtenez entier aléatoire dans le `[0,9]` gamme. Exemple:

``` text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
```

## groupeparraymovingsum {#agg_function-grouparraymovingsum}

Calcule la somme mobile des valeurs d'entrée.

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

La fonction peut prendre la taille de la fenêtre comme paramètre. Si spécifié, la fonction prend la taille de la fenêtre égal au nombre de lignes dans la colonne.

**Paramètre**

-   `numbers_for_summing` — [Expression](../syntax.md#syntax-expressions) résultant en une valeur de type de données Numérique.
-   `window_size` — Size of the calculation window.

**Valeurs renvoyées**

-   Tableau de la même taille et de même type que les données d'entrée.

**Exemple**

La table d'échantillon:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

Requête:

``` sql
SELECT
    groupArrayMovingSum(int) AS I,
    groupArrayMovingSum(float) AS F,
    groupArrayMovingSum(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,7,14] │ [1.1,3.3000002,7.7000003,15.47] │ [1.10,3.30,7.70,15.47] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

``` sql
SELECT
    groupArrayMovingSum(2)(int) AS I,
    groupArrayMovingSum(2)(float) AS F,
    groupArrayMovingSum(2)(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,6,11] │ [1.1,3.3000002,6.6000004,12.17] │ [1.10,3.30,6.60,12.17] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

## groupArrayMovingAvg {#agg_function-grouparraymovingavg}

Calcule la moyenne mobile des valeurs d'entrée.

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

La fonction peut prendre la taille de la fenêtre comme paramètre. Si spécifié, la fonction prend la taille de la fenêtre égal au nombre de lignes dans la colonne.

**Paramètre**

-   `numbers_for_summing` — [Expression](../syntax.md#syntax-expressions) résultant en une valeur de type de données Numérique.
-   `window_size` — Size of the calculation window.

**Valeurs renvoyées**

-   Tableau de la même taille et de même type que les données d'entrée.

La fonction utilise [l'arrondi vers zéro](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). Il tronque les décimales insignifiantes pour le type de données résultant.

**Exemple**

La table d'échantillon `b`:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

Requête:

``` sql
SELECT
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F───────────────────────────────────┬─D─────────────────────┐
│ [0,0,1,3] │ [0.275,0.82500005,1.9250001,3.8675] │ [0.27,0.82,1.92,3.86] │
└───────────┴─────────────────────────────────────┴───────────────────────┘
```

``` sql
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F────────────────────────────────┬─D─────────────────────┐
│ [0,1,3,5] │ [0.55,1.6500001,3.3000002,6.085] │ [0.55,1.65,3.30,6.08] │
└───────────┴──────────────────────────────────┴───────────────────────┘
```

## groupUniqArray(x), groupUniqArray (max_size) (x) {#groupuniqarrayx-groupuniqarraymax-sizex}

Crée un tableau à partir de différentes valeurs d'argument. La consommation de mémoire est la même que pour la `uniqExact` fonction.

La deuxième version (avec le `max_size` paramètre) limite la taille du tableau résultant à `max_size` élément.
Exemple, `groupUniqArray(1)(x)` est équivalent à `[any(x)]`.

## quantile {#quantile}

Calcule une approximation [quantile](https://en.wikipedia.org/wiki/Quantile) des données numériques de la séquence.

Cette fonction s'applique [réservoir d'échantillonnage](https://en.wikipedia.org/wiki/Reservoir_sampling) avec une taille de réservoir jusqu'à 8192 et un générateur de nombres aléatoires pour l'échantillonnage. Le résultat est non-déterministe. Pour obtenir un quantile exact, Utilisez le [quantileExact](#quantileexact) fonction.

Lorsque vous utilisez plusieurs `quantile*` fonctionne avec différents niveaux dans une requête, les états internes ne sont pas combinées (qui est, la requête fonctionne moins efficacement qu'il le pouvait). Dans ce cas, utilisez la [les quantiles](#quantiles) fonction.

**Syntaxe**

``` sql
quantile(level)(expr)
```

Alias: `median`.

**Paramètre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` la valeur dans la plage de `[0.01, 0.99]`. Valeur par défaut: 0.5. À `level=0.5` la fonction calcule [médian](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [types de données](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md).

**Valeur renvoyée**

-   Approximative de quantiles de niveau spécifié.

Type:

-   [Float64](../../sql-reference/data-types/float.md) pour l'entrée de type de données numériques.
-   [Date](../../sql-reference/data-types/date.md) si les valeurs d'entrée ont le `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) si les valeurs d'entrée ont le `DateTime` type.

**Exemple**

Table d'entrée:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Requête:

``` sql
SELECT quantile(val) FROM t
```

Résultat:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**Voir Aussi**

-   [médian](#median)
-   [les quantiles](#quantiles)

## quantileDeterministic {#quantiledeterministic}

Calcule une approximation [quantile](https://en.wikipedia.org/wiki/Quantile) des données numériques de la séquence.

Cette fonction s'applique [réservoir d'échantillonnage](https://en.wikipedia.org/wiki/Reservoir_sampling) avec une taille de réservoir jusqu'à 8192 et un algorithme déterministe d'échantillonnage. Le résultat est déterministe. Pour obtenir un quantile exact, Utilisez le [quantileExact](#quantileexact) fonction.

Lorsque vous utilisez plusieurs `quantile*` fonctionne avec différents niveaux dans une requête, les états internes ne sont pas combinées (qui est, la requête fonctionne moins efficacement qu'il le pouvait). Dans ce cas, utilisez la [les quantiles](#quantiles) fonction.

**Syntaxe**

``` sql
quantileDeterministic(level)(expr, determinator)
```

Alias: `medianDeterministic`.

**Paramètre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` la valeur dans la plage de `[0.01, 0.99]`. Valeur par défaut: 0.5. À `level=0.5` la fonction calcule [médian](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [types de données](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md).
-   `determinator` — Number whose hash is used instead of a random number generator in the reservoir sampling algorithm to make the result of sampling deterministic. As a determinator you can use any deterministic positive number, for example, a user id or an event id. If the same determinator value occures too often, the function works incorrectly.

**Valeur renvoyée**

-   Approximative de quantiles de niveau spécifié.

Type:

-   [Float64](../../sql-reference/data-types/float.md) pour l'entrée de type de données numériques.
-   [Date](../../sql-reference/data-types/date.md) si les valeurs d'entrée ont le `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) si les valeurs d'entrée ont le `DateTime` type.

**Exemple**

Table d'entrée:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Requête:

``` sql
SELECT quantileDeterministic(val, 1) FROM t
```

Résultat:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**Voir Aussi**

-   [médian](#median)
-   [les quantiles](#quantiles)

## quantileExact {#quantileexact}

Exactement calcule l' [quantile](https://en.wikipedia.org/wiki/Quantile) des données numériques de la séquence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` de mémoire, où `n` est un nombre de valeurs qui ont été passées. Cependant, pour un petit nombre de valeurs, la fonction est très efficace.

Lorsque vous utilisez plusieurs `quantile*` fonctionne avec différents niveaux dans une requête, les états internes ne sont pas combinées (qui est, la requête fonctionne moins efficacement qu'il le pouvait). Dans ce cas, utilisez la [les quantiles](#quantiles) fonction.

**Syntaxe**

``` sql
quantileExact(level)(expr)
```

Alias: `medianExact`.

**Paramètre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` la valeur dans la plage de `[0.01, 0.99]`. Valeur par défaut: 0.5. À `level=0.5` la fonction calcule [médian](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [types de données](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md).

**Valeur renvoyée**

-   Quantiles de niveau spécifié.

Type:

-   [Float64](../../sql-reference/data-types/float.md) pour l'entrée de type de données numériques.
-   [Date](../../sql-reference/data-types/date.md) si les valeurs d'entrée ont le `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) si les valeurs d'entrée ont le `DateTime` type.

**Exemple**

Requête:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

Résultat:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

**Voir Aussi**

-   [médian](#median)
-   [les quantiles](#quantiles)

## quantileExactWeighted {#quantileexactweighted}

Exactement calcule l' [quantile](https://en.wikipedia.org/wiki/Quantile) d'une séquence de données numériques, en tenant compte du poids de chaque élément.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Each value is counted with its weight, as if it is present `weight` times. A hash table is used in the algorithm. Because of this, if the passed values ​​are frequently repeated, the function consumes less RAM than [quantileExact](#quantileexact). Vous pouvez utiliser cette fonction au lieu de `quantileExact` et spécifiez le poids 1.

Lorsque vous utilisez plusieurs `quantile*` fonctionne avec différents niveaux dans une requête, les états internes ne sont pas combinées (qui est, la requête fonctionne moins efficacement qu'il le pouvait). Dans ce cas, utilisez la [les quantiles](#quantiles) fonction.

**Syntaxe**

``` sql
quantileExactWeighted(level)(expr, weight)
```

Alias: `medianExactWeighted`.

**Paramètre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` la valeur dans la plage de `[0.01, 0.99]`. Valeur par défaut: 0.5. À `level=0.5` la fonction calcule [médian](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [types de données](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence members. Weight is a number of value occurrences.

**Valeur renvoyée**

-   Quantiles de niveau spécifié.

Type:

-   [Float64](../../sql-reference/data-types/float.md) pour l'entrée de type de données numériques.
-   [Date](../../sql-reference/data-types/date.md) si les valeurs d'entrée ont le `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) si les valeurs d'entrée ont le `DateTime` type.

**Exemple**

Table d'entrée:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

Requête:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

Résultat:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**Voir Aussi**

-   [médian](#median)
-   [les quantiles](#quantiles)

## quantileTiming {#quantiletiming}

Avec la précision déterminée calcule le [quantile](https://en.wikipedia.org/wiki/Quantile) des données numériques de la séquence.

Le résultat est déterministe (il ne dépend pas de l'ordre de traitement de la requête). La fonction est optimisée pour travailler avec des séquences qui décrivent des distributions comme les temps de chargement des pages web ou les temps de réponse du backend.

Lorsque vous utilisez plusieurs `quantile*` fonctionne avec différents niveaux dans une requête, les états internes ne sont pas combinées (qui est, la requête fonctionne moins efficacement qu'il le pouvait). Dans ce cas, utilisez la [les quantiles](#quantiles) fonction.

**Syntaxe**

``` sql
quantileTiming(level)(expr)
```

Alias: `medianTiming`.

**Paramètre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` la valeur dans la plage de `[0.01, 0.99]`. Valeur par défaut: 0.5. À `level=0.5` la fonction calcule [médian](https://en.wikipedia.org/wiki/Median).

-   `expr` — [Expression](../syntax.md#syntax-expressions) sur une colonne Valeurs renvoyant un [Flottant\*](../../sql-reference/data-types/float.md)numéro de type.

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

**Exactitude**

Le calcul est précis si:

-   Le nombre Total de valeurs ne dépasse pas 5670.
-   Le nombre Total de valeurs dépasse 5670, mais le temps de chargement de la page est inférieur à 1024ms.

Sinon, le résultat du calcul est arrondi au plus proche multiple de 16 ms.

!!! note "Note"
    Pour calculer les quantiles de temps de chargement de page, cette fonction est plus efficace et précise que [quantile](#quantile).

**Valeur renvoyée**

-   Quantiles de niveau spécifié.

Type: `Float32`.

!!! note "Note"
    Si aucune valeur n'est transmise à la fonction (lors de l'utilisation de `quantileTimingIf`), [Nan](../../sql-reference/data-types/float.md#data_type-float-nan-inf) est retourné. Le but est de différencier ces cas de cas qui aboutissent à zéro. Voir [Clause ORDER BY](../statements/select/order-by.md#select-order-by) pour des notes sur le tri `NaN` valeur.

**Exemple**

Table d'entrée:

``` text
┌─response_time─┐
│            72 │
│           112 │
│           126 │
│           145 │
│           104 │
│           242 │
│           313 │
│           168 │
│           108 │
└───────────────┘
```

Requête:

``` sql
SELECT quantileTiming(response_time) FROM t
```

Résultat:

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**Voir Aussi**

-   [médian](#median)
-   [les quantiles](#quantiles)

## quantileTimingWeighted {#quantiletimingweighted}

Avec la précision déterminée calcule le [quantile](https://en.wikipedia.org/wiki/Quantile) d'une séquence de données numériques en fonction du poids de chaque élément de séquence.

Le résultat est déterministe (il ne dépend pas de l'ordre de traitement de la requête). La fonction est optimisée pour travailler avec des séquences qui décrivent des distributions comme les temps de chargement des pages web ou les temps de réponse du backend.

Lorsque vous utilisez plusieurs `quantile*` fonctionne avec différents niveaux dans une requête, les états internes ne sont pas combinées (qui est, la requête fonctionne moins efficacement qu'il le pouvait). Dans ce cas, utilisez la [les quantiles](#quantiles) fonction.

**Syntaxe**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

Alias: `medianTimingWeighted`.

**Paramètre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` la valeur dans la plage de `[0.01, 0.99]`. Valeur par défaut: 0.5. À `level=0.5` la fonction calcule [médian](https://en.wikipedia.org/wiki/Median).

-   `expr` — [Expression](../syntax.md#syntax-expressions) sur une colonne Valeurs renvoyant un [Flottant\*](../../sql-reference/data-types/float.md)numéro de type.

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**Exactitude**

Le calcul est précis si:

-   Le nombre Total de valeurs ne dépasse pas 5670.
-   Le nombre Total de valeurs dépasse 5670, mais le temps de chargement de la page est inférieur à 1024ms.

Sinon, le résultat du calcul est arrondi au plus proche multiple de 16 ms.

!!! note "Note"
    Pour calculer les quantiles de temps de chargement de page, cette fonction est plus efficace et précise que [quantile](#quantile).

**Valeur renvoyée**

-   Quantiles de niveau spécifié.

Type: `Float32`.

!!! note "Note"
    Si aucune valeur n'est transmise à la fonction (lors de l'utilisation de `quantileTimingIf`), [Nan](../../sql-reference/data-types/float.md#data_type-float-nan-inf) est retourné. Le but est de différencier ces cas de cas qui aboutissent à zéro. Voir [Clause ORDER BY](../statements/select/order-by.md#select-order-by) pour des notes sur le tri `NaN` valeur.

**Exemple**

Table d'entrée:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

Requête:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

Résultat:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

**Voir Aussi**

-   [médian](#median)
-   [les quantiles](#quantiles)

## quantileTDigest {#quantiletdigest}

Calcule une approximation [quantile](https://en.wikipedia.org/wiki/Quantile) d'une séquence de données numériques utilisant [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithme.

L'erreur maximale est de 1%. La consommation de mémoire est `log(n)`, où `n` est un certain nombre de valeurs. Le résultat dépend de l'ordre d'exécution de la requête et n'est pas déterministe.

La performance de la fonction est inférieure à la performance de [quantile](#quantile) ou [quantileTiming](#quantiletiming). En termes de rapport entre la taille de L'état et la précision, cette fonction est bien meilleure que `quantile`.

Lorsque vous utilisez plusieurs `quantile*` fonctionne avec différents niveaux dans une requête, les états internes ne sont pas combinées (qui est, la requête fonctionne moins efficacement qu'il le pouvait). Dans ce cas, utilisez la [les quantiles](#quantiles) fonction.

**Syntaxe**

``` sql
quantileTDigest(level)(expr)
```

Alias: `medianTDigest`.

**Paramètre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` la valeur dans la plage de `[0.01, 0.99]`. Valeur par défaut: 0.5. À `level=0.5` la fonction calcule [médian](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [types de données](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md).

**Valeur renvoyée**

-   Approximative de quantiles de niveau spécifié.

Type:

-   [Float64](../../sql-reference/data-types/float.md) pour l'entrée de type de données numériques.
-   [Date](../../sql-reference/data-types/date.md) si les valeurs d'entrée ont le `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) si les valeurs d'entrée ont le `DateTime` type.

**Exemple**

Requête:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

Résultat:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**Voir Aussi**

-   [médian](#median)
-   [les quantiles](#quantiles)

## quantileTDigestWeighted {#quantiletdigestweighted}

Calcule une approximation [quantile](https://en.wikipedia.org/wiki/Quantile) d'une séquence de données numériques utilisant [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithme. La fonction prend en compte le poids de chaque séquence de membre. L'erreur maximale est de 1%. La consommation de mémoire est `log(n)`, où `n` est un certain nombre de valeurs.

La performance de la fonction est inférieure à la performance de [quantile](#quantile) ou [quantileTiming](#quantiletiming). En termes de rapport entre la taille de L'état et la précision, cette fonction est bien meilleure que `quantile`.

Le résultat dépend de l'ordre d'exécution de la requête et n'est pas déterministe.

Lorsque vous utilisez plusieurs `quantile*` fonctionne avec différents niveaux dans une requête, les états internes ne sont pas combinées (qui est, la requête fonctionne moins efficacement qu'il le pouvait). Dans ce cas, utilisez la [les quantiles](#quantiles) fonction.

**Syntaxe**

``` sql
quantileTDigest(level)(expr)
```

Alias: `medianTDigest`.

**Paramètre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` la valeur dans la plage de `[0.01, 0.99]`. Valeur par défaut: 0.5. À `level=0.5` la fonction calcule [médian](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [types de données](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**Valeur renvoyée**

-   Approximative de quantiles de niveau spécifié.

Type:

-   [Float64](../../sql-reference/data-types/float.md) pour l'entrée de type de données numériques.
-   [Date](../../sql-reference/data-types/date.md) si les valeurs d'entrée ont le `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) si les valeurs d'entrée ont le `DateTime` type.

**Exemple**

Requête:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

Résultat:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**Voir Aussi**

-   [médian](#median)
-   [les quantiles](#quantiles)

## médian {#median}

Le `median*` les fonctions sont les Alias pour le correspondant `quantile*` fonction. Ils calculent la médiane d'un échantillon de données numériques.

Fonction:

-   `median` — Alias for [quantile](#quantile).
-   `medianDeterministic` — Alias for [quantileDeterministic](#quantiledeterministic).
-   `medianExact` — Alias for [quantileExact](#quantileexact).
-   `medianExactWeighted` — Alias for [quantileExactWeighted](#quantileexactweighted).
-   `medianTiming` — Alias for [quantileTiming](#quantiletiming).
-   `medianTimingWeighted` — Alias for [quantileTimingWeighted](#quantiletimingweighted).
-   `medianTDigest` — Alias for [quantileTDigest](#quantiletdigest).
-   `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](#quantiletdigestweighted).

**Exemple**

Table d'entrée:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Requête:

``` sql
SELECT medianDeterministic(val, 1) FROM t
```

Résultat:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```

## quantiles(level1, level2, …)(x) {#quantiles}

Toutes les fonctions quantiles ont également des fonctions quantiles correspondantes: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`. Ces fonctions calculent tous les quantiles des niveaux listés en une seule passe et renvoient un tableau des valeurs résultantes.

## varSamp (x) {#varsampx}

Calcule le montant `Σ((x - x̅)^2) / (n - 1)`, où `n` est la taille de l'échantillon et `x̅`est la valeur moyenne de `x`.

Il représente une estimation non biaisée de la variance d'une variable aléatoire si les valeurs passées forment son échantillon.

Retourner `Float64`. Lorsque `n <= 1`, retourner `+∞`.

!!! note "Note"
    Cette fonction utilise un algorithme numériquement instable. Si vous avez besoin d' [stabilité numérique](https://en.wikipedia.org/wiki/Numerical_stability) dans les calculs, utiliser le `varSampStable` fonction. Il fonctionne plus lentement, mais fournit une erreur de calcul inférieure.

## varPop (x) {#varpopx}

Calcule le montant `Σ((x - x̅)^2) / n`, où `n` est la taille de l'échantillon et `x̅`est la valeur moyenne de `x`.

En d'autres termes, dispersion pour un ensemble de valeurs. Retourner `Float64`.

!!! note "Note"
    Cette fonction utilise un algorithme numériquement instable. Si vous avez besoin d' [stabilité numérique](https://en.wikipedia.org/wiki/Numerical_stability) dans les calculs, utiliser le `varPopStable` fonction. Il fonctionne plus lentement, mais fournit une erreur de calcul inférieure.

## stddevSamp (x) {#stddevsampx}

Le résultat est égal à la racine carrée de `varSamp(x)`.

!!! note "Note"
    Cette fonction utilise un algorithme numériquement instable. Si vous avez besoin d' [stabilité numérique](https://en.wikipedia.org/wiki/Numerical_stability) dans les calculs, utiliser le `stddevSampStable` fonction. Il fonctionne plus lentement, mais fournit une erreur de calcul inférieure.

## stddevPop (x) {#stddevpopx}

Le résultat est égal à la racine carrée de `varPop(x)`.

!!! note "Note"
    Cette fonction utilise un algorithme numériquement instable. Si vous avez besoin d' [stabilité numérique](https://en.wikipedia.org/wiki/Numerical_stability) dans les calculs, utiliser le `stddevPopStable` fonction. Il fonctionne plus lentement, mais fournit une erreur de calcul inférieure.

## topK (N) (x) {#topknx}

Renvoie un tableau des valeurs approximativement les plus fréquentes dans la colonne spécifiée. Le tableau est trié par ordre décroissant de fréquence approximative des valeurs (et non par les valeurs elles-mêmes).

Met en œuvre la [Gain De Place Filtré](http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf) algorithme d'analyse de TopK, basé sur l'algorithme de réduction et de combinaison de [Économie D'Espace Parallèle](https://arxiv.org/pdf/1401.0702.pdf).

``` sql
topK(N)(column)
```

Cette fonction ne fournit pas un résultat garanti. Dans certaines situations, des erreurs peuvent se produire et renvoyer des valeurs fréquentes qui ne sont pas les valeurs les plus fréquentes.

Nous vous recommandons d'utiliser l' `N < 10` valeur; performance est réduite avec grand `N` valeur. Valeur maximale de `N = 65536`.

**Paramètre**

-   ‘N’ est le nombre d'éléments de retour.

Si le paramètre est omis, la valeur par défaut 10 est utilisé.

**Argument**

-   ' x ' – The value to calculate frequency.

**Exemple**

Prendre la [OnTime](../../getting-started/example-datasets/ontime.md) ensemble de données et sélectionnez les trois valeurs les plus fréquentes `AirlineID` colonne.

``` sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```

``` text
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```

## topKWeighted {#topkweighted}

Semblable à `topK` mais prend un argument de type entier - `weight`. Chaque valeur est comptabilisée `weight` les temps de calcul de fréquence.

**Syntaxe**

``` sql
topKWeighted(N)(x, weight)
```

**Paramètre**

-   `N` — The number of elements to return.

**Argument**

-   `x` – The value.
-   `weight` — The weight. [UInt8](../../sql-reference/data-types/int-uint.md).

**Valeur renvoyée**

Renvoie un tableau des valeurs avec la somme approximative maximale des poids.

**Exemple**

Requête:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

Résultat:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```

## covarSamp(x, y) {#covarsampx-y}

Calcule la valeur de `Σ((x - x̅)(y - y̅)) / (n - 1)`.

Renvoie Float64. Lorsque `n <= 1`, returns +∞.

!!! note "Note"
    Cette fonction utilise un algorithme numériquement instable. Si vous avez besoin d' [stabilité numérique](https://en.wikipedia.org/wiki/Numerical_stability) dans les calculs, utiliser le `covarSampStable` fonction. Il fonctionne plus lentement, mais fournit une erreur de calcul inférieure.

## covarPop (x, y) {#covarpopx-y}

Calcule la valeur de `Σ((x - x̅)(y - y̅)) / n`.

!!! note "Note"
    Cette fonction utilise un algorithme numériquement instable. Si vous avez besoin d' [stabilité numérique](https://en.wikipedia.org/wiki/Numerical_stability) dans les calculs, utiliser le `covarPopStable` fonction. Cela fonctionne plus lentement mais fournit une erreur de calcul inférieure.

## corr (x, y) {#corrx-y}

Calcule le coefficient de corrélation de Pearson: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`.

!!! note "Note"
    Cette fonction utilise un algorithme numériquement instable. Si vous avez besoin d' [stabilité numérique](https://en.wikipedia.org/wiki/Numerical_stability) dans les calculs, utiliser le `corrStable` fonction. Il fonctionne plus lentement, mais fournit une erreur de calcul inférieure.

## categoricalInformationValue {#categoricalinformationvalue}

Calcule la valeur de `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` pour chaque catégorie.

``` sql
categoricalInformationValue(category1, category2, ..., tag)
```

Le résultat indique comment une caractéristique discrète (catégorique) `[category1, category2, ...]` contribuer à un modèle d'apprentissage qui prédit la valeur de `tag`.

## simplelineearregression {#simplelinearregression}

Effectue une régression linéaire simple (unidimensionnelle).

``` sql
simpleLinearRegression(x, y)
```

Paramètre:

-   `x` — Column with dependent variable values.
-   `y` — Column with explanatory variable values.

Valeurs renvoyées:

Constant `(a, b)` de la ligne `y = a*x + b`.

**Exemple**

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])─┐
│ (1,0)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])─┐
│ (1,3)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

## stochasticLinearRegression {#agg_functions-stochasticlinearregression}

Cette fonction implémente la régression linéaire stochastique. Il prend en charge les paramètres personnalisés pour le taux d'apprentissage, le coefficient de régularisation L2, la taille de mini-lot et a peu de méthodes pour mettre à jour les poids ([Adam](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) (utilisé par défaut), [simple SGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent), [Élan](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum), [Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)).

### Paramètre {#agg_functions-stochasticlinearregression-parameters}

Il y a 4 paramètres personnalisables. Ils sont passés à la fonction séquentiellement, mais il n'est pas nécessaire de passer tous les quatre-les valeurs par défaut seront utilisées, mais un bon modèle nécessite un réglage des paramètres.

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` est le coefficient sur la longueur de l'étape, lorsque l'étape de descente de gradient est effectuée. Un taux d'apprentissage trop élevé peut entraîner des poids infinis du modèle. Par défaut est `0.00001`.
2.  `l2 regularization coefficient` ce qui peut aider à éviter le surajustement. Par défaut est `0.1`.
3.  `mini-batch size` définit le nombre d'éléments, dont les gradients seront calculés et additionnés pour effectuer une étape de descente de gradient. La descente stochastique Pure utilise un élément, mais avoir de petits lots (environ 10 éléments) rend les étapes de gradient plus stables. Par défaut est `15`.
4.  `method for updating weights` ils sont: `Adam` (par défaut), `SGD`, `Momentum`, `Nesterov`. `Momentum` et `Nesterov` nécessitent un peu plus de calculs et de mémoire, mais ils sont utiles en termes de vitesse de convergence et de stabilité des méthodes de gradient stochastique.

### Utilisation {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` est utilisé en deux étapes: ajustement du modèle et prédiction sur de nouvelles données. Afin de correspondre le modèle et l'enregistrer son état pour utilisation ultérieure nous utilisons `-State` combinator, qui enregistre essentiellement l'état (poids du modèle, etc.).
Pour prédire nous utilisons la fonction [evalMLMethod](../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod) qui prend un état comme un argument ainsi que des fonctionnalités à prévoir sur.

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** Raccord

Une telle requête peut être utilisé.

``` sql
CREATE TABLE IF NOT EXISTS train_data
(
    param1 Float64,
    param2 Float64,
    target Float64
) ENGINE = Memory;

CREATE TABLE your_model ENGINE = Memory AS SELECT
stochasticLinearRegressionState(0.1, 0.0, 5, 'SGD')(target, param1, param2)
AS state FROM train_data;
```

Ici, nous devons également insérer des données dans `train_data` table. Le nombre de paramètres n'est pas fixe, il dépend uniquement du nombre d'arguments, passés dans `linearRegressionState`. Ils doivent tous être des valeurs numériques.
Notez que la colonne avec la valeur cible (que nous aimerions apprendre à prédire) est insérée comme premier argument.

**2.** Prédire

Après avoir enregistré un État dans la table, nous pouvons l'utiliser plusieurs fois pour la prédiction, ou même fusionner avec d'autres États et créer de nouveaux modèles encore meilleurs.

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

La requête renvoie une colonne de valeurs prédites. Notez que le premier argument de `evalMLMethod` être `AggregateFunctionState` objet, sont ensuite des colonnes de fonctionnalités.

`test_data` est un tableau comme `train_data` mais peut ne pas contenir de valeur cible.

### Note {#agg_functions-stochasticlinearregression-notes}

1.  Pour fusionner deux modèles l'utilisateur peut créer une telle requête:
    `sql  SELECT state1 + state2 FROM your_models`
    où `your_models` le tableau contient les deux modèles. Cette requête renvoie la nouvelle `AggregateFunctionState` objet.

2.  L'utilisateur peut récupérer les poids du modèle pour ses propres fins, sans enregistrer le modèle, si aucune `-State` combinator est utilisé.
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    Une telle requête s'adaptera au Modèle et retournera ses poids-d'abord sont des poids, qui correspondent aux paramètres du modèle, le dernier est un biais. Ainsi, dans l'exemple ci-dessus, la requête renvoie une colonne avec 3 valeurs.

**Voir Aussi**

-   [stochasticLogisticRegression](#agg_functions-stochasticlogisticregression)
-   [Différence entre les régressions linéaires et logistiques](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

Cette fonction implémente la régression logistique stochastique. Il peut être utilisé pour le problème de classification binaire, prend en charge les mêmes paramètres personnalisés que stochasticLinearRegression et fonctionne de la même manière.

### Paramètre {#agg_functions-stochasticlogisticregression-parameters}

Les paramètres sont exactement les mêmes que dans stochasticLinearRegression:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
Pour plus d'informations, voir [paramètre](#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

1.  Raccord

<!-- -->

    See the `Fitting` section in the [stochasticLinearRegression](#stochasticlinearregression-usage-fitting) description.

    Predicted labels have to be in \[-1, 1\].

1.  Prédire

<!-- -->

    Using saved state we can predict probability of object having label `1`.

    ``` sql
    WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) FROM test_data
    ```

    The query will return a column of probabilities. Note that first argument of `evalMLMethod` is `AggregateFunctionState` object, next are columns of features.

    We can also set a bound of probability, which assigns elements to different labels.

    ``` sql
    SELECT ans < 1.1 AND ans > 0.5 FROM
    (WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) AS ans FROM test_data)
    ```

    Then the result will be labels.

    `test_data` is a table like `train_data` but may not contain target value.

**Voir Aussi**

-   [stochasticLinearRegression](#agg_functions-stochasticlinearregression)
-   [Différence entre les régressions linéaires et logistiques.](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## groupBitmapAnd {#groupbitmapand}

Calculs le et d'une colonne bitmap, retour cardinalité de type UInt64, si Ajouter suffixe-État, puis retour [objet bitmap](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapAnd(expr)
```

**Paramètre**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` type.

**Valeur de retour**

La valeur de la `UInt64` type.

**Exemple**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapAnd(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapAnd(z)─┐
│               3   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapAndState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapAndState(z)))─┐
│ [6,8,10]                                         │
└──────────────────────────────────────────────────┘
```

## groupBitmapOr {#groupbitmapor}

Calculs le ou d'une colonne bitmap, retour cardinalité de type UInt64, si Ajouter suffixe-État, puis retour [objet bitmap](../../sql-reference/functions/bitmap-functions.md). C'est l'équivalent de `groupBitmapMerge`.

``` sql
groupBitmapOr(expr)
```

**Paramètre**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` type.

**Valeur de retour**

La valeur de la `UInt64` type.

**Exemple**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapOr(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapOr(z)─┐
│             15   │
└──────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapOrState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapOrState(z)))─┐
│ [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]           │
└─────────────────────────────────────────────────┘
```

## groupBitmapXor {#groupbitmapxor}

Calculs le XOR d'une colonne bitmap, retour cardinalité de type UInt64, si Ajouter suffixe-État, puis retour [objet bitmap](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapOr(expr)
```

**Paramètre**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` type.

**Valeur de retour**

La valeur de la `UInt64` type.

**Exemple**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapXor(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapXor(z)─┐
│              10   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapXorState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapXorState(z)))─┐
│ [1,3,5,6,8,10,11,13,14,15]                       │
└──────────────────────────────────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/agg_functions/reference/) <!--hide-->
