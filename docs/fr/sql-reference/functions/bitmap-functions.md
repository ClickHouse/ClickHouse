---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: Bitmap
---

# Fonctions De Bitmap {#bitmap-functions}

Les fonctions Bitmap fonctionnent pour le calcul de la valeur de L'objet de deux bitmaps, il s'agit de renvoyer un nouveau bitmap ou une cardinalité tout en utilisant le calcul de la formule, tel que and, or, xor, and not, etc.

Il existe 2 types de méthodes de construction pour L'objet Bitmap. L'un doit être construit par la fonction d'agrégation groupBitmap avec-State, l'autre doit être construit par L'objet Array. Il est également de convertir L'objet Bitmap en objet tableau.

RoaringBitmap est enveloppé dans une structure de données pendant le stockage réel des objets Bitmap. Lorsque la cardinalité est inférieure ou égale à 32, elle utilise Set objet. Lorsque la cardinalité est supérieure à 32, elle utilise l'objet RoaringBitmap. C'est pourquoi le stockage de faible cardinalité jeu est plus rapide.

Pour plus d'informations sur RoaringBitmap, voir: [CRoaring](https://github.com/RoaringBitmap/CRoaring).

## bitmapBuild {#bitmap_functions-bitmapbuild}

Construire un bitmap à partir d'un tableau entier non signé.

``` sql
bitmapBuild(array)
```

**Paramètre**

-   `array` – unsigned integer array.

**Exemple**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res)
```

``` text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)    │
└─────┴──────────────────────────────────────────────┘
```

## bitmapToArray {#bitmaptoarray}

Convertir bitmap en tableau entier.

``` sql
bitmapToArray(bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapSubsetInRange {#bitmap-functions-bitmapsubsetinrange}

Retourne le sous-ensemble dans la plage spécifiée (n'inclut pas le range\_end).

``` sql
bitmapSubsetInRange(bitmap, range_start, range_end)
```

**Paramètre**

-   `bitmap` – [Objet Bitmap](#bitmap_functions-bitmapbuild).
-   `range_start` – range start point. Type: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `range_end` – range end point(excluded). Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Exemple**

``` sql
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

``` text
┌─res───────────────┐
│ [30,31,32,33,100] │
└───────────────────┘
```

## bitmapSubsetLimit {#bitmapsubsetlimit}

Crée un sous-ensemble de bitmap avec n éléments pris entre `range_start` et `cardinality_limit`.

**Syntaxe**

``` sql
bitmapSubsetLimit(bitmap, range_start, cardinality_limit)
```

**Paramètre**

-   `bitmap` – [Objet Bitmap](#bitmap_functions-bitmapbuild).
-   `range_start` – The subset starting point. Type: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `cardinality_limit` – The subset cardinality upper limit. Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Valeur renvoyée**

Ensemble.

Type: `Bitmap object`.

**Exemple**

Requête:

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

Résultat:

``` text
┌─res───────────────────────┐
│ [30,31,32,33,100,200,500] │
└───────────────────────────┘
```

## bitmapContains {#bitmap_functions-bitmapcontains}

Vérifie si le bitmap contient un élément.

``` sql
bitmapContains(haystack, needle)
```

**Paramètre**

-   `haystack` – [Objet Bitmap](#bitmap_functions-bitmapbuild) où la fonction recherche.
-   `needle` – Value that the function searches. Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

-   0 — If `haystack` ne contient pas de `needle`.
-   1 — If `haystack` contenir `needle`.

Type: `UInt8`.

**Exemple**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny {#bitmaphasany}

Vérifie si deux bitmaps ont une intersection par certains éléments.

``` sql
bitmapHasAny(bitmap1, bitmap2)
```

Si vous êtes sûr que `bitmap2` contient strictement un élément, envisagez d'utiliser le [bitmapContains](#bitmap_functions-bitmapcontains) fonction. Cela fonctionne plus efficacement.

**Paramètre**

-   `bitmap*` – bitmap object.

**Les valeurs de retour**

-   `1`, si `bitmap1` et `bitmap2` avoir un élément similaire au moins.
-   `0`, autrement.

**Exemple**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll {#bitmaphasall}

Analogue à `hasAll(array, array)` renvoie 1 si le premier bitmap contient tous les éléments du second, 0 sinon.
Si le deuxième argument est un bitmap vide, alors renvoie 1.

``` sql
bitmapHasAll(bitmap,bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  0  │
└─────┘
```

## bitmapCardinality {#bitmapcardinality}

Retrun bitmap cardinalité de type UInt64.

``` sql
bitmapCardinality(bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapMin {#bitmapmin}

Retrun la plus petite valeur de type UInt64 dans l'ensemble, UINT32\_MAX si l'ensemble est vide.

    bitmapMin(bitmap)

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   1 │
    └─────┘

## bitmapMax {#bitmapmax}

Retrun la plus grande valeur de type UInt64 dans l'ensemble, 0 si l'ensemble est vide.

    bitmapMax(bitmap)

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   5 │
    └─────┘

## bitmapTransform {#bitmaptransform}

Transformer un tableau de valeurs d'une image à l'autre tableau de valeurs, le résultat est une nouvelle image.

    bitmapTransform(bitmap, from_array, to_array)

**Paramètre**

-   `bitmap` – bitmap object.
-   `from_array` – UInt32 array. For idx in range \[0, from\_array.size()), if bitmap contains from\_array\[idx\], then replace it with to\_array\[idx\]. Note that the result depends on array ordering if there are common elements between from\_array and to\_array.
-   `to_array` – UInt32 array, its size shall be the same to from\_array.

**Exemple**

``` sql
SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cast([5,999,2] as Array(UInt32)), cast([2,888,20] as Array(UInt32)))) AS res
```

    ┌─res───────────────────┐
    │ [1,3,4,6,7,8,9,10,20] │
    └───────────────────────┘

## bitmapAnd {#bitmapand}

Deux bitmap et calcul, le résultat est un nouveau bitmap.

``` sql
bitmapAnd(bitmap,bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr {#bitmapor}

Deux bitmap ou calcul, le résultat est un nouveau bitmap.

``` sql
bitmapOr(bitmap,bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor {#bitmapxor}

Deux bitmap xor calcul, le résultat est une nouvelle image.

``` sql
bitmapXor(bitmap,bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapetnot {#bitmapandnot}

Deux Bitmap andnot calcul, le résultat est un nouveau bitmap.

``` sql
bitmapAndnot(bitmap,bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapetcardinalité {#bitmapandcardinality}

Deux bitmap et calcul, retour cardinalité de type UInt64.

``` sql
bitmapAndCardinality(bitmap,bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## bitmapOrCardinality {#bitmaporcardinality}

Deux bitmap ou calcul, retour cardinalité de type UInt64.

``` sql
bitmapOrCardinality(bitmap,bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality {#bitmapxorcardinality}

Deux bitmap XOR calcul, retour cardinalité de type UInt64.

``` sql
bitmapXorCardinality(bitmap,bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   4 │
└─────┘
```

## bitmapetnotcardinality {#bitmapandnotcardinality}

Deux bitmap andnot calcul, retour cardinalité de type UInt64.

``` sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**Paramètre**

-   `bitmap` – bitmap object.

**Exemple**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   2 │
└─────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/bitmap_functions/) <!--hide-->
