---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: "E\u015Flem"
---

# Bitmap İşlevleri {#bitmap-functions}

Bitmap işlevleri iki bit eşlemler nesne değeri hesaplama için çalışmak, yeni bitmap veya kardinality formül hesaplama, and, or, xor ve not, vb gibi kullanırken döndürmektir.

Bitmap nesnesi için 2 çeşit inşaat yöntemi vardır. Biri-State ile toplama işlevi groupBitmap tarafından inşa edilecek, diğeri Array nesnesi tarafından inşa edilecek. Ayrıca bitmap nesnesini dizi nesnesine dönüştürmektir.

Roaringbitmap, Bitmap nesnelerinin gerçek depolanması sırasında bir veri yapısına sarılır. Önemlilik 32'den küçük veya eşit olduğunda, Set objet kullanır. Kardinality 32'den büyük olduğunda, roaringbitmap nesnesi kullanır. Bu nedenle düşük kardinalite kümesinin depolanması daha hızlıdır.

RoaringBitmap hakkında daha fazla bilgi için bkz: [CRoaring](https://github.com/RoaringBitmap/CRoaring).

## bitmapBuild {#bitmap_functions-bitmapbuild}

İmzasız tamsayı dizisinden bir bit eşlem oluşturun.

``` sql
bitmapBuild(array)
```

**Parametre**

-   `array` – unsigned integer array.

**Örnek**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res)
```

``` text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)    │
└─────┴──────────────────────────────────────────────┘
```

## bitmapToArray {#bitmaptoarray}

Bitmap'i tamsayı dizisine dönüştürün.

``` sql
bitmapToArray(bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapsubsetınrange {#bitmap-functions-bitmapsubsetinrange}

Belirtilen aralıktaki alt kümesi döndürür (range_end içermez).

``` sql
bitmapSubsetInRange(bitmap, range_start, range_end)
```

**Parametre**

-   `bitmap` – [Bitmap nesnesi](#bitmap_functions-bitmapbuild).
-   `range_start` – range start point. Type: [Uİnt32](../../sql-reference/data-types/int-uint.md).
-   `range_end` – range end point(excluded). Type: [Uİnt32](../../sql-reference/data-types/int-uint.md).

**Örnek**

``` sql
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

``` text
┌─res───────────────┐
│ [30,31,32,33,100] │
└───────────────────┘
```

## bitmapSubsetLimit {#bitmapsubsetlimit}

Arasında alınan n öğeleri ile bitmap bir alt kümesi oluşturur `range_start` ve `cardinality_limit`.

**Sözdizimi**

``` sql
bitmapSubsetLimit(bitmap, range_start, cardinality_limit)
```

**Parametre**

-   `bitmap` – [Bitmap nesnesi](#bitmap_functions-bitmapbuild).
-   `range_start` – The subset starting point. Type: [Uİnt32](../../sql-reference/data-types/int-uint.md).
-   `cardinality_limit` – The subset cardinality upper limit. Type: [Uİnt32](../../sql-reference/data-types/int-uint.md).

**Döndürülen değer**

Alt.

Tür: `Bitmap object`.

**Örnek**

Sorgu:

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

Sonuç:

``` text
┌─res───────────────────────┐
│ [30,31,32,33,100,200,500] │
└───────────────────────────┘
```

## bitmapContains {#bitmap_functions-bitmapcontains}

Bit eşlem bir öğe içerip içermediğini denetler.

``` sql
bitmapContains(haystack, needle)
```

**Parametre**

-   `haystack` – [Bitmap nesnesi](#bitmap_functions-bitmapbuild), fonksiyon arar nerede.
-   `needle` – Value that the function searches. Type: [Uİnt32](../../sql-reference/data-types/int-uint.md).

**Döndürülen değerler**

-   0 — If `haystack` içermez `needle`.
-   1 — If `haystack` içeriyor `needle`.

Tür: `UInt8`.

**Örnek**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny {#bitmaphasany}

İki bit eşlemin bazı öğelerle kesiştiği olup olmadığını kontrol eder.

``` sql
bitmapHasAny(bitmap1, bitmap2)
```

Eğer eminseniz `bitmap2` kesinlikle bir öğe içerir, kullanmayı düşünün [bitmapContains](#bitmap_functions-bitmapcontains) İşlev. Daha verimli çalışır.

**Parametre**

-   `bitmap*` – bitmap object.

**Dönüş değerleri**

-   `1`, eğer `bitmap1` ve `bitmap2` en azından benzer bir öğeye sahip olun.
-   `0`, başka.

**Örnek**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll {#bitmaphasall}

Benzer `hasAll(array, array)` ilk bit eşlem, ikincisinin tüm öğelerini içeriyorsa, 1 değerini döndürür, aksi halde 0.
İkinci bağımsız değişken boş bir bit eşlem ise, 1 döndürür.

``` sql
bitmapHasAll(bitmap,bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  0  │
└─────┘
```

## bitmapCardinality {#bitmapcardinality}

Retrun bit eşlem kardinalite türü Uİnt64.

``` sql
bitmapCardinality(bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapMin {#bitmapmin}

Kümedeki uint64 türünün en küçük değerini yeniden çalıştırın, küme boşsa UİNT32_MAX.

    bitmapMin(bitmap)

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   1 │
    └─────┘

## bitmapMax {#bitmapmax}

Küme boşsa, kümedeki uint64 türünün en büyük değerini 0 olarak yeniden çalıştırın.

    bitmapMax(bitmap)

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   5 │
    └─────┘

## bitmapTransform {#bitmaptransform}

Bitmap'teki bir değer dizisini başka bir değer dizisine dönüştürün, sonuç yeni bir bitmap'tir.

    bitmapTransform(bitmap, from_array, to_array)

**Parametre**

-   `bitmap` – bitmap object.
-   `from_array` – UInt32 array. For idx in range \[0, from_array.size()), if bitmap contains from_array\[idx\], then replace it with to_array\[idx\]. Note that the result depends on array ordering if there are common elements between from_array and to_array.
-   `to_array` – UInt32 array, its size shall be the same to from_array.

**Örnek**

``` sql
SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cast([5,999,2] as Array(UInt32)), cast([2,888,20] as Array(UInt32)))) AS res
```

    ┌─res───────────────────┐
    │ [1,3,4,6,7,8,9,10,20] │
    └───────────────────────┘

## bitmapAnd {#bitmapand}

İki bitmap ve hesaplama, sonuç yeni bir bitmap'tir.

``` sql
bitmapAnd(bitmap,bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr {#bitmapor}

İki bitmap veya hesaplama, sonuç yeni bir bitmap'tir.

``` sql
bitmapOr(bitmap,bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor {#bitmapxor}

İki bitmap XOR hesaplama, sonuç yeni bir bitmap.

``` sql
bitmapXor(bitmap,bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot {#bitmapandnot}

İki bit eşlem andnot hesaplama, sonuç yeni bir bit eşlem.

``` sql
bitmapAndnot(bitmap,bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapAndCardinality {#bitmapandcardinality}

İki bitmap ve hesaplama, uint64 türünün kardinalliğini döndürür.

``` sql
bitmapAndCardinality(bitmap,bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## bitmapOrCardinality {#bitmaporcardinality}

İki bitmap veya hesaplama, uint64 türünün kardinalliğini döndürür.

``` sql
bitmapOrCardinality(bitmap,bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality {#bitmapxorcardinality}

İki bitmap XOR hesaplama, uint64 türünün kardinalliğini döndürür.

``` sql
bitmapXorCardinality(bitmap,bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   4 │
└─────┘
```

## bitmapAndnotCardinality {#bitmapandnotcardinality}

İki bitmap andnot hesaplama, uint64 türünün kardinalliğini döndürür.

``` sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**Parametre**

-   `bitmap` – bitmap object.

**Örnek**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   2 │
└─────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/bitmap_functions/) <!--hide-->
