---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "Y\xFCksek Sipari\u015F"
---

# Yüksek mertebeden fonksiyonlar {#higher-order-functions}

## `->` operatör, lambda (params, expr) fonksiyonu {#operator-lambdaparams-expr-function}

Allows describing a lambda function for passing to a higher-order function. The left side of the arrow has a formal parameter, which is any ID, or multiple formal parameters – any IDs in a tuple. The right side of the arrow has an expression that can use these formal parameters, as well as any table columns.

Örnekler: `x -> 2 * x, str -> str != Referer.`

Daha yüksek mertebeden işlevler yalnızca Lambda işlevlerini işlevsel argümanları olarak kabul edebilir.

Birden çok bağımsız değişkeni kabul eden bir lambda işlevi, daha yüksek mertebeden bir işleve geçirilebilir. Bu durumda, yüksek mertebeden işlev, bu bağımsız değişkenlerin karşılık geleceği aynı uzunlukta birkaç diziden geçirilir.

Gibi bazı işlevler için [arrayCount](#higher_order_functions-array-count) veya [arraySum](#higher_order_functions-array-count), ilk argüman (lambda işlevi) ihmal edilebilir. Bu durumda, aynı eşleme varsayılır.

Aşağıdaki işlevler için bir lambda işlevi ihmal edilemez:

-   [arrayMap](#higher_order_functions-array-map)
-   [arrayFilter](#higher_order_functions-array-filter)
-   [arrayFill](#higher_order_functions-array-fill)
-   [arrayReverseFill](#higher_order_functions-array-reverse-fill)
-   [arraySplit](#higher_order_functions-array-split)
-   [arrayReverseSplit](#higher_order_functions-array-reverse-split)
-   [arrayFirst](#higher_order_functions-array-first)
-   [arrayFirstİndex](#higher_order_functions-array-first-index)

### arrayMap(func, arr1, …) {#higher_order_functions-array-map}

Özgün uygulamadan elde edilen bir dizi döndürür `func` fonksiyon inunda her ele elementmana `arr` dizi.

Örnekler:

``` sql
SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,4,5] │
└─────────┘
```

Aşağıdaki örnek, farklı dizilerden bir öğe kümesinin nasıl oluşturulacağını gösterir:

``` sql
SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res
```

``` text
┌─res─────────────────┐
│ [(1,4),(2,5),(3,6)] │
└─────────────────────┘
```

İlk argümanın (lambda işlevi) atlanamayacağını unutmayın. `arrayMap` İşlev.

### arrayFilter(func, arr1, …) {#higher_order_functions-array-filter}

Yalnızca öğeleri içeren bir dizi döndürür `arr1` hangi için `func` 0'dan başka bir şey döndürür.

Örnekler:

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

İlk argümanın (lambda işlevi) atlanamayacağını unutmayın. `arrayFilter` İşlev.

### arrayFill(func, arr1, …) {#higher_order_functions-array-fill}

Tarama yoluyla `arr1` ilk öğeden son öğeye ve değiştir `arr1[i]` tarafından `arr1[i - 1]` eğer `func` 0 döndürür. İlk eleman `arr1` değiştir notilm .eyecektir.

Örnekler:

``` sql
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res──────────────────────────────┐
│ [1,1,3,11,12,12,12,5,6,14,14,14] │
└──────────────────────────────────┘
```

İlk argümanın (lambda işlevi) atlanamayacağını unutmayın. `arrayFill` İşlev.

### arrayReverseFill(func, arr1, …) {#higher_order_functions-array-reverse-fill}

Tarama yoluyla `arr1` son öğeden ilk öğeye ve değiştir `arr1[i]` tarafından `arr1[i + 1]` eğer `func` 0 döndürür. The La lastst element of `arr1` değiştir notilm .eyecektir.

Örnekler:

``` sql
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res────────────────────────────────┐
│ [1,3,3,11,12,5,5,5,6,14,NULL,NULL] │
└────────────────────────────────────┘
```

İlk argümanın (lambda işlevi) atlanamayacağını unutmayın. `arrayReverseFill` İşlev.

### arraySplit(func, arr1, …) {#higher_order_functions-array-split}

Bölme `arr1` birden fazla diziye. Ne zaman `func` 0'dan başka bir şey döndürür, dizi öğenin sol tarafında bölünecektir. Dizi ilk öğeden önce bölünmez.

Örnekler:

``` sql
SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res─────────────┐
│ [[1,2,3],[4,5]] │
└─────────────────┘
```

İlk argümanın (lambda işlevi) atlanamayacağını unutmayın. `arraySplit` İşlev.

### arrayReverseSplit(func, arr1, …) {#higher_order_functions-array-reverse-split}

Bölme `arr1` birden fazla diziye. Ne zaman `func` 0'dan başka bir şey döndürür, dizi öğenin sağ tarafında bölünecektir. Dizi son öğeden sonra bölünmez.

Örnekler:

``` sql
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res───────────────┐
│ [[1],[2,3,4],[5]] │
└───────────────────┘
```

İlk argümanın (lambda işlevi) atlanamayacağını unutmayın. `arraySplit` İşlev.

### arrayCount(\[func,\] arr1, …) {#higher_order_functions-array-count}

Func 0'dan başka bir şey döndüren arr dizisindeki öğelerin sayısını döndürür. Eğer ‘func’ belirtilmemişse, dizideki sıfır olmayan öğelerin sayısını döndürür.

### arrayExists(\[func,\] arr1, …) {#arrayexistsfunc-arr1}

İçinde en az bir öğe varsa 1 değerini döndürür ‘arr’ hangi için ‘func’ 0'dan başka bir şey döndürür. Aksi takdirde, 0 döndürür.

### arrayAll(\[func,\] arr1, …) {#arrayallfunc-arr1}

Döner 1 Eğer ‘func’ içindeki tüm öğeler için 0'dan başka bir şey döndürür ‘arr’. Aksi takdirde, 0 döndürür.

### arraySum(\[func,\] arr1, …) {#higher-order-functions-array-sum}

Toplamını döndürür ‘func’ değerler. İşlev atlanırsa, sadece dizi öğelerinin toplamını döndürür.

### arrayFirst(func, arr1, …) {#higher_order_functions-array-first}

İlk öğeyi döndürür ‘arr1’ dizi hangi ‘func’ 0'dan başka bir şey döndürür.

İlk argümanın (lambda işlevi) atlanamayacağını unutmayın. `arrayFirst` İşlev.

### arrayFirstIndex(func, arr1, …) {#higher_order_functions-array-first-index}

İlk öğenin dizinini döndürür ‘arr1’ dizi hangi ‘func’ 0'dan başka bir şey döndürür.

İlk argümanın (lambda işlevi) atlanamayacağını unutmayın. `arrayFirstIndex` İşlev.

### arrayCumSum(\[func,\] arr1, …) {#arraycumsumfunc-arr1}

Kaynak dizideki öğelerin kısmi toplamlarının bir dizisini döndürür (çalışan bir toplam). Eğer... `func` işlev belirtilir, daha sonra dizi öğelerinin değerleri toplanmadan önce bu işlev tarafından dönüştürülür.

Örnek:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

``` text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

### arrayCumSumNonNegative(arr) {#arraycumsumnonnegativearr}

Aynı olarak `arrayCumSum`, kaynak dizideki öğelerin kısmi toplamlarının bir dizisini döndürür (çalışan bir toplam). Farklı `arrayCumSum`, daha sonra döndürülen değer sıfırdan küçük bir değer içerdiğinde, değer sıfır ile değiştirilir ve sonraki hesaplama sıfır parametrelerle gerçekleştirilir. Mesela:

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

``` text
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```

### arraySort(\[func,\] arr1, …) {#arraysortfunc-arr1}

Öğeleri sıralama sonucu bir dizi döndürür `arr1` artan düzende. Eğer... `func` fonksiyon belirtilir, sıralama sırası fonksiyonun sonucu ile belirlenir `func` dizi elemanlarına uygulanır (diziler)

Bu [Schwartzian dönüşümü](https://en.wikipedia.org/wiki/Schwartzian_transform) sıralama verimliliğini artırmak için kullanılır.

Örnek:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Hakkında daha fazla bilgi için `arraySort` yöntem, görmek [Dizilerle çalışmak için işlevler](array-functions.md#array_functions-sort) bölme.

### arrayReverseSort(\[func,\] arr1, …) {#arrayreversesortfunc-arr1}

Öğeleri sıralama sonucu bir dizi döndürür `arr1` azalan sırada. Eğer... `func` fonksiyon belirtilir, sıralama sırası fonksiyonun sonucu ile belirlenir `func` dizi (diziler) elemanlarına uygulanır.

Örnek:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

Hakkında daha fazla bilgi için `arrayReverseSort` yöntem, görmek [Dizilerle çalışmak için işlevler](array-functions.md#array_functions-reverse-sort) bölme.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/higher_order_functions/) <!--hide-->
