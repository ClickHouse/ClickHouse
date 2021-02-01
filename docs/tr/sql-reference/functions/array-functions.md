---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "Dizilerle \xE7al\u0131\u015Fma"
---

# Dizilerle çalışmak için işlevler {#functions-for-working-with-arrays}

## boş {#function-empty}

Boş bir dizi için 1 veya boş olmayan bir dizi için 0 döndürür.
Sonuç türü Uint8'dir.
İşlev ayrıca dizeler için de çalışır.

## notEmpty {#function-notempty}

Boş bir dizi için 0 veya boş olmayan bir dizi için 1 döndürür.
Sonuç türü Uint8'dir.
İşlev ayrıca dizeler için de çalışır.

## uzunluk {#array_functions-length}

Dizideki öğe sayısını döndürür.
Sonuç türü Uint64'tür.
İşlev ayrıca dizeler için de çalışır.

## emptyArrayUİnt8, emptyArrayUİnt16, emptyArrayUİnt32, emptyArrayUİnt64 {#emptyarrayuint8-emptyarrayuint16-emptyarrayuint32-emptyarrayuint64}

## emptyArrayİnt8, emptyArrayİnt16, emptyArrayİnt32, emptyArrayİnt64 {#emptyarrayint8-emptyarrayint16-emptyarrayint32-emptyarrayint64}

## emptyArrayFloat32, emptyArrayFloat64 {#emptyarrayfloat32-emptyarrayfloat64}

## emptyArrayDate, emptyArrayDateTime {#emptyarraydate-emptyarraydatetime}

## emptyArrayString {#emptyarraystring}

Sıfır bağımsız değişkeni kabul eder ve uygun türde boş bir dizi döndürür.

## emptyArrayToSingle {#emptyarraytosingle}

Boş bir dizi kabul eder ve varsayılan değere eşit bir tek öğe dizisi döndürür.

## Aralık (bitiş), Aralık(başlangıç, bitiş \[, adım\]) {#rangeend-rangestart-end-step}

1 Adım-başından sonuna kadar sayıların bir dizi döndürür.
Eğer argüman `start` belirtilmemiş, varsayılan olarak 0.
Eğer argüman `step` belirtilmemiş, varsayılan olarak 1.
Neredeyse pythonic gibi davranışlar `range`. Ancak fark, tüm argümanların tipinin olması gerektiğidir `UInt` şiir.
Bir veri bloğunda toplam uzunluğu 100.000.000'den fazla öğe olan diziler oluşturulursa, bir istisna atılır.

## array(x1, …), operator \[x1, …\] {#arrayx1-operator-x1}

İşlev bağımsız değişkenlerinden bir dizi oluşturur.
Bağımsız değişkenler sabit olmalı ve en küçük ortak türe sahip türlere sahip olmalıdır. En az bir argüman geçirilmelidir, çünkü aksi takdirde hangi tür dizinin oluşturulacağı açık değildir. Yani, boş bir dizi oluşturmak için bu işlevi kullanamazsınız (bunu yapmak için, ‘emptyArray\*’ fonksiyon yukarıda açıklandığı.
Ret anur anns an ‘Array(T)’ sonucu yazın, nerede ‘T’ geçirilen bağımsız değişkenlerin en küçük ortak türüdür.

## arrayConcat {#arrayconcat}

Argüman olarak geçirilen dizileri birleştirir.

``` sql
arrayConcat(arrays)
```

**Parametre**

-   `arrays` – Arbitrary number of arguments of [Dizi](../../sql-reference/data-types/array.md) tür.
    **Örnek**

<!-- -->

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

``` text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## arrayElement (arr, n), operatör arr\[n\] {#arrayelementarr-n-operator-arrn}

İnd theex ile eleman alın `n` diz theiden `arr`. `n` herhangi bir tamsayı türü olmalıdır.
Bir dizideki dizinler birinden başlar.
Negatif dizinler desteklenir. Bu durumda, sondan numaralandırılmış ilgili elemanı seçer. Mesela, `arr[-1]` dizideki son öğedir.

Dizin bir dizinin sınırlarının dışına düşerse, bazı varsayılan değer döndürür (sayılar için 0, dizeler için boş bir dize vb.).), sabit olmayan bir dizi ve sabit bir dizin 0 olan durum hariç (bu durumda bir hata olacaktır `Array indices are 1-based`).

## has (arr, elem) {#hasarr-elem}

Olup olmadığını denetler ‘arr’ dizi var ‘elem’ öğe.
Öğe dizide değilse 0 veya varsa 1 değerini döndürür.

`NULL` değer olarak iş islenir.

``` sql
SELECT has([1, 2, NULL], NULL)
```

``` text
┌─has([1, 2, NULL], NULL)─┐
│                       1 │
└─────────────────────────┘
```

## hasAll {#hasall}

Bir dizi başka bir alt kümesi olup olmadığını denetler.

``` sql
hasAll(set, subset)
```

**Parametre**

-   `set` – Array of any type with a set of elements.
-   `subset` – Array of any type with elements that should be tested to be a subset of `set`.

**Dönüş değerleri**

-   `1`, eğer `set` tüm öğeleri içerir `subset`.
-   `0`, başka.

**Tuhaf özellikler**

-   Boş bir dizi, herhangi bir dizinin bir alt kümesidir.
-   `Null` bir değer olarak işlenir.
-   Her iki dizideki değerlerin sırası önemli değil.

**Örnekler**

`SELECT hasAll([], [])` döner 1.

`SELECT hasAll([1, Null], [Null])` döner 1.

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` döner 1.

`SELECT hasAll(['a', 'b'], ['a'])` döner 1.

`SELECT hasAll([1], ['a'])` 0 döndürür.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` 0 döndürür.

## hasAny {#hasany}

İki dizinin bazı öğelerle kesiştiği olup olmadığını kontrol eder.

``` sql
hasAny(array1, array2)
```

**Parametre**

-   `array1` – Array of any type with a set of elements.
-   `array2` – Array of any type with a set of elements.

**Dönüş değerleri**

-   `1`, eğer `array1` ve `array2` en azından benzer bir öğeye sahip olun.
-   `0`, başka.

**Tuhaf özellikler**

-   `Null` bir değer olarak işlenir.
-   Her iki dizideki değerlerin sırası önemli değil.

**Örnekler**

`SELECT hasAny([1], [])` dönüşler `0`.

`SELECT hasAny([Null], [Null, 1])` dönüşler `1`.

`SELECT hasAny([-128, 1., 512], [1])` dönüşler `1`.

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` dönüşler `0`.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` dönüşler `1`.

## ındexof(arr, x) {#indexofarr-x}

İlk dizini döndürür ‘x’ dizide ise öğe (1'den başlayarak) veya değilse 0.

Örnek:

``` sql
SELECT indexOf([1, 3, NULL, NULL], NULL)
```

``` text
┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│                                 3 │
└───────────────────────────────────┘
```

Elem setents set to `NULL` normal değerler olarak ele alınır.

## countEqual(arr, x) {#countequalarr-x}

X eşit dizideki öğelerin sayısını döndürür. Arraycount eşdeğer (elem - \> elem = x, arr).

`NULL` öğeler ayrı değerler olarak işlenir.

Örnek:

``` sql
SELECT countEqual([1, 2, NULL, NULL], NULL)
```

``` text
┌─countEqual([1, 2, NULL, NULL], NULL)─┐
│                                    2 │
└──────────────────────────────────────┘
```

## arrayEnumerate(arr) {#array_functions-arrayenumerate}

Returns the array \[1, 2, 3, …, length (arr) \]

Bu işlev normalde ARRAY JOIN ile kullanılır. ARRAY JOİN uyguladıktan sonra her dizi için sadece bir kez bir şey saymaya izin verir. Örnek:

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

Bu örnekte, Reaches dönüşümlerin sayısıdır (ARRAY JOİN uygulandıktan sonra alınan dizeler) ve İsabetler sayfa görüntüleme sayısıdır (ARRAY JOİN önce dizeler). Bu özel durumda, aynı sonucu daha kolay bir şekilde alabilirsiniz:

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

Bu fonksiyon aynı zamanda yüksek mertebeden fonksiyonlarda da kullanılabilir. Örneğin, bir koşulla eşleşen öğeler için dizi dizinleri almak için kullanabilirsiniz.

## arrayEnumerateUniq(arr, …) {#arrayenumerateuniqarr}

Kaynak diziyle aynı boyutta bir dizi döndürür ve her öğe için aynı değere sahip öğeler arasında konumunun ne olduğunu gösterir.
Örneğin: arrayEnumerateUniq(\[10, 20, 10, 30\]) = \[1, 1, 2, 1\].

Bu işlev, dizi birleştirme ve dizi öğelerinin toplanmasını kullanırken kullanışlıdır.
Örnek:

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

Bu örnekte, her hedef kimliğinin dönüşüm sayısı (hedefler iç içe geçmiş veri yapısındaki her öğe, bir dönüşüm olarak adlandırdığımız ulaşılan bir hedeftir) ve oturum sayısı Hesaplaması vardır. ARRAY JOİN olmadan, oturum sayısını sum(Sign) olarak sayardık. Ancak bu özel durumda, satırlar iç içe geçmiş hedefler yapısıyla çarpıldı, bu nedenle her oturumu bir kez saymak için arrayenumerateuniq değerine bir koşul uyguluyoruz(Goals.ID) fonksiyonu.

Arrayenumerateuniq işlevi, bağımsız değişkenlerle aynı boyutta birden çok dizi alabilir. Bu durumda, tüm dizilerde aynı konumlardaki elemanların tuplesleri için benzersizlik düşünülür.

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

``` text
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

Bu, iç içe geçmiş bir veri yapısı ve bu yapıdaki birden çok öğe arasında daha fazla toplama ile dizi birleşimini kullanırken gereklidir.

## arrayPopBack {#arraypopback}

Son öğeyi diziden kaldırır.

``` sql
arrayPopBack(array)
```

**Parametre**

-   `array` – Array.

**Örnek**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## arrayPopFront {#arraypopfront}

İlk öğeyi diziden kaldırır.

``` sql
arrayPopFront(array)
```

**Parametre**

-   `array` – Array.

**Örnek**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack {#arraypushback}

Dizinin sonuna bir öğe ekler.

``` sql
arrayPushBack(array, single_value)
```

**Parametre**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` dizinin veri türü için yazın. Clickhouse'daki veri türleri hakkında daha fazla bilgi için bkz. “[Veri türleri](../../sql-reference/data-types/index.md#data_types)”. Olabilir `NULL`. Fonksiyon bir ekler `NULL` bir dizi için öğe ve dizi öğeleri türü dönüştürür `Nullable`.

**Örnek**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayPushFront {#arraypushfront}

Dizinin başına bir öğe ekler.

``` sql
arrayPushFront(array, single_value)
```

**Parametre**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` dizinin veri türü için yazın. Clickhouse'daki veri türleri hakkında daha fazla bilgi için bkz. “[Veri türleri](../../sql-reference/data-types/index.md#data_types)”. Olabilir `NULL`. Fonksiyon bir ekler `NULL` bir dizi için öğe ve dizi öğeleri türü dönüştürür `Nullable`.

**Örnek**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayResize {#arrayresize}

Dizinin uzunluğunu değiştirir.

``` sql
arrayResize(array, size[, extender])
```

**Parametre:**

-   `array` — Array.
-   `size` — Required length of the array.
    -   Eğer `size` dizinin orijinal boyutundan daha az, dizi sağdan kesilir.
-   Eğer `size` dizinin başlangıç boyutundan daha büyük, dizi sağa uzatılır `extender` dizi öğelerinin veri türü için değerler veya varsayılan değerler.
-   `extender` — Value for extending an array. Can be `NULL`.

**Döndürülen değer:**

Bir dizi uzunluk `size`.

**Arama örnekleri**

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

Dizinin bir dilimini döndürür.

``` sql
arraySlice(array, offset[, length])
```

**Parametre**

-   `array` – Array of data.
-   `offset` – Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with 1.
-   `length` - Gerekli dilimin uzunluğu. Negatif bir değer belirtirseniz, işlev açık bir dilim döndürür `[offset, array_length - length)`. Değeri atlarsanız, işlev dilimi döndürür `[offset, the_end_of_array]`.

**Örnek**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res
```

``` text
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

Ar arrayray elem toents set to `NULL` normal değerler olarak ele alınır.

## arraySort(\[func,\] arr, …) {#array_functions-sort}

Elemanları sıralar `arr` artan düzende dizi. Eğer... `func` fonksiyonu belirtilir, sıralama düzeni sonucu belirlenir `func` fonksiyon dizinin elemanlarına uygulanır. Eğer `func` birden fazla argüman kabul eder, `arraySort` fonksiyon argümanları birkaç diziler geçirilir `func` karşılık gelir. Ayrıntılı örnekler sonunda gösterilir `arraySort` açıklama.

Tamsayı değerleri sıralama örneği:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```

``` text
┌─arraySort([1, 3, 3, 0])─┐
│ [0,1,3,3]               │
└─────────────────────────┘
```

Dize değerleri sıralama örneği:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```

``` text
┌─arraySort(['hello', 'world', '!'])─┐
│ ['!','hello','world']              │
└────────────────────────────────────┘
```

Aşağıdaki sıralama sırasını göz önünde bulundurun `NULL`, `NaN` ve `Inf` değerler:

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

``` text
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
│ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
└───────────────────────────────────────────────────────────┘
```

-   `-Inf` değerler dizide ilk sırada yer alır.
-   `NULL` değerler dizideki son değerlerdir.
-   `NaN` değerler hemen önce `NULL`.
-   `Inf` değerler hemen önce `NaN`.

Not thate that `arraySort` is a [yüksek sipariş fonksiyonu](higher-order-functions.md). Bir lambda işlevini ilk argüman olarak iletebilirsiniz. Bu durumda, sıralama sırası, dizinin elemanlarına uygulanan lambda işlevinin sonucu ile belirlenir.

Aşağıdaki örneği ele alalım:

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

For each element of the source array, the lambda function returns the sorting key, that is, \[1 –\> -1, 2 –\> -2, 3 –\> -3\]. Since the `arraySort` fonksiyon tuşları artan sırayla sıralar, sonuç \[3, 2, 1\]. Böylece, `(x) –> -x` lambda fonksiyonu setleri [azalan düzen](#array_functions-reverse-sort) bir sıralama içinde.

Lambda işlevi birden çok bağımsız değişken kabul edebilir. Bu durumda, geçmek gerekir `arraySort` işlev lambda işlevinin argümanlarının karşılık geleceği aynı uzunlukta birkaç dizi. Elde edilen dizi ilk giriş dizisinden elemanlardan oluşacaktır; bir sonraki giriş dizilerinden elemanlar sıralama anahtarlarını belirtir. Mesela:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Burada, ikinci dizide (\[2, 1\]) geçirilen öğeler, kaynak diziden karşılık gelen öğe için bir sıralama anahtarı tanımlar (\[‘hello’, ‘world’\]), bu, \[‘hello’ –\> 2, ‘world’ –\> 1\]. Since the lambda function doesn't use `x`, kaynak dizinin gerçek değerleri sonuçtaki sırayı etkilemez. Böyle, ‘hello’ sonuçtaki ikinci eleman olacak ve ‘world’ ilk olacak.

Diğer örnekler aşağıda gösterilmiştir.

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

!!! note "Not"
    Sıralama verimliliğini artırmak için, [Schwartzian dönüşümü](https://en.wikipedia.org/wiki/Schwartzian_transform) kullanılır.

## arrayReverseSort(\[func,\] arr, …) {#array_functions-reverse-sort}

Elemanları sıralar `arr` azalan sırayla dizi. Eğer... `func` fonksiyon belirtilir, `arr` sonucuna göre sıra islanır. `func` işlev dizinin öğelerine uygulanır ve sonra sıralanmış dizi tersine çevrilir. Eğer `func` birden fazla argüman kabul eder, `arrayReverseSort` fonksiyon argümanları birkaç diziler geçirilir `func` karşılık gelir. Ayrıntılı örnekler sonunda gösterilir `arrayReverseSort` açıklama.

Tamsayı değerleri sıralama örneği:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

``` text
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3,3,1,0]                      │
└────────────────────────────────┘
```

Dize değerleri sıralama örneği:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

``` text
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ ['world','hello','!']                     │
└───────────────────────────────────────────┘
```

Aşağıdaki sıralama sırasını göz önünde bulundurun `NULL`, `NaN` ve `Inf` değerler:

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

``` text
┌─res───────────────────────────────────┐
│ [inf,3,2,1,-4,-inf,nan,nan,NULL,NULL] │
└───────────────────────────────────────┘
```

-   `Inf` değerler dizide ilk sırada yer alır.
-   `NULL` değerler dizideki son değerlerdir.
-   `NaN` değerler hemen önce `NULL`.
-   `-Inf` değerler hemen önce `NaN`.

Not `arrayReverseSort` is a [yüksek sipariş fonksiyonu](higher-order-functions.md). Bir lambda işlevini ilk argüman olarak iletebilirsiniz. Örnek aşağıda gösterilmiştir.

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

Dizi aşağıdaki şekilde sıralanır:

1.  İlk başta, kaynak dizi (\[1, 2, 3\]), dizinin elemanlarına uygulanan lambda işlevinin sonucuna göre sıralanır. Sonuç bir dizidir \[3, 2, 1\].
2.  Önceki adımda elde edilen dizi tersine çevrilir. Yani, nihai sonuç \[1, 2, 3\].

Lambda işlevi birden çok bağımsız değişken kabul edebilir. Bu durumda, geçmek gerekir `arrayReverseSort` işlev lambda işlevinin argümanlarının karşılık geleceği aynı uzunlukta birkaç dizi. Elde edilen dizi ilk giriş dizisinden elemanlardan oluşacaktır; bir sonraki giriş dizilerinden elemanlar sıralama anahtarlarını belirtir. Mesela:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

Bu örnekte, dizi aşağıdaki şekilde sıralanır:

1.  İlk başta, kaynak dizi (\[‘hello’, ‘world’\]) dizilerin elemanlarına uygulanan lambda işlevinin sonucuna göre sıralanır. İkinci dizide geçirilen öğeler (\[2, 1\]), kaynak diziden karşılık gelen öğeler için sıralama anahtarlarını tanımlar. Sonuç bir dizidir \[‘world’, ‘hello’\].
2.  Önceki adımda sıralanmış dizi tersine çevrilir. Yani, nihai sonuç \[‘hello’, ‘world’\].

Diğer örnekler aşağıda gösterilmiştir.

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

Bir bağımsız değişken geçirilirse, dizideki farklı öğelerin sayısını sayar.
Birden çok bağımsız değişken geçirilirse, birden çok dizideki karşılık gelen konumlardaki farklı öğe kümelerinin sayısını sayar.

Bir dizideki benzersiz öğelerin bir listesini almak istiyorsanız, arrayreduce kullanabilirsiniz(‘groupUniqArray’, arr).

## arrayJoin(arr) {#array-functions-join}

Özel bir işlev. Bölümüne bakınız [“ArrayJoin function”](array-join.md#functions_arrayjoin).

## arrayDifference {#arraydifference}

Bitişik dizi öğeleri arasındaki farkı hesaplar. İlk öğenin 0 olacağı bir dizi döndürür, ikincisi arasındaki farktır `a[1] - a[0]`, etc. The type of elements in the resulting array is determined by the type inference rules for subtraction (e.g. `UInt8` - `UInt8` = `Int16`).

**Sözdizimi**

``` sql
arrayDifference(array)
```

**Parametre**

-   `array` – [Dizi](https://clickhouse.tech/docs/en/data_types/array/).

**Döndürülen değerler**

Bitişik öğeler arasındaki farklar dizisini döndürür.

Tür: [Uİnt\*](https://clickhouse.tech/docs/en/data_types/int_uint/#uint-ranges), [Tamsayı\*](https://clickhouse.tech/docs/en/data_types/int_uint/#int-ranges), [Yüzdürmek\*](https://clickhouse.tech/docs/en/data_types/float/).

**Örnek**

Sorgu:

``` sql
SELECT arrayDifference([1, 2, 3, 4])
```

Sonuç:

``` text
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0,1,1,1]                     │
└───────────────────────────────┘
```

Sonuç türü Int64 nedeniyle taşma örneği:

Sorgu:

``` sql
SELECT arrayDifference([0, 10000000000000000000])
```

Sonuç:

``` text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0,-8446744073709551616]                   │
└────────────────────────────────────────────┘
```

## arrayDistinct {#arraydistinct}

Bir dizi alır, yalnızca farklı öğeleri içeren bir dizi döndürür.

**Sözdizimi**

``` sql
arrayDistinct(array)
```

**Parametre**

-   `array` – [Dizi](https://clickhouse.tech/docs/en/data_types/array/).

**Döndürülen değerler**

Farklı öğeleri içeren bir dizi döndürür.

**Örnek**

Sorgu:

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1])
```

Sonuç:

``` text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## arrayEnumerateDense(arr) {#array_functions-arrayenumeratedense}

Kaynak diziyle aynı boyutta bir dizi döndürür ve her öğenin kaynak dizide ilk olarak nerede göründüğünü gösterir.

Örnek:

``` sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

``` text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1,2,1,3]                             │
└───────────────────────────────────────┘
```

## arrayıntersect(arr) {#array-functions-arrayintersect}

Birden çok dizi alır, tüm kaynak dizilerde bulunan öğeleri içeren bir dizi döndürür. Elde edilen dizideki öğeler sırası ilk dizideki ile aynıdır.

Örnek:

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

Dizi öğelerine bir toplama işlevi uygular ve sonucunu döndürür. Toplama işlevinin adı, tek tırnak içinde bir dize olarak geçirilir `'max'`, `'sum'`. Parametrik toplama işlevleri kullanıldığında, parametre parantez içinde işlev adından sonra gösterilir `'uniqUpTo(6)'`.

**Sözdizimi**

``` sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**Parametre**

-   `agg_func` — The name of an aggregate function which should be a constant [dize](../../sql-reference/data-types/string.md).
-   `arr` — Any number of [dizi](../../sql-reference/data-types/array.md) sütunları toplama işlevinin parametreleri olarak yazın.

**Döndürülen değer**

**Örnek**

``` sql
SELECT arrayReduce('max', [1, 2, 3])
```

``` text
┌─arrayReduce('max', [1, 2, 3])─┐
│                             3 │
└───────────────────────────────┘
```

Bir toplama işlevi birden çok bağımsız değişken alırsa, bu işlev aynı boyuttaki birden çok diziye uygulanmalıdır.

``` sql
SELECT arrayReduce('maxIf', [3, 5], [1, 0])
```

``` text
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐
│                                    3 │
└──────────────────────────────────────┘
```

Parametrik toplama fonksiyonu ile örnek:

``` sql
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
```

``` text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
```

## arrayReduceİnRanges {#arrayreduceinranges}

Belirli aralıklardaki dizi öğelerine bir toplama işlevi uygular ve her aralığa karşılık gelen sonucu içeren bir dizi döndürür. Fonksiyon aynı sonucu birden fazla olarak döndürür `arrayReduce(agg_func, arraySlice(arr1, index, length), ...)`.

**Sözdizimi**

``` sql
arrayReduceInRanges(agg_func, ranges, arr1, arr2, ..., arrN)
```

**Parametre**

-   `agg_func` — The name of an aggregate function which should be a constant [dize](../../sql-reference/data-types/string.md).
-   `ranges` — The ranges to aggretate which should be an [dizi](../../sql-reference/data-types/array.md) -den [Demetler](../../sql-reference/data-types/tuple.md) indeks ve her aralığın uzunluğunu içeren.
-   `arr` — Any number of [dizi](../../sql-reference/data-types/array.md) sütunları toplama işlevinin parametreleri olarak yazın.

**Döndürülen değer**

**Örnek**

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

Öğeleri ters sırada içeren orijinal diziyle aynı boyutta bir dizi döndürür.

Örnek:

``` sql
SELECT arrayReverse([1, 2, 3])
```

``` text
┌─arrayReverse([1, 2, 3])─┐
│ [3,2,1]                 │
└─────────────────────────┘
```

## ters (arr) {#array-functions-reverse}

Eşanlamlı [“arrayReverse”](#arrayreverse)

## arrayFlatten {#arrayflatten}

Bir dizi diziyi düz bir diziye dönüştürür.

İşlev:

-   İç içe geçmiş dizilerin herhangi bir derinliği için geçerlidir.
-   Zaten düz olan dizileri değiştirmez.

Düzleştirilmiş dizi, tüm kaynak dizilerdeki tüm öğeleri içerir.

**Sözdizimi**

``` sql
flatten(array_of_arrays)
```

Takma ad: `flatten`.

**Parametre**

-   `array_of_arrays` — [Dizi](../../sql-reference/data-types/array.md) dizilerin. Mesela, `[[1,2,3], [4,5]]`.

**Örnekler**

``` sql
SELECT flatten([[[1]], [[2], [3]]])
```

``` text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1,2,3]                                     │
└─────────────────────────────────────────────┘
```

## arrayCompact {#arraycompact}

Ardışık yinelenen öğeleri bir diziden kaldırır. Sonuç değerlerinin sırası, kaynak dizideki sıraya göre belirlenir.

**Sözdizimi**

``` sql
arrayCompact(arr)
```

**Parametre**

`arr` — The [dizi](../../sql-reference/data-types/array.md) incelemek.

**Döndürülen değer**

Yinelenen olmadan dizi.

Tür: `Array`.

**Örnek**

Sorgu:

``` sql
SELECT arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])
```

Sonuç:

``` text
┌─arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])─┐
│ [1,nan,nan,2,3]                            │
└────────────────────────────────────────────┘
```

## arrayZip {#arrayzip}

Birden çok diziyi tek bir dizide birleştirir. Elde edilen dizi, listelenen bağımsız değişken sırasına göre gruplandırılmış kaynak dizilerin karşılık gelen öğelerini içerir.

**Sözdizimi**

``` sql
arrayZip(arr1, arr2, ..., arrN)
```

**Parametre**

-   `arrN` — [Dizi](../data-types/array.md).

İşlev, farklı türde herhangi bir dizi alabilir. Tüm giriş dizileri eşit boyutta olmalıdır.

**Döndürülen değer**

-   Gruplandırılmış kaynak dizilerden öğelerle dizi [Demetler](../data-types/tuple.md). Veri türleri tuple giriş dizileri türleri ile aynıdır ve diziler geçirilir aynı sırada.

Tür: [Dizi](../data-types/array.md).

**Örnek**

Sorgu:

``` sql
SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1])
```

Sonuç:

``` text
┌─arrayZip(['a', 'b', 'c'], [5, 2, 1])─┐
│ [('a',5),('b',2),('c',1)]            │
└──────────────────────────────────────┘
```

## arrayAUC {#arrayauc}

Auc'yi hesaplayın (makine öğreniminde bir kavram olan eğrinin altındaki alan, daha fazla ayrıntıya bakın: https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve).

**Sözdizimi**

``` sql
arrayAUC(arr_scores, arr_labels)
```

**Parametre**
- `arr_scores` — scores prediction model gives.
- `arr_labels` — labels of samples, usually 1 for positive sample and 0 for negtive sample.

**Döndürülen değer**
Float64 türü ile AUC değerini döndürür.

**Örnek**
Sorgu:

``` sql
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
```

Sonuç:

``` text
┌─arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])─┐
│                                          0.75 │
└────────────────────────────────────────---──┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/array_functions/) <!--hide-->
