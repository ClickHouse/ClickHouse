---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: "Ba\u015Fvurma"
---

# Toplama Fonksiyonu Referansı {#aggregate-functions-reference}

## sayma {#agg_function-count}

Satır veya NOT-NULL değerleri sayar.

ClickHouse için aşağıdaki sözdizimleri destekler `count`:
- `count(expr)` veya `COUNT(DISTINCT expr)`.
- `count()` veya `COUNT(*)`. Bu `count()` sözdizimi ClickHouse özeldir.

**Parametre**

Fonksiyon alabilir:

-   Sıfır parametreler.
-   Bir [ifade](../syntax.md#syntax-expressions).

**Döndürülen değer**

-   Fonksiyon parametreleri olmadan çağrılırsa, satır sayısını sayar.
-   Eğer... [ifade](../syntax.md#syntax-expressions) geçirilir, daha sonra işlev bu ifadenin kaç kez NOT null döndürdüğünü sayar. İfad aede bir [Nullable](../../sql-reference/data-types/nullable.md)- type değeri, sonra sonucu `count` kalır değil `Nullable`. İfade döndürülürse işlev 0 döndürür `NULL` tüm satırlar için.

Her iki durumda da döndürülen değerin türü [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Ayrıntı**

ClickHouse destekler `COUNT(DISTINCT ...)` sözdizimi. Bu yapının davranışı Aşağıdakilere bağlıdır [count_distinct_implementation](../../operations/settings/settings.md#settings-count_distinct_implementation) ayar. Aşağıdakilerden hang theisini tanımlar [uniq\*](#agg_function-uniq) fonksiyonlar işlemi gerçekleştirmek için kullanılır. Varsayılan değer [uniqExact](#agg_function-uniqexact) İşlev.

Bu `SELECT count() FROM table` tablodaki girdi sayısı ayrı olarak depolanmadığı için sorgu en iyi duruma getirilmez. Tablodan küçük bir sütun seçer ve içindeki değerlerin sayısını sayar.

**Örnekler**

Örnek 1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

Örnek 2:

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

Bu örnek gösteriyor ki `count(DISTINCT num)` tarafından gerçekleştirilir `uniqExact` fonksiyonu göre `count_distinct_implementation` ayar değeri.

## herhangi(x) {#agg_function-any}

İlk karşılaşılan değeri seçer.
Sorgu herhangi bir sırada ve hatta her seferinde farklı bir sırada çalıştırılabilir, bu nedenle bu işlevin sonucu belirsizdir.
Belirli bir sonuç elde etmek için ‘min’ veya ‘max’ fonksiyon yerine ‘any’.

Bazı durumlarda, yürütme sırasına güvenebilirsiniz. Bu, select ORDER BY kullanan bir alt sorgudan geldiğinde durumlar için geçerlidir.

Ne zaman bir `SELECT` sorgu vardır `GROUP BY` yan tümce veya en az bir toplama işlevi, ClickHouse (Mysql'in aksine), tüm ifadelerin `SELECT`, `HAVING`, ve `ORDER BY` anahtar functionslardan veya toplama işlev .lerinden hesaplan .malıdır. Başka bir deyişle, tablodan seçilen her sütun, anahtarlarda veya toplama işlevlerinde kullanılmalıdır. Mysql'de olduğu gibi davranış elde etmek için, diğer sütunları `any` toplama işlevi.

## anyHeavy (x) {#anyheavyx}

Kullanarak sık oluşan bir değer seçer [ağır vurucular](http://www.cs.umd.edu/~samir/498/karp.pdf) algoritma. Sorgunun yürütme iş parçacığı her durumda yarısından fazlasını oluşan bir değer varsa, bu değer döndürülür. Normalde, sonuç belirsizdir.

``` sql
anyHeavy(column)
```

**Değişkenler**

-   `column` – The column name.

**Örnek**

Tak thee the [OnTime](../../getting-started/example-datasets/ontime.md) veri kümesi ve herhangi bir sık oluşan değeri seçin `AirlineID` sütun.

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```

## anyLast(x) {#anylastx}

Karşılaşılan son değeri seçer.
Sonuç için olduğu kadar belirsiz `any` İşlev.

## groupBitAnd {#groupbitand}

Bitwise uygular `AND` sayı serisi için.

``` sql
groupBitAnd(expr)
```

**Parametre**

`expr` – An expression that results in `UInt*` tür.

**Dönüş değeri**

Bu değer `UInt*` tür.

**Örnek**

Test verileri:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Sorgu:

``` sql
SELECT groupBitAnd(num) FROM t
```

Nerede `num` test verileri ile sütundur.

Sonuç:

``` text
binary     decimal
00000100 = 4
```

## groupBitOr {#groupbitor}

Bitwise uygular `OR` sayı serisi için.

``` sql
groupBitOr(expr)
```

**Parametre**

`expr` – An expression that results in `UInt*` tür.

**Dönüş değeri**

Bu değer `UInt*` tür.

**Örnek**

Test verileri:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Sorgu:

``` sql
SELECT groupBitOr(num) FROM t
```

Nerede `num` test verileri ile sütundur.

Sonuç:

``` text
binary     decimal
01111101 = 125
```

## groupBitXor {#groupbitxor}

Bitwise uygular `XOR` sayı serisi için.

``` sql
groupBitXor(expr)
```

**Parametre**

`expr` – An expression that results in `UInt*` tür.

**Dönüş değeri**

Bu değer `UInt*` tür.

**Örnek**

Test verileri:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Sorgu:

``` sql
SELECT groupBitXor(num) FROM t
```

Nerede `num` test verileri ile sütundur.

Sonuç:

``` text
binary     decimal
01101000 = 104
```

## groupBitmap {#groupbitmap}

İşaretsiz tamsayı sütun, Uınt64 tür iade önem, gelen bit eşlem veya Toplama hesaplamaları suffix ekleme -Devlet, sonra iade [bitmap nesnesi](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**Parametre**

`expr` – An expression that results in `UInt*` tür.

**Dönüş değeri**

Bu değer `UInt64` tür.

**Örnek**

Test verileri:

``` text
UserID
1
1
2
3
```

Sorgu:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

Sonuç:

``` text
num
3
```

## min (x) {#agg_function-min}

Minimum hesaplar.

## max (x) {#agg_function-max}

Maksimum hesaplar.

## argMin (arg, val) {#agg-function-argmin}

Hesaplar ‘arg’ minimum değer ‘val’ değer. Birkaç farklı değer varsa ‘arg’ minimum değerler için ‘val’, karşılaşılan bu değerlerin ilki çıktıdır.

**Örnek:**

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

## argMax (arg, val) {#agg-function-argmax}

Hesaplar ‘arg’ maksimum değer ‘val’ değer. Birkaç farklı değer varsa ‘arg’ maksimum değerler için ‘val’, karşılaşılan bu değerlerin ilki çıktıdır.

## s (um (x) {#agg_function-sum}

Toplamı hesaplar.
Sadece sayılar için çalışır.

## sumWithOverflow(x) {#sumwithoverflowx}

Giriş parametreleri için olduğu gibi sonuç için aynı veri türünü kullanarak sayıların toplamını hesaplar. Toplam bu veri türü için en büyük değeri aşarsa, işlev bir hata döndürür.

Sadece sayılar için çalışır.

## sumMap (anahtar, değer), sumMap(Tuple (anahtar, değer)) {#agg_functions-summap}

Toplam thelar ‘value’ belirtilen tuş accordinglara göre dizi ‘key’ dizi.
Anahtarları ve değerleri diziler dizi geçen anahtarları ve değerleri iki dizi geçen synonymical var.
Eleman sayısı ‘key’ ve ‘value’ toplam her satır için aynı olmalıdır.
Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

Örnek:

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

Hesaplar [çarpıklık](https://en.wikipedia.org/wiki/Skewness) bir sıra.

``` sql
skewPop(expr)
```

**Parametre**

`expr` — [İfade](../syntax.md#syntax-expressions) bir numara döndürüyor.

**Döndürülen değer**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**Örnek**

``` sql
SELECT skewPop(value) FROM series_with_value_column
```

## skewSamp {#skewsamp}

Hesaplar [örnek çarpıklık](https://en.wikipedia.org/wiki/Skewness) bir sıra.

Bir rassal değişkenin çarpıklığının tarafsız bir tahminini temsil eder.

``` sql
skewSamp(expr)
```

**Parametre**

`expr` — [İfade](../syntax.md#syntax-expressions) bir numara döndürüyor.

**Döndürülen değer**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). Eğer `n <= 1` (`n` örnek boyutudur), daha sonra işlev döner `nan`.

**Örnek**

``` sql
SELECT skewSamp(value) FROM series_with_value_column
```

## kurtPop {#kurtpop}

Hesaplar [kurtosis](https://en.wikipedia.org/wiki/Kurtosis) bir sıra.

``` sql
kurtPop(expr)
```

**Parametre**

`expr` — [İfade](../syntax.md#syntax-expressions) bir numara döndürüyor.

**Döndürülen değer**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**Örnek**

``` sql
SELECT kurtPop(value) FROM series_with_value_column
```

## kurtSamp {#kurtsamp}

Hesaplar [örnek kurtoz](https://en.wikipedia.org/wiki/Kurtosis) bir sıra.

Eğer geçen değerleri örnek oluşturur, eğer bir rassal değişken kurtosis tarafsız bir tahmini temsil eder.

``` sql
kurtSamp(expr)
```

**Parametre**

`expr` — [İfade](../syntax.md#syntax-expressions) bir numara döndürüyor.

**Döndürülen değer**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). Eğer `n <= 1` (`n` örnek bir boyutudur), daha sonra işlev döner `nan`.

**Örnek**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column
```

## avg (x) {#agg_function-avg}

Ortalama hesaplar.
Sadece sayılar için çalışır.
Sonuç Her zaman Float64.

## avgWeighted {#avgweighted}

Hesaplar [ağırlıklı aritmetik ortalama](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean).

**Sözdizimi**

``` sql
avgWeighted(x, weight)
```

**Parametre**

-   `x` — Values. [Tamsayı](../data-types/int-uint.md) veya [kayan nokta](../data-types/float.md).
-   `weight` — Weights of the values. [Tamsayı](../data-types/int-uint.md) veya [kayan nokta](../data-types/float.md).

Türü `x` ve `weight` aynı olmalıdır.

**Döndürülen değer**

-   Ağırlıklı ortalama.
-   `NaN`. Tüm ağırlıklar 0'a eşitse.

Tür: [Float64](../data-types/float.md).

**Örnek**

Sorgu:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

Sonuç:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

## uniq {#agg_function-uniq}

Bağımsız değişken farklı değerlerin yaklaşık sayısını hesaplar.

``` sql
uniq(x[, ...])
```

**Parametre**

Fonksiyon değişken sayıda parametre alır. Parametreler olabilir `Tuple`, `Array`, `Date`, `DateTime`, `String` veya sayısal türleri.

**Döndürülen değer**

-   A [Uİnt64](../../sql-reference/data-types/int-uint.md)- tip numarası.

**Uygulama Detayları**

İşlev:

-   Toplamdaki tüm parametreler için bir karma hesaplar, daha sonra hesaplamalarda kullanır.

-   Bir adaptif örnekleme algoritması kullanır. Hesaplama durumu için işlev, 65536'ya kadar öğe karma değerlerinin bir örneğini kullanır.

        This algorithm is very accurate and very efficient on the CPU. When the query contains several of these functions, using `uniq` is almost as fast as using other aggregate functions.

-   Sonucu deterministically sağlar (sorgu işleme sırasına bağlı değildir).

Bu işlevi hemen hemen tüm senaryolarda kullanmanızı öneririz.

**Ayrıca Bakınız**

-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined {#agg_function-uniqcombined}

Farklı bağımsız değişken değerlerinin yaklaşık sayısını hesaplar.

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

Bu `uniqCombined` fonksiyon, farklı değerlerin sayısını hesaplamak için iyi bir seçimdir.

**Parametre**

Fonksiyon değişken sayıda parametre alır. Parametreler olabilir `Tuple`, `Array`, `Date`, `DateTime`, `String` veya sayısal türleri.

`HLL_precision` hücre sayısının baz-2 logaritmasıdır. [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). İsteğe bağlı olarak işlevi kullanabilirsiniz `uniqCombined(x[, ...])`. İçin varsayılan değer `HLL_precision` etkin bir şekilde 96 KiB alan olan 17'dir (2^17 hücre, her biri 6 bit).

**Döndürülen değer**

-   Numara [Uİnt64](../../sql-reference/data-types/int-uint.md)- tip numarası.

**Uygulama Detayları**

İşlev:

-   Bir karma hesaplar (64-bit karma için `String` ve 32-bit aksi halde) agregadaki tüm parametreler için, hesaplamalarda kullanır.

-   Bir hata düzeltme tablosu ile dizi, karma tablo ve HyperLogLog: üç algoritmaları bir arada kullanır.

        For a small number of distinct elements, an array is used. When the set size is larger, a hash table is used. For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.

-   Sonucu deterministically sağlar (sorgu işleme sırasına bağlı değildir).

!!! note "Not"
    Olmayan için 32-bit karma kullandığından-`String` tipi, sonuç cardinalities önemli ölçüde daha büyük için çok yüksek hata olacak `UINT_MAX` (birkaç on milyarlarca farklı değerden sonra hata hızla artacaktır), bu durumda kullanmanız gerekir [uniqCombined64](#agg_function-uniqcombined64)

İle karşılaştırıldığında [uniq](#agg_function-uniq) fonksiyonu, `uniqCombined`:

-   Birkaç kez daha az bellek tüketir.
-   Birkaç kat daha yüksek doğrulukla hesaplar.
-   Genellikle biraz daha düşük performansa sahiptir. Bazı senaryolarda, `uniqCombined` daha iyi performans gösterebilir `uniq` örneğin, ağ üzerinden çok sayıda toplama durumu ileten dağıtılmış sorgularla.

**Ayrıca Bakınız**

-   [uniq](#agg_function-uniq)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined64 {#agg_function-uniqcombined64}

Aynı olarak [uniqCombined](#agg_function-uniqcombined), ancak tüm veri türleri için 64 bit karma kullanır.

## uniqHLL12 {#agg_function-uniqhll12}

Farklı argüman değerlerinin yaklaşık sayısını hesaplar [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algoritma.

``` sql
uniqHLL12(x[, ...])
```

**Parametre**

Fonksiyon değişken sayıda parametre alır. Parametreler olabilir `Tuple`, `Array`, `Date`, `DateTime`, `String` veya sayısal türleri.

**Döndürülen değer**

-   A [Uİnt64](../../sql-reference/data-types/int-uint.md)- tip numarası.

**Uygulama Detayları**

İşlev:

-   Toplamdaki tüm parametreler için bir karma hesaplar, daha sonra hesaplamalarda kullanır.

-   Farklı bağımsız değişken değerlerinin sayısını yaklaştırmak için HyperLogLog algoritmasını kullanır.

        212 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

-   Belirli sonucu sağlar (sorgu işleme sırasına bağlı değildir).

Bu işlevi kullanmanızı önermiyoruz. Çoğu durumda, kullan [uniq](#agg_function-uniq) veya [uniqCombined](#agg_function-uniqcombined) İşlev.

**Ayrıca Bakınız**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqExact](#agg_function-uniqexact)

## uniqExact {#agg_function-uniqexact}

Farklı bağımsız değişken değerlerinin tam sayısını hesaplar.

``` sql
uniqExact(x[, ...])
```

Kullan... `uniqExact` kesinlikle kesin bir sonuca ihtiyacınız varsa işlev. Aksi takdirde kullanın [uniq](#agg_function-uniq) İşlev.

Bu `uniqExact` fonksiyonu daha fazla bellek kullanır `uniq`, çünkü farklı değerlerin sayısı arttıkça devletin büyüklüğü sınırsız büyümeye sahiptir.

**Parametre**

Fonksiyon değişken sayıda parametre alır. Parametreler olabilir `Tuple`, `Array`, `Date`, `DateTime`, `String` veya sayısal türleri.

**Ayrıca Bakınız**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqHLL12](#agg_function-uniqhll12)

## groupArray (x), groupArray (max_size)(x) {#agg_function-grouparray}

Bağımsız değişken değerleri dizisi oluşturur.
Değerler diziye herhangi bir (belirsiz) sırayla eklenebilir.

İkinci versiyonu (ile `max_size` parametre), elde edilen dizinin boyutunu sınırlar `max_size` öğeler.
Mesela, `groupArray (1) (x)` eşdeğ toer equivalentdir `[any (x)]`.

Bazı durumlarda, hala yürütme sırasına güvenebilirsiniz. Bu, aşağıdaki durumlar için geçerlidir `SELECT` kullanan bir alt sorgudan gelir `ORDER BY`.

## grouparrayınsertat {#grouparrayinsertat}

Belirtilen konumda diziye bir değer ekler.

**Sözdizimi**

``` sql
groupArrayInsertAt(default_x, size)(x, pos);
```

Bir sorguda aynı konuma birkaç değer eklenirse, işlev aşağıdaki şekillerde davranır:

-   Bir sorgu tek bir iş parçacığında yürütülürse, eklenen değerlerden ilki kullanılır.
-   Bir sorgu birden çok iş parçacığında yürütülürse, ortaya çıkan değer, eklenen değerlerden belirsiz bir değerdir.

**Parametre**

-   `x` — Value to be inserted. [İfade](../syntax.md#syntax-expressions) biri sonuçta [desteklenen veri türleri](../../sql-reference/data-types/index.md).
-   `pos` — Position at which the specified element `x` eklen .ecektir. Dizideki dizin numaralandırma sıfırdan başlar. [Uİnt32](../../sql-reference/data-types/int-uint.md#uint-ranges).
-   `default_x`— Default value for substituting in empty positions. Optional parameter. [İfade](../syntax.md#syntax-expressions) için yapılandırılmış veri türü ile sonuçlanan `x` parametre. Eğer `default_x` tanımlan ,mamıştır, [varsayılan değerler](../../sql-reference/statements/create.md#create-default-values) kullanılır.
-   `size`— Length of the resulting array. Optional parameter. When using this parameter, the default value `default_x` belirt .ilmelidir. [Uİnt32](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Döndürülen değer**

-   Eklenen değerlerle dizi.

Tür: [Dizi](../../sql-reference/data-types/array.md#data-type-array).

**Örnek**

Sorgu:

``` sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

Sonuç:

``` text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐
│ ['0','','1','','2','','3','','4']                         │
└───────────────────────────────────────────────────────────┘
```

Sorgu:

``` sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

Sonuç:

``` text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

Sorgu:

``` sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

Sonuç:

``` text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

Elemanların tek bir konuma çok dişli yerleştirilmesi.

Sorgu:

``` sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

Bu sorgu sonucunda rastgele tamsayı elde edersiniz `[0,9]` Aralık. Mesela:

``` text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
```

## groupArrayMovingSum {#agg_function-grouparraymovingsum}

Giriş değerlerinin hareketli toplamını hesaplar.

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

İşlev, pencere boyutunu bir parametre olarak alabilir. Belirtilmemiş bırakılırsa, işlev, sütundaki satır sayısına eşit pencere boyutunu alır.

**Parametre**

-   `numbers_for_summing` — [İfade](../syntax.md#syntax-expressions) sayısal veri türü değeri ile sonuçlanır.
-   `window_size` — Size of the calculation window.

**Döndürülen değerler**

-   Giriş verileri ile aynı boyut ve türdeki dizi.

**Örnek**

Örnek tablo:

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

Sorgu:

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

Giriş değerlerinin hareketli ortalamasını hesaplar.

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

İşlev, pencere boyutunu bir parametre olarak alabilir. Belirtilmemiş bırakılırsa, işlev, sütundaki satır sayısına eşit pencere boyutunu alır.

**Parametre**

-   `numbers_for_summing` — [İfade](../syntax.md#syntax-expressions) sayısal veri türü değeri ile sonuçlanır.
-   `window_size` — Size of the calculation window.

**Döndürülen değerler**

-   Giriş verileri ile aynı boyut ve türdeki dizi.

İşlev kullanır [sıfıra doğru yuvarlama](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). Sonuç veri türü için önemsiz ondalık basamaklar keser.

**Örnek**

Örnek tablo `b`:

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

Sorgu:

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

## groupUniqArray (x), groupUniqArray (max_size)(x) {#groupuniqarrayx-groupuniqarraymax-sizex}

Farklı bağımsız değişken değerlerinden bir dizi oluşturur. Bellek tüketimi için aynıdır `uniqExact` İşlev.

İkinci versiyonu (ile `max_size` parametre), elde edilen dizinin boyutunu sınırlar `max_size` öğeler.
Mesela, `groupUniqArray(1)(x)` eşdeğ toer equivalentdir `[any(x)]`.

## quantile {#quantile}

Yaklaşık hesaplar [quantile](https://en.wikipedia.org/wiki/Quantile) sayısal veri dizisinin.

Bu işlev geçerlidir [rezerv reservoiruar örnek samplinglemesi](https://en.wikipedia.org/wiki/Reservoir_sampling) 8192'ye kadar bir rezervuar boyutu ve örnekleme için rastgele sayı üreteci ile. Sonuç deterministik değildir. Tam bir miktar elde etmek için [quantileExact](#quantileexact) İşlev.

Çoklu kullanırken `quantile*` bir sorguda farklı düzeylerde işlevler, iç durumları birleştirilmez (diğer bir deyişle, sorgu olabilir daha az verimli çalışır). Bu durumda, kullan [quantiles](#quantiles) İşlev.

**Sözdizimi**

``` sql
quantile(level)(expr)
```

Takma ad: `median`.

**Parametre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` aralığında değer `[0.01, 0.99]`. Varsayılan değer: 0.5. Yanında `level=0.5` fonksiyon hesaplar [medyan](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [veri türleri](../../sql-reference/data-types/index.md#data_types), [Tarihli](../../sql-reference/data-types/date.md) veya [DateTime](../../sql-reference/data-types/datetime.md).

**Döndürülen değer**

-   Belirtilen seviyenin yaklaşık miktarı.

Tür:

-   [Float64](../../sql-reference/data-types/float.md) sayısal veri türü girişi için.
-   [Tarihli](../../sql-reference/data-types/date.md) giriş değerleri varsa `Date` tür.
-   [DateTime](../../sql-reference/data-types/datetime.md) giriş değerleri varsa `DateTime` tür.

**Örnek**

Giriş tablosu:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Sorgu:

``` sql
SELECT quantile(val) FROM t
```

Sonuç:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**Ayrıca Bakınız**

-   [medyan](#median)
-   [quantiles](#quantiles)

## quantileDeterministic {#quantiledeterministic}

Yaklaşık hesaplar [quantile](https://en.wikipedia.org/wiki/Quantile) sayısal veri dizisinin.

Bu işlev geçerlidir [rezerv reservoiruar örnek samplinglemesi](https://en.wikipedia.org/wiki/Reservoir_sampling) 8192'ye kadar bir rezervuar boyutu ve örnekleme deterministik algoritması ile. Sonuç deterministiktir. Tam bir miktar elde etmek için [quantileExact](#quantileexact) İşlev.

Çoklu kullanırken `quantile*` bir sorguda farklı düzeylerde işlevler, iç durumları birleştirilmez (diğer bir deyişle, sorgu olabilir daha az verimli çalışır). Bu durumda, kullan [quantiles](#quantiles) İşlev.

**Sözdizimi**

``` sql
quantileDeterministic(level)(expr, determinator)
```

Takma ad: `medianDeterministic`.

**Parametre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` aralığında değer `[0.01, 0.99]`. Varsayılan değer: 0.5. Yanında `level=0.5` fonksiyon hesaplar [medyan](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [veri türleri](../../sql-reference/data-types/index.md#data_types), [Tarihli](../../sql-reference/data-types/date.md) veya [DateTime](../../sql-reference/data-types/datetime.md).
-   `determinator` — Number whose hash is used instead of a random number generator in the reservoir sampling algorithm to make the result of sampling deterministic. As a determinator you can use any deterministic positive number, for example, a user id or an event id. If the same determinator value occures too often, the function works incorrectly.

**Döndürülen değer**

-   Belirtilen seviyenin yaklaşık miktarı.

Tür:

-   [Float64](../../sql-reference/data-types/float.md) sayısal veri türü girişi için.
-   [Tarihli](../../sql-reference/data-types/date.md) giriş değerleri varsa `Date` tür.
-   [DateTime](../../sql-reference/data-types/datetime.md) giriş değerleri varsa `DateTime` tür.

**Örnek**

Giriş tablosu:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Sorgu:

``` sql
SELECT quantileDeterministic(val, 1) FROM t
```

Sonuç:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**Ayrıca Bakınız**

-   [medyan](#median)
-   [quantiles](#quantiles)

## quantileExact {#quantileexact}

Tam olarak hesaplar [quantile](https://en.wikipedia.org/wiki/Quantile) sayısal veri dizisinin.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` bellek, nerede `n` geçirilen değerler say .ısıdır. Bununla birlikte, az sayıda değer için, işlev çok etkilidir.

Çoklu kullanırken `quantile*` bir sorguda farklı düzeylerde işlevler, iç durumları birleştirilmez (diğer bir deyişle, sorgu olabilir daha az verimli çalışır). Bu durumda, kullan [quantiles](#quantiles) İşlev.

**Sözdizimi**

``` sql
quantileExact(level)(expr)
```

Takma ad: `medianExact`.

**Parametre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` aralığında değer `[0.01, 0.99]`. Varsayılan değer: 0.5. Yanında `level=0.5` fonksiyon hesaplar [medyan](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [veri türleri](../../sql-reference/data-types/index.md#data_types), [Tarihli](../../sql-reference/data-types/date.md) veya [DateTime](../../sql-reference/data-types/datetime.md).

**Döndürülen değer**

-   Belirtilen seviyenin miktarı.

Tür:

-   [Float64](../../sql-reference/data-types/float.md) sayısal veri türü girişi için.
-   [Tarihli](../../sql-reference/data-types/date.md) giriş değerleri varsa `Date` tür.
-   [DateTime](../../sql-reference/data-types/datetime.md) giriş değerleri varsa `DateTime` tür.

**Örnek**

Sorgu:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

Sonuç:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

**Ayrıca Bakınız**

-   [medyan](#median)
-   [quantiles](#quantiles)

## quantilexactweighted {#quantileexactweighted}

Tam olarak hesaplar [quantile](https://en.wikipedia.org/wiki/Quantile) her elemanın ağırlığını dikkate alarak sayısal bir veri dizisinin.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Each value is counted with its weight, as if it is present `weight` times. A hash table is used in the algorithm. Because of this, if the passed values ​​are frequently repeated, the function consumes less RAM than [quantileExact](#quantileexact). Bunun yerine bu işlevi kullanabilirsiniz `quantileExact` ve 1 ağırlığını belirtin.

Çoklu kullanırken `quantile*` bir sorguda farklı düzeylerde işlevler, iç durumları birleştirilmez (diğer bir deyişle, sorgu olabilir daha az verimli çalışır). Bu durumda, kullan [quantiles](#quantiles) İşlev.

**Sözdizimi**

``` sql
quantileExactWeighted(level)(expr, weight)
```

Takma ad: `medianExactWeighted`.

**Parametre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` aralığında değer `[0.01, 0.99]`. Varsayılan değer: 0.5. Yanında `level=0.5` fonksiyon hesaplar [medyan](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [veri türleri](../../sql-reference/data-types/index.md#data_types), [Tarihli](../../sql-reference/data-types/date.md) veya [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence members. Weight is a number of value occurrences.

**Döndürülen değer**

-   Belirtilen seviyenin miktarı.

Tür:

-   [Float64](../../sql-reference/data-types/float.md) sayısal veri türü girişi için.
-   [Tarihli](../../sql-reference/data-types/date.md) giriş değerleri varsa `Date` tür.
-   [DateTime](../../sql-reference/data-types/datetime.md) giriş değerleri varsa `DateTime` tür.

**Örnek**

Giriş tablosu:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

Sorgu:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

Sonuç:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**Ayrıca Bakınız**

-   [medyan](#median)
-   [quantiles](#quantiles)

## quantileTiming {#quantiletiming}

Belirlenen hassas hesaplar ile [quantile](https://en.wikipedia.org/wiki/Quantile) sayısal veri dizisinin.

Sonuç deterministiktir (sorgu işleme sırasına bağlı değildir). Fonksiyon yükleme web sayfaları kez veya arka uç yanıt süreleri gibi dağılımları tanımlamak dizileri ile çalışmak için optimize edilmiştir.

Çoklu kullanırken `quantile*` bir sorguda farklı düzeylerde işlevler, iç durumları birleştirilmez (diğer bir deyişle, sorgu olabilir daha az verimli çalışır). Bu durumda, kullan [quantiles](#quantiles) İşlev.

**Sözdizimi**

``` sql
quantileTiming(level)(expr)
```

Takma ad: `medianTiming`.

**Parametre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` aralığında değer `[0.01, 0.99]`. Varsayılan değer: 0.5. Yanında `level=0.5` fonksiyon hesaplar [medyan](https://en.wikipedia.org/wiki/Median).

-   `expr` — [İfade](../syntax.md#syntax-expressions) bir sütun değerleri üzerinde dönen bir [Yüzdürmek\*](../../sql-reference/data-types/float.md)- tip numarası.

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

**Doğruluk**

Hesaplama doğru ise:

-   Toplam değer sayısı 5670'i geçmez.
-   Toplam değer sayısı 5670'i aşıyor, ancak sayfa yükleme süresi 1024 ms'den az.

Aksi takdirde, hesaplamanın sonucu 16 MS'nin en yakın katlarına yuvarlanır.

!!! note "Not"
    Sayfa yükleme süresi nicelerini hesaplamak için, bu işlev daha etkili ve doğrudur [quantile](#quantile).

**Döndürülen değer**

-   Belirtilen seviyenin miktarı.

Tür: `Float32`.

!!! note "Not"
    İşlev valuese hiçbir değer geçir (ilmem (işse (kullanırken `quantileTimingIf`), [Nine](../../sql-reference/data-types/float.md#data_type-float-nan-inf) döndürülür. Bunun amacı, bu vakaları sıfır ile sonuçlanan vakalardan ayırmaktır. Görmek [ORDER BY FLA BYGE](../statements/select/order-by.md#select-order-by) sıralama ile ilgili notlar için `NaN` değerler.

**Örnek**

Giriş tablosu:

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

Sorgu:

``` sql
SELECT quantileTiming(response_time) FROM t
```

Sonuç:

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**Ayrıca Bakınız**

-   [medyan](#median)
-   [quantiles](#quantiles)

## niceletimingweighted {#quantiletimingweighted}

Belirlenen hassas hesaplar ile [quantile](https://en.wikipedia.org/wiki/Quantile) her sıra üyesi ağırlığına göre sayısal veri dizisi.

Sonuç deterministiktir (sorgu işleme sırasına bağlı değildir). Fonksiyon yükleme web sayfaları kez veya arka uç yanıt süreleri gibi dağılımları tanımlamak dizileri ile çalışmak için optimize edilmiştir.

Çoklu kullanırken `quantile*` bir sorguda farklı düzeylerde işlevler, iç durumları birleştirilmez (diğer bir deyişle, sorgu olabilir daha az verimli çalışır). Bu durumda, kullan [quantiles](#quantiles) İşlev.

**Sözdizimi**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

Takma ad: `medianTimingWeighted`.

**Parametre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` aralığında değer `[0.01, 0.99]`. Varsayılan değer: 0.5. Yanında `level=0.5` fonksiyon hesaplar [medyan](https://en.wikipedia.org/wiki/Median).

-   `expr` — [İfade](../syntax.md#syntax-expressions) bir sütun değerleri üzerinde dönen bir [Yüzdürmek\*](../../sql-reference/data-types/float.md)- tip numarası.

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**Doğruluk**

Hesaplama doğru ise:

-   Toplam değer sayısı 5670'i geçmez.
-   Toplam değer sayısı 5670'i aşıyor, ancak sayfa yükleme süresi 1024 ms'den az.

Aksi takdirde, hesaplamanın sonucu 16 MS'nin en yakın katlarına yuvarlanır.

!!! note "Not"
    Sayfa yükleme süresi nicelerini hesaplamak için, bu işlev daha etkili ve doğrudur [quantile](#quantile).

**Döndürülen değer**

-   Belirtilen seviyenin miktarı.

Tür: `Float32`.

!!! note "Not"
    İşlev valuese hiçbir değer geçir (ilmem (işse (kullanırken `quantileTimingIf`), [Nine](../../sql-reference/data-types/float.md#data_type-float-nan-inf) döndürülür. Bunun amacı, bu vakaları sıfır ile sonuçlanan vakalardan ayırmaktır. Görmek [ORDER BY FLA BYGE](../statements/select/order-by.md#select-order-by) sıralama ile ilgili notlar için `NaN` değerler.

**Örnek**

Giriş tablosu:

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

Sorgu:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

Sonuç:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

**Ayrıca Bakınız**

-   [medyan](#median)
-   [quantiles](#quantiles)

## quantileTDigest {#quantiletdigest}

Yaklaşık hesaplar [quantile](https://en.wikipedia.org/wiki/Quantile) kullanarak sayısal veri diz ofisinin [t-dig -est](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algoritma.

Maksimum hata %1'dir. Bellek tüketimi `log(n)`, nere `n` değer say isısıdır. Sonuç, sorguyu çalıştırma sırasına bağlıdır ve nondeterministic.

Fonksiyonun performansı, performanstan daha düşüktür [quantile](#quantile) veya [quantileTiming](#quantiletiming). Durum boyutunun hassasiyete oranı açısından, bu işlev çok daha iyidir `quantile`.

Çoklu kullanırken `quantile*` bir sorguda farklı düzeylerde işlevler, iç durumları birleştirilmez (diğer bir deyişle, sorgu olabilir daha az verimli çalışır). Bu durumda, kullan [quantiles](#quantiles) İşlev.

**Sözdizimi**

``` sql
quantileTDigest(level)(expr)
```

Takma ad: `medianTDigest`.

**Parametre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` aralığında değer `[0.01, 0.99]`. Varsayılan değer: 0.5. Yanında `level=0.5` fonksiyon hesaplar [medyan](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [veri türleri](../../sql-reference/data-types/index.md#data_types), [Tarihli](../../sql-reference/data-types/date.md) veya [DateTime](../../sql-reference/data-types/datetime.md).

**Döndürülen değer**

-   Belirtilen seviyenin yaklaşık miktarı.

Tür:

-   [Float64](../../sql-reference/data-types/float.md) sayısal veri türü girişi için.
-   [Tarihli](../../sql-reference/data-types/date.md) giriş değerleri varsa `Date` tür.
-   [DateTime](../../sql-reference/data-types/datetime.md) giriş değerleri varsa `DateTime` tür.

**Örnek**

Sorgu:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

Sonuç:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**Ayrıca Bakınız**

-   [medyan](#median)
-   [quantiles](#quantiles)

## quantileTDigestWeighted {#quantiletdigestweighted}

Yaklaşık hesaplar [quantile](https://en.wikipedia.org/wiki/Quantile) kullanarak sayısal veri diz ofisinin [t-dig -est](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algoritma. İşlev, her sıra üyesinin ağırlığını dikkate alır. Maksimum hata %1'dir. Bellek tüketimi `log(n)`, nere `n` değer say isısıdır.

Fonksiyonun performansı, performanstan daha düşüktür [quantile](#quantile) veya [quantileTiming](#quantiletiming). Durum boyutunun hassasiyete oranı açısından, bu işlev çok daha iyidir `quantile`.

Sonuç, sorguyu çalıştırma sırasına bağlıdır ve nondeterministic.

Çoklu kullanırken `quantile*` bir sorguda farklı düzeylerde işlevler, iç durumları birleştirilmez (diğer bir deyişle, sorgu olabilir daha az verimli çalışır). Bu durumda, kullan [quantiles](#quantiles) İşlev.

**Sözdizimi**

``` sql
quantileTDigest(level)(expr)
```

Takma ad: `medianTDigest`.

**Parametre**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` aralığında değer `[0.01, 0.99]`. Varsayılan değer: 0.5. Yanında `level=0.5` fonksiyon hesaplar [medyan](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [veri türleri](../../sql-reference/data-types/index.md#data_types), [Tarihli](../../sql-reference/data-types/date.md) veya [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**Döndürülen değer**

-   Belirtilen seviyenin yaklaşık miktarı.

Tür:

-   [Float64](../../sql-reference/data-types/float.md) sayısal veri türü girişi için.
-   [Tarihli](../../sql-reference/data-types/date.md) giriş değerleri varsa `Date` tür.
-   [DateTime](../../sql-reference/data-types/datetime.md) giriş değerleri varsa `DateTime` tür.

**Örnek**

Sorgu:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

Sonuç:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**Ayrıca Bakınız**

-   [medyan](#median)
-   [quantiles](#quantiles)

## medyan {#median}

Bu `median*` fonksiyonlar karşılık gelen takma adlardır `quantile*` işlevler. Sayısal bir veri örneğinin medyanını hesaplarlar.

İşlevler:

-   `median` — Alias for [quantile](#quantile).
-   `medianDeterministic` — Alias for [quantileDeterministic](#quantiledeterministic).
-   `medianExact` — Alias for [quantileExact](#quantileexact).
-   `medianExactWeighted` — Alias for [quantilexactweighted](#quantileexactweighted).
-   `medianTiming` — Alias for [quantileTiming](#quantiletiming).
-   `medianTimingWeighted` — Alias for [niceletimingweighted](#quantiletimingweighted).
-   `medianTDigest` — Alias for [quantileTDigest](#quantiletdigest).
-   `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](#quantiletdigestweighted).

**Örnek**

Giriş tablosu:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Sorgu:

``` sql
SELECT medianDeterministic(val, 1) FROM t
```

Sonuç:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```

## quantiles(level1, level2, …)(x) {#quantiles}

Tüm quantile fonksiyonları da karşılık gelen quantile fonksiyonlarına sahiptir: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`. Bu işlevler, listelenen seviyelerin tüm nicelerini tek geçişte hesaplar ve elde edilen değerlerin bir dizisini döndürür.

## varSamp (x) {#varsampx}

Miktarı hesaplar `Σ((x - x̅)^2) / (n - 1)`, nere `n` örneklem büyüklüğü ve `x̅`ortalama değer isidir `x`.

Bir rassal değişkenin varyansının tarafsız bir tahminini temsil eder, eğer geçirilen değerler numunesini oluşturursa.

Dönüşler `Float64`. Ne zaman `n <= 1`, dönüşler `+∞`.

!!! note "Not"
    Bu işlev sayısal olarak kararsız algoritma kullanır. İhtiyacınız varsa [sayısal kararlılık](https://en.wikipedia.org/wiki/Numerical_stability) hesaplamalarda kullan `varSampStable` İşlev. Daha yavaş çalışır, ancak daha düşük hesaplama hatası sağlar.

## varPop (x) {#varpopx}

Miktarı hesaplar `Σ((x - x̅)^2) / n`, nere `n` örneklem büyüklüğü ve `x̅`ortalama değer isidir `x`.

Başka bir deyişle, bir dizi değer için dağılım. Dönüşler `Float64`.

!!! note "Not"
    Bu işlev sayısal olarak kararsız algoritma kullanır. İhtiyacınız varsa [sayısal kararlılık](https://en.wikipedia.org/wiki/Numerical_stability) hesaplamalarda kullan `varPopStable` İşlev. Daha yavaş çalışır, ancak daha düşük hesaplama hatası sağlar.

## stddevSamp(x) {#stddevsampx}

Sonuç kareköküne eşittir `varSamp(x)`.

!!! note "Not"
    Bu işlev sayısal olarak kararsız algoritma kullanır. İhtiyacınız varsa [sayısal kararlılık](https://en.wikipedia.org/wiki/Numerical_stability) hesaplamalarda kullan `stddevSampStable` İşlev. Daha yavaş çalışır, ancak daha düşük hesaplama hatası sağlar.

## stddevPop(x) {#stddevpopx}

Sonuç kareköküne eşittir `varPop(x)`.

!!! note "Not"
    Bu işlev sayısal olarak kararsız algoritma kullanır. İhtiyacınız varsa [sayısal kararlılık](https://en.wikipedia.org/wiki/Numerical_stability) hesaplamalarda kullan `stddevPopStable` İşlev. Daha yavaş çalışır, ancak daha düşük hesaplama hatası sağlar.

## topK (N) (x) {#topknx}

Belirtilen sütundaki yaklaşık en sık değerleri bir dizi döndürür. Elde edilen dizi, değerlerin yaklaşık frekansının azalan sırasına göre sıralanır (değerlerin kendileri tarafından değil).

Uygular [Filtrelenmiş Yerden Tasarruf](http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf) TopK analiz etmek için algoritma, azaltmak ve birleştirmek algoritması dayalı [Paralel Alan Tasarrufu](https://arxiv.org/pdf/1401.0702.pdf).

``` sql
topK(N)(column)
```

Bu işlev garantili bir sonuç sağlamaz. Bazı durumlarda, hatalar oluşabilir ve en sık kullanılan değerler olmayan sık değerler döndürebilir.

Biz kullanmanızı öneririz `N < 10` değer; performans büyük ile azalır `N` değerler. Maksimum değeri `N = 65536`.

**Parametre**

-   ‘N’ dönmek için Öğe sayısıdır.

Parametre atlanırsa, varsayılan değer 10 kullanılır.

**Değişkenler**

-   ' x ' – The value to calculate frequency.

**Örnek**

Tak thee the [OnTime](../../getting-started/example-datasets/ontime.md) veri kümesi ve üç en sık oluşan değerleri seçin `AirlineID` sütun.

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

Benzer `topK` ancak tamsayı türünde bir ek argüman alır - `weight`. Her değer muhasebeleştirilir `weight` frekans hesaplaması için zamanlar.

**Sözdizimi**

``` sql
topKWeighted(N)(x, weight)
```

**Parametre**

-   `N` — The number of elements to return.

**Değişkenler**

-   `x` – The value.
-   `weight` — The weight. [Uİnt8](../../sql-reference/data-types/int-uint.md).

**Döndürülen değer**

Maksimum yaklaşık ağırlık toplamına sahip değerlerin bir dizisini döndürür.

**Örnek**

Sorgu:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

Sonuç:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```

## covarSamp(x, y) {#covarsampx-y}

Değerini hesaplar `Σ((x - x̅)(y - y̅)) / (n - 1)`.

Float64 Döndürür. Ne zaman `n <= 1`, returns +∞.

!!! note "Not"
    Bu işlev sayısal olarak kararsız algoritma kullanır. İhtiyacınız varsa [sayısal kararlılık](https://en.wikipedia.org/wiki/Numerical_stability) hesaplamalarda kullan `covarSampStable` İşlev. Daha yavaş çalışır, ancak daha düşük hesaplama hatası sağlar.

## covarPop (x, y) {#covarpopx-y}

Değerini hesaplar `Σ((x - x̅)(y - y̅)) / n`.

!!! note "Not"
    Bu işlev sayısal olarak kararsız algoritma kullanır. İhtiyacınız varsa [sayısal kararlılık](https://en.wikipedia.org/wiki/Numerical_stability) hesaplamalarda kullan `covarPopStable` İşlev. Daha yavaş çalışır, ancak daha düşük bir hesaplama hatası sağlar.

## corr(x, y) {#corrx-y}

Pearson korelasyon katsayısını hesaplar: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`.

!!! note "Not"
    Bu işlev sayısal olarak kararsız algoritma kullanır. İhtiyacınız varsa [sayısal kararlılık](https://en.wikipedia.org/wiki/Numerical_stability) hesaplamalarda kullan `corrStable` İşlev. Daha yavaş çalışır, ancak daha düşük hesaplama hatası sağlar.

## categoricalınformationvalue {#categoricalinformationvalue}

Değerini hesaplar `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` her kategori için.

``` sql
categoricalInformationValue(category1, category2, ..., tag)
```

Sonuç, ayrık (kategorik) bir özelliğin nasıl olduğunu gösterir `[category1, category2, ...]` değerini öngör aen bir öğrenme modeline katkıda `tag`.

## simpleLinearRegression {#simplelinearregression}

Basit (tek boyutlu) doğrusal regresyon gerçekleştirir.

``` sql
simpleLinearRegression(x, y)
```

Parametre:

-   `x` — Column with dependent variable values.
-   `y` — Column with explanatory variable values.

Döndürülen değerler:

Devamlılar `(a, b)` ortaya çıkan hat linetın `y = a*x + b`.

**Örnekler**

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

## stokastiklinearregression {#agg_functions-stochasticlinearregression}

Bu fonksiyon stokastik doğrusal regresyon uygular. Öğrenme oranı, L2 regularization katsayısı, mini-batch boyutu için özel parametreleri destekler ve ağırlıkları güncellemek için birkaç yöntem vardır ([Adem](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) (varsayılan olarak kullanılır), [basit SGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent), [İvme](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum), [Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)).

### Parametre {#agg_functions-stochasticlinearregression-parameters}

4 özelleştirilebilir parametre vardır. Onlar sırayla işleve geçirilir, ancak dört varsayılan değerleri kullanılacak geçmek gerek yoktur, ancak iyi bir model bazı parametre ayarlama gerekli.

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` Gradyan iniş adımı gerçekleştirildiğinde adım uzunluğundaki katsayıdır. Çok büyük öğrenme oranı, modelin sonsuz ağırlıklarına neden olabilir. Default is `0.00001`.
2.  `l2 regularization coefficient` hangi overfitting önlemek için yardımcı olabilir. Default is `0.1`.
3.  `mini-batch size` gradyanların hesaplanacağı ve Gradyan inişinin bir adımını gerçekleştirmek için toplanacağı öğelerin sayısını ayarlar. Saf stokastik iniş bir eleman kullanır, ancak küçük partilere(yaklaşık 10 eleman) sahip olmak degrade adımları daha kararlı hale getirir. Default is `15`.
4.  `method for updating weights` onlar : `Adam` (varsayılan olarak), `SGD`, `Momentum`, `Nesterov`. `Momentum` ve `Nesterov` biraz daha fazla hesaplama ve bellek gerektirir, ancak stokastik Gradyan yöntemlerinin yakınsama hızı ve kararlılığı açısından yararlı olurlar.

### Kullanma {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` iki adımda kullanılır: modelin takılması ve yeni verilerin tahmin edilmesi. Modeli sığdırmak ve daha sonra kullanım için durumunu kaydetmek için kullandığımız `-State` temel olarak durumu kurtaran birleştirici (model ağırlıkları, vb.).
Fonksiyonu kullan wedığımızı tahmin etmek [evalMLMethod](../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod), bir argüman olarak bir durumu yanı sıra tahmin etmek için özellikler alır.

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** Uydurma

Böyle bir sorgu kullanılabilir.

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

Burada ayrıca veri eklememiz gerekiyor `train_data` Tablo. Parametrelerin sayısı sabit değildir, sadece argümanların sayısına bağlıdır, `linearRegressionState`. Hepsi sayısal değerler olmalıdır.
Hedef değere sahip sütunun(tahmin etmeyi öğrenmek istediğimiz) ilk argüman olarak eklendiğini unutmayın.

**2.** Öngören

Bir durumu tabloya kaydettikten sonra, tahmin için birden çok kez kullanabilir, hatta diğer durumlarla birleşebilir ve yeni daha iyi modeller oluşturabiliriz.

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

Sorgu, tahmin edilen değerlerin bir sütununu döndürür. Not ilk argüman `evalMLMethod` oluyor `AggregateFunctionState` nesne, sonraki özelliklerin sütunlarıdır.

`test_data` bir tablo gibi mi `train_data` ancak hedef değer içermeyebilir.

### Not {#agg_functions-stochasticlinearregression-notes}

1.  İki modeli birleştirmek için Kullanıcı böyle bir sorgu oluşturabilir:
    `sql  SELECT state1 + state2 FROM your_models`
    nerede `your_models` tablo her iki modeli de içerir. Bu sorgu yeni dönecektir `AggregateFunctionState` nesne.

2.  Kullanıcı, modeli kaydetmeden oluşturulan modelin ağırlıklarını kendi amaçları için alabilir `-State` birleştirici kullanılır.
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    Bu sorgu modele uyacak ve ağırlıklarını geri getirecektir-ilk önce modelin parametrelerine karşılık gelen ağırlıklar, sonuncusu önyargıdır. Yani yukarıdaki örnekte sorgu 3 değer içeren bir sütun döndürecektir.

**Ayrıca Bakınız**

-   [stochasticLogisticRegression](#agg_functions-stochasticlogisticregression)
-   [Doğrusal ve lojistik regresyonlar arasındaki fark](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

Bu işlev stokastik lojistik regresyon uygular. İkili sınıflandırma problemi için kullanılabilir, stochasticLinearRegression ile aynı özel parametreleri destekler ve aynı şekilde çalışır.

### Parametre {#agg_functions-stochasticlogisticregression-parameters}

Parametreler tam olarak stochasticLinearRegression ile aynıdır:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
Daha fazla bilgi için bkz. [parametre](#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

1.  Uydurma

<!-- -->

    See the `Fitting` section in the [stochasticLinearRegression](#stochasticlinearregression-usage-fitting) description.

    Predicted labels have to be in \[-1, 1\].

1.  Öngören

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

**Ayrıca Bakınız**

-   [stokastiklinearregression](#agg_functions-stochasticlinearregression)
-   [Doğrusal ve lojistik regresyonlar arasındaki fark.](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## groupBitmapAnd {#groupbitmapand}

Bu VE bir bitmap sütun, Uınt64 tür iade önem, hesaplamaları suffix ekleme -Devlet, sonra iade [bitmap nesnesi](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapAnd(expr)
```

**Parametre**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` tür.

**Dönüş değeri**

Bu değer `UInt64` tür.

**Örnek**

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

YA da bir bitmap sütun, Uınt64 tür iade önem, hesaplamaları suffix ekleme -Devlet, sonra iade [bitmap nesnesi](../../sql-reference/functions/bitmap-functions.md). Bu eşdeğerdir `groupBitmapMerge`.

``` sql
groupBitmapOr(expr)
```

**Parametre**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` tür.

**Dönüş değeri**

Bu değer `UInt64` tür.

**Örnek**

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

Bir bitmap sütun, Uınt64 tür iade önem hesaplamaları XOR, suffix ekleme -Devlet, sonra iade [bitmap nesnesi](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapOr(expr)
```

**Parametre**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` tür.

**Dönüş değeri**

Bu değer `UInt64` tür.

**Örnek**

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

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/agg_functions/reference/) <!--hide-->
