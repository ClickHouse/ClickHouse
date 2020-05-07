---
machine_translated: true
machine_translated_rev: 0f7ef7704d018700049223525bad4a63911b6e70
toc_priority: 33
toc_title: SELECT
---

# Select Queries sözdizimi {#select-queries-syntax}

`SELECT` veri alma gerçekleştirir.

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN (subquery)|table USING columns_list
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

Tüm yan tümceleri isteğe bağlıdır, hemen sonra ifadelerin gerekli listesi hariç seçin.
Aşağıdaki yan tümceleri sorgu yürütme konveyör hemen hemen aynı sırada açıklanmıştır.

Sorgu atlarsa `DISTINCT`, `GROUP BY` ve `ORDER BY` CLA andus Andes and the `IN` ve `JOIN` alt sorgular, sorgu o (1) RAM miktarını kullanarak tamamen akış işlenecektir.
Aksi takdirde, uygun kısıtlamalar belirtilmezse, sorgu çok fazla RAM tüketebilir: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. Daha fazla bilgi için bölüme bakın “Settings”. Harici sıralama (geçici tabloları bir diske kaydetme) ve harici toplama kullanmak mümkündür. `The system does not have "merge join"`.

### Fık WİTHRA ile {#with-clause}

Bu bölüm, ortak tablo ifadeleri için destek sağlar ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), bazı sınırlamalar ile:
1. Özyinelemeli sorgular desteklenmiyor
2. Alt sorgu bölüm ile birlikte kullanıldığında, sonuç tam olarak bir satır ile skaler olmalıdır
3. İfadenin sonuçları alt sorgularda kullanılamaz
WITH yan tümcesi ifadeleri sonuçları SELECT yan tümcesi içinde kullanılabilir.

Örnek 1: Sabit ifadeyi aşağıdaki gibi kullanma “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

Örnek 2: SELECT yan tümcesi sütun listesinden toplam(bayt) ifade sonucunu çıkarma

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

Örnek 3: skaler alt sorgu sonuçlarını kullanma

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

Örnek 4: alt sorguda ifadeyi yeniden kullanma
Alt sorgularda ifade kullanımı için geçerli sınırlama için bir geçici çözüm olarak çoğaltabilirsiniz.

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```

### Fık FROMRAS FROMINDAN {#select-from}

FROM yan tümcesi atlanırsa, veriler `system.one` Tablo.
Bu `system.one` tablo tam olarak bir satır içerir (bu tablo diğer Dbms'lerde bulunan çift tablo ile aynı amacı yerine getirir).

Bu `FROM` yan tümcesi veri okumak için kaynak belirtir:

-   Tablo
-   Alt sorgu
-   [Tablo fonksiyonu](../table-functions/index.md#table-functions)

`ARRAY JOIN` ve düzenli `JOIN` ayrıca dahil edilebilir (aşağıya bakınız).

Bunun yerine bir tablo, `SELECT` alt sorgu parantez içinde belirtilebilir.
Standart SQL aksine, bir eşanlamlı bir alt sorgudan sonra belirtilmesi gerekmez.

Bir sorguyu yürütmek için, sorguda listelenen tüm sütunlar uygun tablodan ayıklanır. Dış sorgu için gerekli olmayan tüm sütunlar alt sorgulardan atılır.
Bir sorgu herhangi bir sütun listelemezse (örneğin, `SELECT count() FROM t`), satır sayısını hesaplamak için yine de tablodan bir sütun çıkarılır (en küçük olanı tercih edilir).

#### Son değiştirici {#select-from-final}

Tablolardan veri seçerken uygulanabilir [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)- motor ailesi dışında `GraphiteMergeTree`. Ne zaman `FINAL` belirtilen, ClickHouse sonucu döndürmeden önce verileri tam olarak birleştirir ve böylece verilen tablo altyapısı için birleştirmeler sırasında gerçekleşen tüm veri dönüşümlerini gerçekleştirir.

Ayrıca için desteklenen:
- [Çoğaltıyordu](../../engines/table-engines/mergetree-family/replication.md) sürümleri `MergeTree` motorlar.
- [Görünüm](../../engines/table-engines/special/view.md), [Arabellek](../../engines/table-engines/special/buffer.md), [Dağılı](../../engines/table-engines/special/distributed.md), ve [MaterializedView](../../engines/table-engines/special/materializedview.md) üzerinden oluşturul ,maları koşuluyla diğer motorlar üzerinde çalışan motorlar `MergeTree`- motor masaları.

Kullanan sorgular `FINAL` olmayan benzer sorgular kadar hızlı Yürüt ,ülür, çünkü:

-   Sorgu tek bir iş parçacığında yürütülür ve veri sorgu yürütme sırasında birleştirilir.
-   İle sorgular `FINAL` sorguda belirtilen sütunlara ek olarak birincil anahtar sütunlarını okuyun.

Çoğu durumda, kullanmaktan kaçının `FINAL`.

### Örnek Madde {#select-sample-clause}

Bu `SAMPLE` yan tümcesi yaklaşık sorgu işleme için izin verir.

Veri örneklemesi etkinleştirildiğinde, sorgu tüm veriler üzerinde değil, yalnızca belirli bir veri kesirinde (örnek) gerçekleştirilir. Örneğin, tüm ziyaretler için istatistikleri hesaplamanız gerekiyorsa, sorguyu tüm ziyaretlerin 1/10 kesirinde yürütmek ve ardından sonucu 10 ile çarpmak yeterlidir.

Yaklaşık sorgu işleme aşağıdaki durumlarda yararlı olabilir:

-   Sıkı zamanlama gereksinimleriniz olduğunda (\<100ms gibi), ancak bunları karşılamak için ek donanım kaynaklarının maliyetini haklı çıkaramazsınız.
-   Ham verileriniz doğru olmadığında, yaklaşım kaliteyi belirgin şekilde düşürmez.
-   İş gereksinimleri yaklaşık sonuçları hedef alır (maliyet etkinliği için veya kesin sonuçları premium kullanıcılara pazarlamak için).

!!! note "Not"
    Örneklemeyi yalnızca aşağıdaki tablolarla kullanabilirsiniz: [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tablo oluşturma sırasında örnekleme ifadesi belirtilmişse (bkz [MergeTree motoru](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).

Veri örneklemesinin özellikleri aşağıda listelenmiştir:

-   Veri örneklemesi deterministik bir mekanizmadır. Aynı sonucu `SELECT .. SAMPLE` sorgu her zaman aynıdır.
-   Örnekleme, farklı tablolar için sürekli olarak çalışır. Tek bir örnekleme anahtarına sahip tablolar için, aynı katsayıya sahip bir örnek her zaman olası verilerin aynı alt kümesini seçer. Örneğin, kullanıcı kimlikleri örneği, farklı tablolardan olası tüm kullanıcı kimliklerinin aynı alt kümesine sahip satırları alır. Bu, örneği alt sorgularda kullanabileceğiniz anlamına gelir. [IN](#select-in-operators) yan. Ayrıca, kullanarak örnekleri katılabilir [JOIN](#select-join) yan.
-   Örnekleme, bir diskten daha az veri okumayı sağlar. Örnekleme anahtarını doğru belirtmeniz gerektiğini unutmayın. Daha fazla bilgi için, bkz. [MergeTree tablosu oluşturma](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

İçin `SAMPLE` yan tümcesi aşağıdaki sözdizimi desteklenir:

| SAMPLE Clause Syntax | Açıklama                                                                                                                                                                                                                                                             |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | Burada `k` 0'dan 1'e kadar olan sayıdır.</br>Sorgu üzerinde yürütülür `k` verilerin kesir. Mesela, `SAMPLE 0.1` sorguyu verilerin %10'unda çalıştırır. [Daha fazla bilgi edinin](#select-sample-k)                                                                   |
| `SAMPLE n`           | Burada `n` yeterince büyük bir tamsayıdır.</br>Sorgu en az bir örnek üzerinde yürütülür `n` satırlar (ancak bundan önemli ölçüde daha fazla değil). Mesela, `SAMPLE 10000000` sorguyu en az 10.000.000 satır çalıştırır. [Daha fazla bilgi edinin](#select-sample-n) |
| `SAMPLE k OFFSET m`  | Burada `k` ve `m` 0'dan 1'e kadar olan sayılardır.</br>Sorgu bir örnek üzerinde yürütülür `k` verilerin kesir. Örnek için kullanılan veriler, `m` bölme. [Daha fazla bilgi edinin](#select-sample-offset)                                                            |

#### SAMPLE K {#select-sample-k}

Burada `k` 0'dan 1'e kadar olan sayıdır (hem kesirli hem de ondalık gösterimler desteklenir). Mesela, `SAMPLE 1/2` veya `SAMPLE 0.5`.

İn a `SAMPLE k` fık ,ra, örnek alınır `k` verilerin kesir. Örnek aşağıda gösterilmiştir:

``` sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

Bu örnekte, sorgu 0.1 (%10) veri örneği üzerinde yürütülür. Toplam fonksiyonların değerleri otomatik olarak düzeltilmez, bu nedenle yaklaşık bir sonuç elde etmek için, değer `count()` elle 10 ile çarpılır.

#### SAMPLE N {#select-sample-n}

Burada `n` yeterince büyük bir tamsayıdır. Mesela, `SAMPLE 10000000`.

Bu durumda, sorgu en az bir örnek üzerinde yürütülür `n` satırlar (ancak bundan önemli ölçüde daha fazla değil). Mesela, `SAMPLE 10000000` sorguyu en az 10.000.000 satır çalıştırır.

Veri okuma için minimum birim bir granül olduğundan (boyutu `index_granularity` ayar), granülün boyutundan çok daha büyük bir örnek ayarlamak mantıklıdır.

Kullanırken `SAMPLE n` yan tümce, verilerin hangi göreli yüzde işlendiğini bilmiyorsunuz. Yani toplam fonksiyonların çarpılması gereken katsayıyı bilmiyorsunuz. Kullan... `_sample_factor` sanal sütun yaklaşık sonucu almak için.

Bu `_sample_factor` sütun dinamik olarak hesaplanan göreli katsayıları içerir. Bu sütun otomatik olarak oluşturulduğunda [oluşturmak](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) belirtilen örnekleme anahtarına sahip bir tablo. Kullanım örnekleri `_sample_factor` sütun aşağıda gösterilmiştir.

Masayı düşünelim `visits`, site ziyaretleri ile ilgili istatistikleri içerir. İlk örnek, sayfa görünümlerinin sayısını nasıl hesaplayacağınızı gösterir:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

Bir sonraki örnek, toplam ziyaret sayısını nasıl hesaplayacağınızı gösterir:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

Aşağıdaki örnek, ortalama oturum süresinin nasıl hesaplanacağını göstermektedir. Ortalama değerleri hesaplamak için göreli katsayıyı kullanmanız gerekmediğini unutmayın.

``` sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

#### SAMPLE K OFFSET M {#select-sample-offset}

Burada `k` ve `m` 0'dan 1'e kadar olan sayılardır. Örnekler aşağıda gösterilmiştir.

**Örnek 1**

``` sql
SAMPLE 1/10
```

Bu örnekte, örnek tüm verilerin 1/10'udur:

`[++------------]`

**Örnek 2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

Burada, verilerin ikinci yarısından %10'luk bir örnek alınır.

`[------++------]`

### Dizi Jo JOİNİN yan tüm Clausecesi {#select-array-join-clause}

Yürüt allowsmeyi sağlar `JOIN` bir dizi veya iç içe veri yapısı ile. Niyet benzer [arrayJoin](../functions/array-join.md#functions_arrayjoin) işlev, ancak işlevselliği daha geniştir.

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

Yalnızca bir tek belirtebilirsiniz `ARRAY JOIN` bir sorguda yan tümcesi.

Sorgu yürütme sırası çalışırken en iyi duruma getirilmiştir `ARRAY JOIN`. Rağmen `ARRAY JOIN` her zaman önce belirtilmelidir `WHERE/PREWHERE` fık ,ra, daha önce de yapılabilir `WHERE/PREWHERE` (sonuç bu maddede gerekliyse) veya tamamladıktan sonra (hesaplamaların hacmini azaltmak için). İşlem sırası sorgu iyileştiricisi tarafından denetlenir.

Desteklenen türleri `ARRAY JOIN` aşağıda listelenmiştir:

-   `ARRAY JOIN` - Bu durumda, boş diziler sonucu dahil değildir `JOIN`.
-   `LEFT ARRAY JOIN` Bunun sonucu `JOIN` boş dizilere sahip satırlar içerir. Boş bir dizinin değeri, dizi öğesi türü için varsayılan değere ayarlanır (genellikle 0, boş dize veya NULL).

Aşağıdaki örnekler kullanımını göstermektedir `ARRAY JOIN` ve `LEFT ARRAY JOIN` yanlar. Bir tablo oluşturalım [Dizi](../../sql-reference/data-types/array.md) sütun yazın ve içine değerler ekleyin:

``` sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

``` text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

Aşağıdaki örnek kullanır `ARRAY JOIN` yan:

``` sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

``` text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

Sonraki örnek kullanımlar `LEFT ARRAY JOIN` yan:

``` sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

``` text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

#### Takma Ad Kullanma {#using-aliases}

Bir dizi için bir diğer ad belirtilebilir `ARRAY JOIN` yan. Bu durumda, bir dizi öğesine bu diğer adla erişilebilir, ancak dizinin kendisine özgün adla erişilir. Örnek:

``` sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

``` text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

Takma adlar kullanarak şunları yapabilirsiniz `ARRAY JOIN` harici bir dizi ile. Mesela:

``` sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

``` text
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

Birden çok diziler virgülle ayrılmış olabilir `ARRAY JOIN` yan. Bu durumda, `JOIN` onlarla aynı anda gerçekleştirilir (doğrudan toplam, kartezyen ürün değil). Tüm dizilerin aynı boyuta sahip olması gerektiğini unutmayın. Örnek:

``` sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

Aşağıdaki örnek kullanır [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) işlev:

``` sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

#### İç içe veri yapısı ile dizi birleştirme {#array-join-with-nested-data-structure}

`ARRAY`Jo "in " ile de çalışır [iç içe veri yapıları](../../sql-reference/data-types/nested-data-structures/nested.md). Örnek:

``` sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

``` text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

İç içe geçmiş veri yapılarının adlarını belirtirken `ARRAY JOIN` anlam aynıdır `ARRAY JOIN` içerdiği tüm dizi öğeleri ile. Örnekler aşağıda listelenmiştir:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Bu varyasyon da mantıklı:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

Bir diğer ad, iç içe geçmiş bir veri yapısı için kullanılabilir. `JOIN` sonuç veya kaynak dizi. Örnek:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

Kullanma örneği [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) işlev:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

### Jo {#select-join}

Verileri normal olarak birleştirir [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) anlama.

!!! info "Not"
    İlgili [ARRAY JOIN](#select-array-join-clause).

``` sql
SELECT <expr_list>
FROM <left_subquery>
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN <right_subquery>
(ON <expr_list>)|(USING <column_list>) ...
```

Tablo adları yerine belirtilebilir `<left_subquery>` ve `<right_subquery>`. Bu eşdeğerdir `SELECT * FROM table` alt sorgu, tablonun sahip olduğu özel bir durum dışında [Katmak](../../engines/table-engines/special/join.md) engine – an array prepared for joining.

#### Desteklenen türleri `JOIN` {#select-join-types}

-   `INNER JOIN` (veya `JOIN`)
-   `LEFT JOIN` (veya `LEFT OUTER JOIN`)
-   `RIGHT JOIN` (veya `RIGHT OUTER JOIN`)
-   `FULL JOIN` (veya `FULL OUTER JOIN`)
-   `CROSS JOIN` (veya `,` )

Standarda bakın [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) açıklama.

#### Çoklu birleştirme {#multiple-join}

Sorguları gerçekleştiren ClickHouse, çoklu tablo birleşimlerini iki tablo birleşimlerinin sırasına yeniden yazar. Örneğin, JOIN ClickHouse için dört tablo varsa birinci ve ikinci katılır, ardından üçüncü tablo ile sonuç katılır ve son adımda dördüncü bir katılır.

Bir sorgu içeriyorsa `WHERE` yan tümcesi, ClickHouse Ara birleştirme aracılığıyla bu yan tümcesi filtreleri pushdown çalışır. Filtreyi her Ara birleşime uygulayamazsa, tüm birleşimler tamamlandıktan sonra clickhouse filtreleri uygular.

Biz tavsiye `JOIN ON` veya `JOIN USING` sorguları oluşturmak için sözdizimi. Mesela:

``` sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a = t3.a
```

Virgülle ayrılmış tablo listelerini kullanabilirsiniz. `FROM` yan. Mesela:

``` sql
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a
```

Bu sözdizimleri karıştırmayın.

ClickHouse virgülle sözdizimini doğrudan desteklemez, bu yüzden bunları kullanmanızı önermiyoruz. Algoritma, sorguyu şu şekilde yeniden yazmaya çalışır: `CROSS JOIN` ve `INNER JOIN` yan tümceleri ve sonra sorgu işleme devam eder. Sorguyu yeniden yazarken, ClickHouse performansı ve bellek tüketimini en iyi duruma getirmeye çalışır. Varsayılan olarak, ClickHouse virgülleri bir `INNER JOIN` CLA anduse and conver andts `INNER JOIN` -e doğru `CROSS JOIN` algoritma bunu garanti edemez zaman `INNER JOIN` gerekli verileri döndürür.

#### Katılık {#select-join-strictness}

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Kartezyen ürün](https://en.wikipedia.org/wiki/Cartesian_product) eşleşen satırlardan. Bu standart `JOIN` SQL davranış.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of queries with `ANY` ve `ALL` anahtar kelimeler aynıdır.
-   `ASOF` — For joining sequences with a non-exact match. `ASOF JOIN` kullanım aşağıda açıklanmıştır.

**ASOF JOIN kullanımı**

`ASOF JOIN` tam olarak eşleşmeyen kayıtlara katılmanız gerektiğinde kullanışlıdır.

İçin tablolar `ASOF JOIN` sıralı bir sıra sütunu olmalıdır. Bu sütun bir tabloda tek başına olamaz ve veri türlerinden biri olmalıdır: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date`, ve `DateTime`.

Sözdizimi `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

Herhangi bir sayıda eşitlik koşulunu ve tam olarak en yakın eşleşme koşulunu kullanabilirsiniz. Mesela, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

En yakın maç için desteklenen koşullar: `>`, `>=`, `<`, `<=`.

Sözdizimi `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` kullanma `equi_columnX` eşit onliğe katılma ve `asof_column` ile en yakın maça katılmak için `table_1.asof_column >= table_2.asof_column` koşul. Bu `asof_column` sütun her zaman sonuncusu `USING` yan.

Örneğin, aşağıdaki tabloları göz önünde bulundurun:

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN` bir kullanıcı etkinliğinin zaman damgasını alabilir `table_1` ve bir olay bulmak `table_2` zaman damgasının olayın zaman damgasına en yakın olduğu yer `table_1` en yakın maç durumuna karşılık gelir. Varsa eşit zaman damgası değerleri en yakın olanıdır. Burada `user_id` sütun eşitlik üzerine katılmak için kullanılabilir ve `ev_time` sütun en yakın eşleşmeye katılmak için kullanılabilir. Örn oureğ inimizde, `event_1_1` ile Birleştir joinedilebilir `event_2_1` ve `event_1_2` ile Birleştir joinedilebilir `event_2_3`, ama `event_2_2` Birleştir .ilemez.

!!! note "Not"
    `ASOF` Jo isin is **değil** desteklenen [Katmak](../../engines/table-engines/special/join.md) masa motoru.

Varsayılan strictness değerini ayarlamak için oturum yapılandırma parametresini kullanın [join\_default\_strictness](../../operations/settings/settings.md#settings-join_default_strictness).

#### GLOBAL JOIN {#global-join}

Normal kullanırken `JOIN`, sorgu uzak sunuculara gönderilir. Alt sorgular, doğru tabloyu yapmak için her biri üzerinde çalıştırılır ve birleştirme bu tablo ile gerçekleştirilir. Başka bir deyişle, doğru tablo her sunucuda ayrı ayrı oluşturulur.

Kullanırken `GLOBAL ... JOIN`, önce istekte bulunan sunucu, doğru tabloyu hesaplamak için bir alt sorgu çalıştırır. Bu geçici tablo her uzak sunucuya geçirilir ve iletilen geçici verileri kullanarak sorgular çalıştırılır.

Kullanırken dikkatli olun `GLOBAL`. Daha fazla bilgi için bölüme bakın [Dağıtılmış alt sorgular](#select-distributed-subqueries).

#### Kullanım Önerileri {#usage-recommendations}

Çalışırken bir `JOIN`, sorgunun diğer aşamaları ile ilgili olarak yürütme sırasının optimizasyonu yoktur. Birleştirme (sağ tablodaki bir arama), filtrelemeden önce çalıştırılır `WHERE` ve toplamadan önce. İşlem sırasını açıkça ayarlamak için, bir `JOIN` bir alt sorgu ile alt sorgu.

Örnek:

``` sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

``` text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

Alt sorgular, belirli bir alt sorgudan bir sütuna başvurmak için adları ayarlamanıza veya bunları kullanmanıza izin vermez.
Belirtilen sütunlar `USING` her iki alt sorguda da aynı adlara sahip olmalı ve diğer sütunların farklı olarak adlandırılması gerekir. Alt sorgulardaki sütunların adlarını değiştirmek için diğer adları kullanabilirsiniz (örnek, diğer adları kullanır `hits` ve `visits`).

Bu `USING` yan tümcesi, bu sütunların eşitliğini oluşturan katılmak için bir veya daha fazla sütun belirtir. Sütunların listesi parantez olmadan ayarlanır. Daha karmaşık birleştirme koşulları desteklenmez.

Sağ tablo (alt sorgu sonucu) RAM'de bulunur. Yeterli bellek yoksa, bir `JOIN`.

Her seferinde bir sorgu aynı ile çalıştırılır `JOIN`, sonuç önbelleğe alınmadığı için alt sorgu yeniden çalıştırılır. Bunu önlemek için özel [Katmak](../../engines/table-engines/special/join.md) her zaman RAM'de olan birleştirme için hazırlanmış bir dizi olan tablo motoru.

Bazı durumlarda, kullanımı daha verimlidir `IN` yerine `JOIN`.
Çeşitli türleri arasında `JOIN` en verimli `ANY LEFT JOIN`, sonraları `ANY INNER JOIN`. En az verimli `ALL LEFT JOIN` ve `ALL INNER JOIN`.

Bir ihtiyacınız varsa `JOIN` boyut tablolarıyla birleştirmek için (bunlar, reklam kampanyalarının adları gibi boyut özelliklerini içeren nispeten küçük tablolardır), bir `JOIN` her sorgu için doğru tabloya yeniden erişilmesi nedeniyle çok uygun olmayabilir. Bu durumlar için, bir “external dictionaries” yerine kullanmanız gereken özellik `JOIN`. Daha fazla bilgi için bölüme bakın [Dış söz dictionarieslükler](../dictionaries/external-dictionaries/external-dicts.md).

**Bellek Sınırlamaları**

ClickHouse kullanır [has joinh Jo joinin](https://en.wikipedia.org/wiki/Hash_join) algoritma. ClickHouse alır `<right_subquery>` ve RAM'de bunun için bir karma tablo oluşturur. Birleştirme işlemi bellek tüketimini kısıtlamanız gerekiyorsa aşağıdaki ayarları kullanın:

-   [max\_rows\_in\_join](../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [max\_bytes\_in\_join](../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

Bu sınırlardan herhangi birine ulaşıldığında, ClickHouse [join\_overflow\_mode](../../operations/settings/query-complexity.md#settings-join_overflow_mode) ayar talimatı verir.

#### Boş veya boş hücrelerin işlenmesi {#processing-of-empty-or-null-cells}

Tabloları birleştirirken, boş hücreler görünebilir. Ayar [join\_use\_nulls](../../operations/settings/settings.md#join_use_nulls) Clickhouse'un bu hücreleri nasıl doldurduğunu tanımlayın.

Eğer... `JOIN` key Ares are [Nullable](../data-types/nullable.md) alanlar, anahtarlardan en az birinin değeri olan satırlar [NULL](../syntax.md#null-literal) Birleştir .ilmez.

#### Sözdizimi Sınırlamaları {#syntax-limitations}

Çoklu için `JOIN` CLA aus AES in a single `SELECT` sorgu:

-   Aracılığıyla tüm sütunları alarak `*` yalnızca tablolar birleştirildiğinde kullanılabilir, alt sorgular değil.
-   Bu `PREWHERE` fıkra availablesı mevcut değildir.

İçin `ON`, `WHERE`, ve `GROUP BY` yanlar:

-   Keyfi ifadeler kullanılamaz `ON`, `WHERE`, ve `GROUP BY` yan tümceleri, ancak bir ifade tanımlayabilirsiniz `SELECT` yan tümce ve daha sonra bu yan tümcelerde bir takma ad ile kullanın.

### WHERE  {#select-where}

Bir WHERE yan tümcesi varsa, uint8 türüne sahip bir ifade içermelidir. Bu genellikle karşılaştırma ve mantıksal işleçlere sahip bir ifadedir.
Bu ifade, diğer tüm dönüşümlerden önce verileri filtrelemek için kullanılır.

Dizinler veritabanı tablo altyapısı tarafından destekleniyorsa, ifade dizinleri kullanma yeteneği değerlendirilir.

### PREWHERE maddesi {#prewhere-clause}

Bu madde, WHERE maddesi ile aynı anlama sahiptir. Fark, verilerin tablodan okunmasıdır.
Prewhere kullanırken, önce yalnızca prewhere yürütmek için gerekli olan sütunlar okunur. Daha sonra sorguyu çalıştırmak için gerekli olan diğer sütunlar okunur, ancak yalnızca PREWHERE ifadesinin doğru olduğu bloklar.

Sorgudaki sütunların azınlığı tarafından kullanılan, ancak güçlü veri filtrasyonu sağlayan filtreleme koşulları varsa, PREWHERE kullanmak mantıklıdır. Bu, okunacak veri hacmini azaltır.

Örneğin, çok sayıda sütun ayıklayan, ancak yalnızca birkaç sütun için filtrelemeye sahip olan sorgular için PREWHERE yazmak yararlıdır.

PREWHERE sadece tablolar tarafından desteklenmektedir `*MergeTree` aile.

Bir sorgu aynı anda prewhere ve WHERE belirtebilirsiniz. Bu durumda, PREWHERE nerede önce gelir.

Eğer... ‘optimize\_move\_to\_prewhere’ ayar 1 olarak ayarlanır ve prewhere atlanır, sistem otomatik olarak ifadelerin parçalarını prewhere yerden taşımak için sezgisel kullanır.

### GROUP BY Fık Clausera {#select-group-by-clause}

Bu, sütun yönelimli bir DBMS'NİN en önemli parçalarından biridir.

Bir GROUP BY yan tümcesi varsa, ifadelerin bir listesini içermelidir. Her ifade burada bir “key”.
SELECT, HAVİNG ve ORDER BY yan tümcelerindeki tüm ifadeler, anahtarlardan veya toplama işlevlerinden hesaplanmalıdır. Başka bir deyişle, tablodan seçilen her sütun, anahtarlarda veya toplama işlevlerinde kullanılmalıdır.

Bir sorgu toplama işlevleri içinde yalnızca tablo sütunları içeriyorsa, GROUP BY yan tümcesi atlanabilir ve boş bir anahtar kümesi tarafından toplama varsayılır.

Örnek:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

Bununla birlikte, standart SQL'İN aksine, tabloda herhangi bir satır yoksa (ya hiç yok ya da FİLTRELENECEK yeri kullandıktan sonra yok), boş bir sonuç döndürülür ve toplam işlevlerin başlangıç değerlerini içeren satırlardan birinin sonucu değil.

Mysql'in aksine (ve standart SQL'E uygun olarak), bir anahtar veya toplama işlevinde olmayan (sabit ifadeler hariç) bazı sütunun bir değerini alamazsınız. Bu sorunu gidermek için kullanabilirsiniz ‘any’ toplama işlevi (ilk karşılaşılan değeri al) veya ‘min/max’.

Örnek:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

Karşılaşılan her farklı anahtar değeri için GROUP BY, bir dizi toplama işlevi değeri hesaplar.

GROUP BY dizi sütunları için desteklenmiyor.

Bir sabit, toplam işlevler için bağımsız değişken olarak belirtilemez. Örnek: Toplam (1). Bunun yerine, sabitten kurtulabilirsiniz. Örnek: `count()`.

#### NULL işleme {#null-processing}

Gruplama için ClickHouse yorumlar [NULL](../syntax.md#null-literal) bir değer olarak ve `NULL=NULL`.

İşte bunun ne anlama geldiğini göstermek için bir örnek.

Bu tabloya sahip olduğunuzu varsayalım:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Sorgu `SELECT sum(x), y FROM t_null_big GROUP BY y` res inult ins in:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

Bunu görebilirsiniz `GROUP BY` için `y = NULL` özetlemek `x`, sanki `NULL` bu değerdir.

İçin birkaç anahtar geç iferseniz `GROUP BY`, sonuç size seçimin tüm kombinasyonlarını verecek, sanki `NULL` belirli bir değer vardı.

#### Toplamlar değiştirici ile {#with-totals-modifier}

Toplamları değiştirici ile belirtilirse, başka bir satır hesaplanır. Bu satır, varsayılan değerleri (sıfırlar veya boş satırlar) içeren anahtar sütunlara ve tüm satırlar arasında hesaplanan değerlerle toplam işlevlerin sütunlarına sahip olacaktır ( “total” değerler).

Bu ekstra satır, diğer satırlardan ayrı olarak JSON\*, TabSeparated\* ve Pretty\* formatlarında çıktıdır. Diğer biçimlerde, bu satır çıktı değildir.

JSON \* formatlarında, bu satır ayrı olarak çıktı ‘totals’ alan. TabSeparated \* biçimlerinde satır, boş bir satırdan önce gelen ana sonuçtan sonra gelir (diğer verilerden sonra). Pretty \* biçimlerinde, satır ana sonuçtan sonra ayrı bir tablo olarak çıktılanır.

`WITH TOTALS` sahip olduğunda farklı şekillerde çalıştırılabilir. Davranış bağlıdır ‘totals\_mode’ ayar.
Varsayılan olarak, `totals_mode = 'before_having'`. Bu durumda, ‘totals’ olan ve geç onesmeyen satırlar da dahil olmak üzere tüm satırlar arasında hesaplanır. ‘max\_rows\_to\_group\_by’.

Diğer alternatifler, yalnızca sahip olan satırları içerir ‘totals’, ve ayar ile farklı davranır `max_rows_to_group_by` ve `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. Başka bir deyişle, ‘totals’ eğer olduğu gibi daha az veya aynı sayıda satıra sahip olacak `max_rows_to_group_by` atlanmış.

`after_having_inclusive` – Include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ içinde ‘totals’. Başka bir deyişle, ‘totals’ daha fazla veya aynı sayıda satır olacak `max_rows_to_group_by` atlanmış.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ içinde ‘totals’. Aksi takdirde, bunları dahil etmeyin.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

Eğer `max_rows_to_group_by` ve `group_by_overflow_mode = 'any'` kullanıl ,mıyor, tüm vary ,asyonları `after_having` aynıdır ve bunlardan herhangi birini kullanabilirsiniz (Örneğin, `after_having_auto`).

JOIN yan tümcesindeki alt sorgular da dahil olmak üzere alt sorgulardaki TOPLAMLARLA birlikte kullanabilirsiniz (bu durumda, ilgili toplam değerler birleştirilir).

#### Harici bellekte grupla {#select-group-by-in-external-memory}

Sırasında bellek kullanımını kısıtlamak için diske geçici veri boşaltma etkinleştirebilirsiniz `GROUP BY`.
Bu [max\_bytes\_before\_external\_group\_by](../../operations/settings/settings.md#settings-max_bytes_before_external_group_by) ayar damping için eşik RAM tüketimini belirler `GROUP BY` dosya sistemine geçici veri. 0 (varsayılan) olarak ayarlanırsa, devre dışı bırakılır.

Kullanırken `max_bytes_before_external_group_by` öneririz ayarladığınız `max_memory_usage` yüksek iki katı daha fazla. Bu, toplanmanın iki aşaması olduğundan gereklidir: tarihi okumak ve ara verileri (1) oluşturmak ve ara verileri (2) birleştirmek. Dosya sistemine veri damping yalnızca aşama 1 sırasında oluşabilir. Geçici veriler dökülmediyse, aşama 2, aşama 1'deki gibi aynı miktarda bellek gerektirebilir.

Örneğin, [max\_memory\_usage](../../operations/settings/settings.md#settings_max_memory_usage) 100000000 olarak ayarlandı ve harici toplama kullanmak istiyorsanız, ayarlamak mantıklı `max_bytes_before_external_group_by` için 10000000000 ve max\_memory\_usage için 20000000000. Harici toplama tetiklendiğinde (en az bir geçici veri dökümü varsa), maksimum RAM tüketimi sadece biraz daha fazladır `max_bytes_before_external_group_by`.

Dağıtılmış sorgu işleme ile uzak sunucularda harici toplama gerçekleştirilir. İstekte bulunan sunucunun yalnızca az miktarda RAM kullanması için `distributed_aggregation_memory_efficient` 1'e.

Verileri diske temizlerken ve uzak sunuculardan gelen sonuçları birleştirirken `distributed_aggregation_memory_efficient` ayarı etkin, tüketir kadar `1/256 * the_number_of_threads` toplam RAM miktarından.

Harici toplama etkinleştirildiğinde, daha az olsaydı `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

Eğer bir `ORDER BY` ile bir `LIMIT` sonra `GROUP BY`, daha sonra kullanılan RAM miktarı veri miktarına bağlıdır `LIMIT` bütün masada değil. Ama eğer `ORDER BY` yok `LIMIT`, harici sıralamayı etkinleştirmeyi unutmayın (`max_bytes_before_external_sort`).

### Madde ile sınır {#limit-by-clause}

İle bir sorgu `LIMIT n BY expressions` fık thera birinci `n` her bir farklı değer için satırlar `expressions`. Anahtarı `LIMIT BY` herhangi bir sayıda içerebilir [ifadeler](../syntax.md#syntax-expressions).

ClickHouse aşağıdaki sözdizimini destekler:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

Sorgu işleme sırasında ClickHouse, sıralama anahtarı tarafından sipariş edilen verileri seçer. Sıralama anahtarı açıkça bir [ORDER BY](#select-order-by) yan tümcesi veya örtük bir özellik olarak tablo altyapısı. Sonra ClickHouse geçerlidir `LIMIT n BY expressions` ve ilk döndürür `n` her farklı kombinasyon için satırlar `expressions`. Eğer `OFFSET` Belirtilen, daha sonra farklı bir kombinasyonuna ait her veri bloğu için `expressions`, ClickHouse atlar `offset_value` bloğun başından itibaren satır sayısı ve en fazla döndürür `n` sonuç olarak satırlar. Eğer `offset_value` veri bloğundaki satır sayısından daha büyük olan ClickHouse, bloktan sıfır satır döndürür.

`LIMIT BY` ilgili değildir `LIMIT`. Her ikisi de aynı sorguda kullanılabilir.

**Örnekler**

Örnek tablo:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by values(1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Sorgular:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Bu `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` sorgu aynı sonucu verir.

Aşağıdaki sorgu, her biri için en iyi 5 yönlendiriciyi döndürür `domain, device_type` toplamda maksimum 100 satır ile eşleştirin (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

### Fık HAVİNGRA olması {#having-clause}

WHERE yan tümcesine benzer şekilde, gruptan sonra alınan sonucun filtrelenmesine izin verir.
NEREDE ve sonra gerçekleştirilmesi sırasında toplama (GRUP) TARAFINDAN daha önce yapılacağı YERİ olan, farklı OLMASI.
Toplama yapılmazsa, sahip kullanılamaz.

### ORDER BY FLA BYGE {#select-order-by}

ORDER BY yan tümcesi, her birine DESC veya ASC (sıralama yönü) atanabilen ifadelerin bir listesini içerir. Yön belirtilmezse, ASC varsayılır. ASC artan sırada sıralanır ve azalan sırada DESC edilir. Sıralama yönü, tüm listeye değil, tek bir ifade için geçerlidir. Örnek: `ORDER BY Visits DESC, SearchPhrase`

Dize değerlerine göre sıralamak için harmanlama (karşılaştırma) belirtebilirsiniz. Örnek: `ORDER BY SearchPhrase COLLATE 'tr'` - artan sırayla anahtar kelimeye göre sıralama için, Türk alfabesini kullanarak, büyük / küçük harf duyarsız, dizelerin UTF-8 kodlanmış olduğunu varsayarak. Harmanlama belirtilebilir veya her ifade için bağımsız olarak sırayla değil. ASC veya DESC belirtilirse, harmanla ondan sonra belirtilir. Harmanlama kullanırken, sıralama her zaman büyük / küçük harf duyarsızdır.

Harmanlama ile sıralama, baytlara göre normal sıralamadan daha az verimli olduğundan, yalnızca az sayıda satırın son sıralaması için harmanlamayı kullanmanızı öneririz.

Sıralama ifadeleri listesi için aynı değerlere sahip olan satırlar, isteğe bağlı bir sırayla çıktılanır ve bu da nondeterministic (her seferinde farklı) olabilir.
ORDER BY yan tümcesi atlanırsa, satırların sırası da tanımsızdır ve nondeterministic de olabilir.

`NaN` ve `NULL` sıralama sırası:

-   Değiştirici ile `NULLS FIRST` — First `NULL`, sonraları `NaN`, sonra diğer değerler.
-   Değiştirici ile `NULLS LAST` — First the values, then `NaN`, sonraları `NULL`.
-   Default — The same as with the `NULLS LAST` değiştirici.

Örnek:

Masa için

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Sorguyu Çalıştır `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` olmak:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Kayan nokta numaraları sıralandığında, Nan'lar diğer değerlerden ayrıdır. Sıralama sırasına bakılmaksızın, Nan'lar sonunda gelir. Başka bir deyişle, artan sıralama için, diğer tüm sayılardan daha büyük gibi yerleştirilirken, azalan sıralama için, diğerlerinden daha küçük gibi yerleştirilirler.

Tarafından siparişe ek olarak yeterince küçük bir sınır belirtilirse daha az RAM kullanılır. Aksi takdirde, harcanan bellek miktarı sıralama için veri hacmi ile orantılıdır. Dağıtılmış sorgu işleme için GROUP BY atlanırsa, sıralama kısmen uzak sunucularda yapılır ve sonuçları istekte bulunan sunucuda birleştirilir. Bu, dağıtılmış sıralama için, sıralanacak veri hacminin tek bir sunucudaki bellek miktarından daha büyük olabileceği anlamına gelir.

Yeterli RAM yoksa, harici bellekte sıralama yapmak mümkündür (bir diskte geçici dosyalar oluşturmak). Ayarı kullan `max_bytes_before_external_sort` bu amaçla. 0 (varsayılan) olarak ayarlanırsa, dış sıralama devre dışı bırakılır. Etkinleştirilirse, sıralanacak veri hacmi belirtilen bayt sayısına ulaştığında, toplanan veriler sıralanır ve geçici bir dosyaya dökülür. Tüm veriler okunduktan sonra, sıralanmış tüm dosyalar birleştirilir ve sonuçlar çıktılanır. Dosyalar yapılandırmada /var/lib/clickhouse/tmp/ dizinine yazılır (varsayılan olarak, ancak ‘tmp\_path’ bu ayarı değiştirmek için parametre).

Bir sorguyu çalıştırmak, daha fazla bellek kullanabilir ‘max\_bytes\_before\_external\_sort’. Bu nedenle, bu ayarın önemli ölçüde daha küçük bir değere sahip olması gerekir ‘max\_memory\_usage’. Örnek olarak, sunucunuzda 128 GB RAM varsa ve tek bir sorgu çalıştırmanız gerekiyorsa, ‘max\_memory\_usage’ 100 GB ve ‘max\_bytes\_before\_external\_sort’ için 80 GB.

Harici sıralama, RAM'de sıralamaktan çok daha az etkili çalışır.

### SELECT CLA Clauseuse {#select-select}

[İfadeler](../syntax.md#syntax-expressions) belirtilen `SELECT` yukarıda açıklanan yan tümcelerde tüm işlemler tamamlandıktan sonra yan tümce hesaplanır. Bu ifadeler, sonuçtaki ayrı satırlara uygulanıyormuş gibi çalışır. İf ifadeleri `SELECT` yan tümcesi toplama işlevleri içerir, daha sonra ClickHouse toplama işlevleri ve argümanları sırasında kullanılan ifadeleri işler. [GROUP BY](#select-group-by-clause) toplanma.

Sonuçtaki tüm sütunları eklemek istiyorsanız, yıldız işaretini kullanın (`*`) sembol. Mesela, `SELECT * FROM ...`.

Sonuçtaki bazı sütunları bir ile eşleştirmek için [re2](https://en.wikipedia.org/wiki/RE2_(software)) düzenli ifade, kullanabilirsiniz `COLUMNS` ifade.

``` sql
COLUMNS('regexp')
```

Örneğin, tabloyu düşünün:

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

Aşağıdaki sorgu içeren tüm sütunlardan veri seçer `a` onların adına sembol.

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Seçilen sütunlar alfabetik sırayla döndürülür.

Birden fazla kullanabilirsiniz `COLUMNS` bir sorgudaki ifadeler ve bunlara işlevler uygulanır.

Mesela:

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Tarafından döndürülen her sütun `COLUMNS` ifade, işleve ayrı bir bağımsız değişken olarak geçirilir. Ayrıca, diğer argümanları da destekliyorsa işleve iletebilirsiniz. Fonksiyonları kullanırken dikkatli olun. Bir işlev, kendisine ilettiğiniz bağımsız değişken sayısını desteklemiyorsa, ClickHouse bir istisna atar.

Mesela:

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2.
```

Bu örnekte, `COLUMNS('a')` iki sütun döndürür: `aa` ve `ab`. `COLUMNS('c')` ret theur thens the `bc` sütun. Bu `+` operatör 3 argüman için geçerli olamaz, Bu nedenle ClickHouse ilgili mesajla bir istisna atar.

Eşleşen sütunlar `COLUMNS` ifade farklı veri türlerine sahip olabilir. Eğer `COLUMNS` herhangi bir sütun eşleşmiyor ve sadece ifadedir `SELECT`, ClickHouse bir istisna atar.

### Farklı Madde {#select-distinct}

DISTINCT belirtilirse, sonuçta tam olarak eşleşen satır kümelerinin dışında yalnızca tek bir satır kalır.
Sonuç, grup tarafından, toplama işlevleri olmadan SEÇ'TE belirtilen tüm alanlar arasında belirtildiği gibi aynı olacaktır. Ancak gruptan birkaç farklılık var:

-   DI DİSTİNCTST DİSTİNCTINC GROUPT GROUP BY ile birlikte uygulanabilir.
-   ORDER BY atlandığında ve LİMİT tanımlandığında, gerekli sayıda farklı satır okunduktan hemen sonra sorgu çalışmayı durdurur.
-   Veri blokları, işlenirken, tüm sorgunun çalışmayı bitirmesini beklemeden çıktılanır.

Select en az bir dizi sütunu varsa DISTINCT desteklenmez.

`DISTINCT` ile çalışır [NULL](../syntax.md#null-literal) sanki `NULL` belirli bir değer ve `NULL=NULL`. Diğer bir deyişle, içinde `DISTINCT` sonuçlar, farklı kombinasyonlar ile `NULL` yalnızca bir kez meydana gelir.

ClickHouse kullanarak destekler `DISTINCT` ve `ORDER BY` bir sorguda farklı sütunlar için yan tümceleri. Bu `DISTINCT` fık thera önce Yürüt theülür `ORDER BY` yan.

Örnek tablo:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

İle veri seç whenerken `SELECT DISTINCT a FROM t1 ORDER BY b ASC` sorgu, aşağıdaki sonucu elde ederiz:

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

Sıralama yönünü değiştirirsek `SELECT DISTINCT a FROM t1 ORDER BY b DESC` alalım şu sonuç:

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

Satır `2, 4` sıralamadan önce kesildi.

Sorguları programlarken bu uygulama özgüllüğünü dikkate alın.

### LİMİT maddesi {#limit-clause}

`LIMIT m` ilk seçmenizi sağlar `m` sonuçtan satırlar.

`LIMIT n, m` ilk seçmenizi sağlar `m` ilkini atladıktan sonra sonuçtan satırlar `n` satırlar. Bu `LIMIT m OFFSET n` sözdizimi de desteklenmektedir.

`n` ve `m` negatif olmayan tamsayılar olmalıdır.

Eğer yoksa bir `ORDER BY` sonuçları açıkça sıralayan yan tümce, sonuç keyfi ve belirsiz olabilir.

### Birlik tüm Fık Clausera {#union-all-clause}

Herhangi bir sayıda sorguyu birleştirmek için tümünü bir araya getir'i kullanabilirsiniz. Örnek:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Sadece birlik tüm desteklenmektedir. Düzenli birlik (birlik DISTINCT) desteklenmiyor. UNION DISTINCT gerekiyorsa, UNION ALL içeren bir alt sorgudan SELECT DISTINCT yazabilirsiniz.

UNİON ALL'IN bir parçası olan sorgular aynı anda çalıştırılabilir ve sonuçları birlikte karıştırılabilir.

Sonuçların yapısı (sütun sayısı ve türü) sorgular için eşleşmelidir. Ancak sütun adları farklı olabilir. Bu durumda, nihai sonuç için sütun adları ilk sorgudan alınacaktır. Sendikalar için Tip döküm yapılır. Örneğin, birleştirilen iki sorgu, non ile aynı alana sahipse-`Nullable` ve `Nullable` uyumlu bir türden tür, ortaya çıkan `UNION ALL` has a `Nullable` türü alanı.

UNİON'UN bir parçası olan sorgular parantez içine alınamaz. ORDER BY ve LİMİT, nihai sonuca değil, ayrı sorgulara uygulanır. Nihai sonuca bir dönüştürme uygulamanız gerekiyorsa, tüm sorguları UNION ALL ile FROM yan tümcesinde bir alt sorguya koyabilirsiniz.

### OUTFİLE fıkra içine {#into-outfile-clause}

Add the `INTO OUTFILE filename` yan tümcesi (burada dosyaadı bir dize değişmez) belirtilen dosyaya sorgu çıktısını yeniden yönlendirmek için.
MySQL aksine, dosya istemci tarafında oluşturulur. Aynı dosya adı ile bir dosya zaten varsa, sorgu başarısız olur.
Bu işlevsellik, komut satırı istemcisinde ve clickhouse-local'de kullanılabilir (HTTP arabirimi üzerinden gönderilen bir sorgu başarısız olur).

Varsayılan çıkış biçimi TabSeparated (komut satırı istemci toplu iş modunda olduğu gibi).

### FORMAT CLA Clauseuse {#format-clause}

Belirtmek ‘FORMAT format’ belirtilen herhangi bir biçimde veri almak için.
Bunu kolaylık sağlamak veya döküntüler oluşturmak için kullanabilirsiniz.
Daha fazla bilgi için bölüme bakın “Formats”.
FORMAT yan tümcesi atlanırsa, db'ye erişmek için kullanılan hem ayarlara hem de arabirime bağlı olan varsayılan biçim kullanılır. HTTP arabirimi ve toplu iş modunda komut satırı istemcisi için varsayılan biçim TabSeparated. Etkileşimli modda komut satırı istemcisi için varsayılan biçim PrettyCompact (çekici ve kompakt tablolara sahiptir).

Komut satırı istemcisini kullanırken, veri istemciye bir iç verimli biçimde geçirilir. İstemci, sorgunun FORMAT yan tümcesini bağımsız olarak yorumlar ve verilerin kendisini biçimlendirir (böylece ağı ve sunucuyu yükten kurtarır).

### Operatör İNLERDE {#select-in-operators}

Bu `IN`, `NOT IN`, `GLOBAL IN`, ve `GLOBAL NOT IN` operatörler, işlevleri oldukça zengin olduğu için ayrı ayrı kaplıdır.

Operatörün sol tarafı tek bir sütun veya bir tuple'dır.

Örnekler:

``` sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

Sol tarafı dizindeki tek bir sütun ve sağ tarafı sabitler kümesidir, sistem sorguyu işlemek için dizin kullanır.

Don't list too many values explicitly (i.e. millions). If a data set is large, put it in a temporary table (for example, see the section “External data for query processing”), sonra bir alt sorgu kullanın.

Operatörün sağ tarafı, sabit ifadeler kümesi, sabit ifadelere sahip bir dizi dizi (yukarıdaki örneklerde gösterilmiştir) veya bir veritabanı tablosunun adı veya parantez içinde alt sorgu olabilir.

Operatörün sağ tarafı bir tablonun adı ise (örneğin, `UserID IN users`), bu alt sorguya eşdeğerdir `UserID IN (SELECT * FROM users)`. Sorgu ile birlikte gönderilen dış verilerle çalışırken bunu kullanın. Örneğin, sorgu için yüklenen kullanıcı kimlikleri kümesi ile birlikte gönderilebilir ‘users’ filtrelenmesi gereken geçici tablo.

Operatörün sağ tarafında Set altyapısı (her zaman RAM'de hazırlanmış bir veri kümesi) olan bir tablo adı ise, veri kümesi her sorgu için yeniden oluşturulmaz.

Alt sorgu, tuples süzme için birden fazla sütun belirtebilir.
Örnek:

``` sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

In işlecinin solundaki ve sağındaki sütunlar aynı türe sahip olmalıdır.

In işleci ve alt sorgu, toplam işlevleri ve lambda işlevleri de dahil olmak üzere sorgunun herhangi bir bölümünde oluşabilir.
Örnek:

``` sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

``` text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

17 Mart'tan sonraki her gün için, 17 Mart'ta siteyi ziyaret eden kullanıcılar tarafından yapılan sayfa görüntüleme yüzdesini Sayın.
IN yan tümcesinde BIR alt sorgu her zaman tek bir sunucuda yalnızca bir kez çalıştırılır. Bağımlı alt sorgular yoktur.

#### NULL işleme {#null-processing-1}

İstek işleme sırasında, In operatörü, bir işlemin sonucunun [NULL](../syntax.md#null-literal) her zaman eşittir `0` olsun ne olursa olsun `NULL` operatörün sağ veya sol tarafındadır. `NULL` değerler herhangi bir veri kümesine dahil edilmez, birbirine karşılık gelmez ve karşılaştırılamaz.

İşte bir örnek ile `t_null` Tablo:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Sorguyu çalıştırma `SELECT x FROM t_null WHERE y IN (NULL,3)` size aşağıdaki sonucu verir:

``` text
┌─x─┐
│ 2 │
└───┘
```

Bu satır görebilirsiniz hangi `y = NULL` sorgu sonuçları dışarı atılır. Bunun nedeni ClickHouse karar veremez `NULL` dahildir `(NULL,3)` set, döner `0` operasyon sonucunda ve `SELECT` bu satırı son çıktıdan hariç tutar.

``` sql
SELECT y IN (NULL, 3)
FROM t_null
```

``` text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

#### Dağıtılmış Alt Sorgular {#select-distributed-subqueries}

Alt sorgularla IN-s için iki seçenek vardır (Birleştirmelere benzer): normal `IN` / `JOIN` ve `GLOBAL IN` / `GLOBAL JOIN`. Dağıtılmış sorgu işleme için nasıl çalıştırıldıkları bakımından farklılık gösterirler.

!!! attention "Dikkat"
    Aşağıda açıklanan algoritmaların Aşağıdakilere bağlı olarak farklı şekilde çalışabileceğini unutmayın [ayarlar](../../operations/settings/settings.md) `distributed_product_mode` ayar.

Normal IN kullanırken, sorgu uzak sunuculara gönderilir ve her biri alt sorguları `IN` veya `JOIN` yan.

Kullanırken `GLOBAL IN` / `GLOBAL JOINs`, ilk olarak tüm alt sorgular için çalıştırılır `GLOBAL IN` / `GLOBAL JOINs` ve sonuçlar geçici tablolarda toplanır. Daha sonra geçici tablolar, bu geçici verileri kullanarak sorguların çalıştırıldığı her uzak sunucuya gönderilir.

Dağıtılmış olmayan bir sorgu için normal `IN` / `JOIN`.

Alt sorguları kullanırken dikkatli olun `IN` / `JOIN` dağıtılmış sorgu işleme için yan tümceleri.

Bazı örneklere bakalım. Kümedeki her sunucunun normal olduğunu varsayalım **local\_table**. Her sunucu ayrıca bir **distributed\_table** tablo ile **Dağılı** kümedeki tüm sunuculara bakan tür.

Bir sorgu için **distributed\_table**, sorgu tüm uzak sunuculara gönderilecek ve bunları kullanarak üzerinde çalışacak **local\_table**.

Örneğin, sorgu

``` sql
SELECT uniq(UserID) FROM distributed_table
```

olarak tüm uzak sunucu sentlara gönder willilecektir

``` sql
SELECT uniq(UserID) FROM local_table
```

ve ara sonuçların birleştirilebileceği aşamaya ulaşana kadar her biri paralel olarak çalıştırın. Daha sonra Ara sonuçlar istekte bulunan sunucuya döndürülür ve üzerinde birleştirilir ve nihai sonuç istemciye gönderilir.

Şimdi bir sorguyu İN ile inceleyelim:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

-   İki sitenin izleyicilerinin kesişiminin hesaplanması.

Bu sorgu tüm uzak sunuculara şu şekilde gönderilecektir

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

Diğer bir deyişle, veri kümesi In yan tümcesi her sunucuda bağımsız olarak, yalnızca yerel olarak her bir sunucu üzerinde depolanan veriler üzerinden toplanır.

Bu durum için hazırlanan ve tek bir kullanıcı kimliği için veri tamamen tek bir sunucuda bulunduğu küme sunucuları arasında veri yaymak, bu düzgün ve en iyi şekilde çalışır. Bu durumda, gerekli tüm veriler her sunucuda yerel olarak mevcut olacaktır. Aksi takdirde, sonuç yanlış olacaktır. Sorgunun bu varyasyonuna şu şekilde atıfta bulunuyoruz “local IN”.

Veriler küme sunucularına rastgele yayıldığında sorgunun nasıl çalıştığını düzeltmek için şunları belirtebilirsiniz **distributed\_table** bir alt sorgu içinde. Sorgu şöyle görünürdü:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Bu sorgu tüm uzak sunuculara şu şekilde gönderilecektir

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Alt sorgu, her uzak sunucuda çalışmaya başlayacaktır. Alt sorgu dağıtılmış bir tablo kullandığından, her uzak sunucuda bulunan alt sorgu, her uzak sunucuya şu şekilde yeniden gönderilecektir

``` sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

Örneğin, 100 sunucu kümeniz varsa, tüm sorguyu yürütmek, genellikle kabul edilemez olarak kabul edilen 10.000 temel istek gerektirir.

Bu gibi durumlarda, her zaman IN yerine GLOBAL IN kullanmalısınız. Sorgu için nasıl çalıştığına bakalım

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Istekçi sunucu alt sorgu çalıştıracaktır

``` sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

ve sonuç RAM'de geçici bir tabloya konacak. Sonra istek olarak her uzak sunucuya gönderilecektir

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

ve geçici tablo `_data1` Sorgu ile her uzak sunucuya gönderilir (geçici tablonun adı uygulama tanımlı).

Bu normal İN kullanmaktan daha uygun. Ancak, aşağıdaki noktaları aklınızda bulundurun:

1.  Geçici bir tablo oluştururken, veriler benzersiz hale getirilmez. Ağ üzerinden iletilen verilerin hacmini azaltmak için ALT sorguda DISTINCT belirtin. (Bunu normal bir İN için yapmanız gerekmez.)
2.  Geçici tablo tüm uzak sunuculara gönderilir. İletim, ağ topolojisini hesaba katmaz. Örneğin, 10 uzak sunucu, istek sahibi sunucuya göre çok uzak bir veri merkezinde bulunuyorsa, veriler uzak veri merkezine kanal üzerinden 10 kez gönderilir. GLOBAL IN kullanırken büyük veri kümelerinden kaçınmaya çalışın.
3.  Uzak sunuculara veri iletirken, ağ bant genişliği üzerindeki kısıtlamalar yapılandırılabilir değildir. Şebekeyi aşırı yükleyebilirsiniz.
4.  Verileri sunucular arasında dağıtmaya çalışın, böylece GLOBAL IN'Yİ düzenli olarak kullanmanız gerekmez.
5.  GLOBAL IN sık sık kullanmanız gerekiyorsa, tek bir yinelemeler grubunun aralarında hızlı bir ağ bulunan birden fazla veri merkezinde bulunmasını sağlamak için ClickHouse kümesinin konumunu planlayın; böylece bir sorgu tamamen tek bir veri merkezi içinde işlenebilir.

Aynı zamanda yerel bir tablo belirtmek için mantıklı `GLOBAL IN` yan tümcesi, bu yerel tablo yalnızca istek sahibi sunucuda kullanılabilir ve verileri uzak sunucularda kullanmak istediğiniz durumda.

### Aşırı Değerler {#extreme-values}

Sonuçlara ek olarak, sonuçlar sütunları için minimum ve maksimum değerleri de alabilirsiniz. Bunu yapmak için, set **çıkmaz** ayar 1. Minimum ve maksimum değerler, sayısal türler, tarihler ve zamanlarla tarihler için hesaplanır. Diğer sütunlar için varsayılan değerler çıktıdır.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`, ve `Pretty*` [biçimliler](../../interfaces/formats.md), diğer satırlardan ayrı. Diğer formatlar için çıktı değildir.

İçinde `JSON*` biçimleri, aşırı değerler ayrı bir çıktı ‘extremes’ alan. İçinde `TabSeparated*` formatlar, satır ana sonuçtan sonra gelir ve sonra ‘totals’ varsa. Boş bir satırdan önce gelir (diğer verilerden sonra). İçinde `Pretty*` formatlar, satır ana sonuçtan sonra ayrı bir tablo olarak çıktılanır ve sonra `totals` varsa.

Aşırı değerler önce satırlar için hesaplanır `LIMIT` ama sonra `LIMIT BY`. Ancak, kullanırken `LIMIT offset, size`, önceki satırlar `offset` dahildir `extremes`. Akış isteklerinde, sonuç, içinden geçen az sayıda satır da içerebilir `LIMIT`.

### Not {#notes}

Bu `GROUP BY` ve `ORDER BY` yan tümceleri konumsal bağımsız değişkenleri desteklemez. Bu MySQL ile çelişir, ancak standart SQL ile uyumludur.
Mesela, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

Eşanlamlıları kullanabilirsiniz (`AS` diğer adlar) sorgunun herhangi bir bölümünde.

Bir ifade yerine bir sorgunun herhangi bir bölümüne Yıldız İşareti koyabilirsiniz. Sorgu analiz edildiğinde, Yıldız İşareti tüm tablo sütunlarının bir listesine genişletilir ( `MATERIALIZED` ve `ALIAS` sütun). Bir yıldız işareti kullanmanın haklı olduğu sadece birkaç durum vardır:

-   Bir tablo dökümü oluştururken.
-   Sistem tabloları gibi sadece birkaç sütun içeren tablolar için.
-   Bir tabloda hangi sütunların bulunduğu hakkında bilgi almak için. Bu durumda, set `LIMIT 1`. Ama kullanmak daha iyidir `DESC TABLE` sorgu.
-   Kullanarak az sayıda sütun üzerinde güçlü filtrasyon olduğunda `PREWHERE`.
-   Alt sorgularda (dış sorgu için gerekli olmayan sütunlar alt sorgulardan hariç tutulduğundan).

Diğer tüm durumlarda, yıldız işaretini kullanmanızı önermiyoruz, çünkü sadece avantajlar yerine sütunlu bir DBMS'NİN dezavantajlarını veriyor. Başka bir deyişle yıldız işaretini kullanmak önerilmez.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/select/) <!--hide-->
