---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
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

### Fık WİTHRA Ile {#with-clause}

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
Bu `system.one` tablo tam olarak bir satır içerir (bu tablo diğer Dbms’lerde bulunan çift tablo ile aynı amacı yerine getirir).

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
| `SAMPLE k`           | Burada `k` 0’dan 1’e kadar olan sayıdır.</br>Sorgu üzerinde yürütülür `k` verilerin kesir. Mesela, `SAMPLE 0.1` sorguyu verilerin %10’unda çalıştırır. [Daha fazla bilgi edinin](#select-sample-k)                                                                   |
| `SAMPLE n`           | Burada `n` yeterince büyük bir tamsayıdır.</br>Sorgu en az bir örnek üzerinde yürütülür `n` satırlar (ancak bundan önemli ölçüde daha fazla değil). Mesela, `SAMPLE 10000000` sorguyu en az 10.000.000 satır çalıştırır. [Daha fazla bilgi edinin](#select-sample-n) |
| `SAMPLE k OFFSET m`  | Burada `k` ve `m` 0’dan 1’e kadar olan sayılardır.</br>Sorgu bir örnek üzerinde yürütülür `k` verilerin kesir. Örnek için kullanılan veriler, `m` bölme. [Daha fazla bilgi edinin](#select-sample-offset)                                                            |

#### SAMPLE K {#select-sample-k}

Burada `k` 0’dan 1’e kadar olan sayıdır (hem kesirli hem de ondalık gösterimler desteklenir). Mesela, `SAMPLE 1/2` veya `SAMPLE 0.5`.

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

Kullanırken `SAMPLE n` yan tümce, verilerin hangi göreli yüzde işlendiğini bilmiyorsunuz. Yani toplam fonksiyonların çarpılması gereken katsayıyı bilmiyorsunuz. Kullan… `_sample_factor` sanal sütun yaklaşık sonucu almak için.

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

Burada `k` ve `m` 0’dan 1’e kadar olan sayılardır. Örnekler aşağıda gösterilmiştir.

**Örnek 1**

``` sql
SAMPLE 1/10
```

Bu örnekte, örnek tüm verilerin 1/10’udur:

`[++------------]`

**Örnek 2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

Burada, verilerin ikinci yarısından %10’luk bir örnek alınır.

`[------++------]`

### Dizi Jo JOİNİN Yan tüm Clausecesi {#select-array-join-clause}

Yürüt allowsmeyi sağlar `JOIN` bir dizi veya iç içe veri yapısı ile. Niyet benzer [arrayJoin](../../sql-reference/functions/array-join.md#functions_arrayjoin) işlev, ancak işlevselliği daha geniştir.

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

#### İç içe Veri yapısı Ile Dizi birleştirme {#array-join-with-nested-data-structure}

`ARRAY`Jo “in” ile de çalışır [iç içe veri yapıları](../../sql-reference/data-types/nested-data-structures/nested.md). Örnek:

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

#### Desteklenen Türleri `JOIN` {#select-join-types}

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

\`\`\` Metin
table\_1 table\_2

olay / ev\_time / user\_id olay / ev\_time / user\_id
