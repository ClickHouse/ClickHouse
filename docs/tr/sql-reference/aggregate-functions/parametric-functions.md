---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: Parametrik
---

# Parametrik Agrega Fonksiyonları {#aggregate_functions_parametric}

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## çubuk {#histogram}

Uyarlanabilir bir histogram hesaplar. Kesin sonuçları garanti etmez.

``` sql
histogram(number_of_bins)(values)
```

İşlevleri kullanır [Bir Akış Paralel Karar Ağacı Algoritması](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). Histogram kutularının sınırları, yeni veriler bir işleve girdiğinde ayarlanır. Ortak durumda, kutu genişlikleri eşit değildir.

**Parametre**

`number_of_bins` — Upper limit for the number of bins in the histogram. The function automatically calculates the number of bins. It tries to reach the specified number of bins, but if it fails, it uses fewer bins.
`values` — [İfade](../syntax.md#syntax-expressions) giriş değerleri ile sonuçlanır.

**Döndürülen değerler**

-   [Dizi](../../sql-reference/data-types/array.md) -den [Demetler](../../sql-reference/data-types/tuple.md) aşağıdaki format oftan:

        ```
        [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
        ```

        - `lower` — Lower bound of the bin.
        - `upper` — Upper bound of the bin.
        - `height` — Calculated height of the bin.

**Örnek**

``` sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

Bir histogram ile görselleştirebilirsiniz [bar](../../sql-reference/functions/other-functions.md#function-bar) fonksiyon, örneğin:

``` sql
WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height,
    bar(height, 0, 6, 5) AS bar
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

Bu durumda, histogram kutusu kenarlıklarını bilmediğinizi unutmamalısınız.

## sequenceMatch(pattern)(timestamp, cond1, cond2, …) {#function-sequencematch}

Dizinin desenle eşleşen bir olay zinciri içerip içermediğini denetler.

``` sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

!!! warning "Uyarıcı"
    Aynı saniyede meydana gelen olaylar sonucu etkileyen tanımsız bir sırada sırayla yatıyordu.

**Parametre**

-   `pattern` — Pattern string. See [Desen sözdizimi](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` ve `DateTime`. Ayrıca desteklenen herhangi birini kullanabilirsiniz [Uİnt](../../sql-reference/data-types/int-uint.md) veri türleri.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. En fazla 32 koşul argümanını iletebilirsiniz. İşlev yalnızca bu koşullarda açıklanan olayları dikkate alır. Sıra, bir koşulda açıklanmayan veriler içeriyorsa, işlev bunları atlar.

**Döndürülen değerler**

-   1, Eğer desen eşleşti.
-   Desen eşleşmezse 0.

Tür: `UInt8`.

<a name="sequence-function-pattern-syntax"></a>
**Desen sözdizimi**

-   `(?N)` — Matches the condition argument at position `N`. Şartlar numaralandırılmıştır `[1, 32]` Aralık. Mesela, `(?1)` argü theman thela eşleş their `cond1` parametre.

-   `.*` — Matches any number of events. You don't need conditional arguments to match this element of the pattern.

-   `(?t operator value)` — Sets the time in seconds that should separate two events. For example, pattern `(?1)(?t>1800)(?2)` birbirinden 1800 saniyeden fazla meydana gelen olayları eşleşir. Bu olaylar arasında herhangi bir olayın keyfi bir sayısı olabilir. Kullanabilirsiniz `>=`, `>`, `<`, `<=` operatörler.

**Örnekler**

Verileri göz önünde bulundurun `t` Tablo:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

Sorguyu gerçekleştir:

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

İşlev, 2 numarasının 1 numarayı takip ettiği olay zincirini buldu. Sayı bir olay olarak tanımlanmadığı için aralarında 3 sayısını atladı. Örnekte verilen olay zincirini ararken bu numarayı dikkate almak istiyorsak, bunun için bir koşul oluşturmalıyız.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

Bu durumda, işlev desenle eşleşen olay zincirini bulamadı, çünkü 3 numaralı olay 1 ile 2 arasında gerçekleşti. Aynı durumda 4 numaralı koşulu kontrol edersek, sıra desenle eşleşir.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Ayrıca Bakınız**

-   [sequenceCount](#function-sequencecount)

## sequenceCount(pattern)(time, cond1, cond2, …) {#function-sequencecount}

Desenle eşleşen olay zincirlerinin sayısını sayar. İşlev, çakışmayan olay zincirlerini arar. Geçerli zincir eşleştirildikten sonra bir sonraki zinciri aramaya başlar.

!!! warning "Uyarıcı"
    Aynı saniyede meydana gelen olaylar sonucu etkileyen tanımsız bir sırada sırayla yatıyordu.

``` sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**Parametre**

-   `pattern` — Pattern string. See [Desen sözdizimi](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` ve `DateTime`. Ayrıca desteklenen herhangi birini kullanabilirsiniz [Uİnt](../../sql-reference/data-types/int-uint.md) veri türleri.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. En fazla 32 koşul argümanını iletebilirsiniz. İşlev yalnızca bu koşullarda açıklanan olayları dikkate alır. Sıra, bir koşulda açıklanmayan veriler içeriyorsa, işlev bunları atlar.

**Döndürülen değerler**

-   Eşleşen çakışmayan olay zincirlerinin sayısı.

Tür: `UInt64`.

**Örnek**

Verileri göz önünde bulundurun `t` Tablo:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

2 numara aralarında diğer sayıların herhangi bir miktarda 1 numaradan sonra meydana kaç kez Sayın:

``` sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

**Ayrıca Bakınız**

-   [sequenceMatch](#function-sequencematch)

## windowFunnel {#windowfunnel}

Kayan bir zaman penceresinde olay zincirlerini arar ve zincirden meydana gelen en fazla olay sayısını hesaplar.

Fonksiyon algoritmaya göre çalışır:

-   İşlev, zincirdeki ilk koşulu tetikleyen ve olay sayacını 1'e ayarlayan verileri arar. Sürgülü pencerenin başladığı an budur.

-   Zincirdeki olaylar pencerede sırayla gerçekleşirse, sayaç artırılır. Olayların sırası bozulursa, sayaç artırılmaz.

-   Verilerin çeşitli tamamlanma noktalarında birden çok olay zinciri varsa, işlev yalnızca en uzun zincirin boyutunu çıkarır.

**Sözdizimi**

``` sql
windowFunnel(window, [mode])(timestamp, cond1, cond2, ..., condN)
```

**Parametre**

-   `window` — Length of the sliding window in seconds.
-   `mode` - Bu isteğe bağlı bir argüman.
    -   `'strict'` - Zaman `'strict'` ayarlanırsa, windowFunnel () yalnızca benzersiz değerler için koşullar uygular.
-   `timestamp` — Name of the column containing the timestamp. Data types supported: [Tarihli](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) ve diğer imzasız tamsayı türleri (timestamp'ın `UInt64` yazın, değeri 2^63 - 1 olan Int64 maksimum değerini aşamaz).
-   `cond` — Conditions or data describing the chain of events. [Uİnt8](../../sql-reference/data-types/int-uint.md).

**Döndürülen değer**

Sürgülü zaman penceresi içindeki zincirden ardışık tetiklenen koşulların maksimum sayısı.
Seçimdeki tüm zincirler analiz edilir.

Tür: `Integer`.

**Örnek**

Kullanıcının bir telefon seçmesi ve çevrimiçi mağazada iki kez satın alması için belirli bir süre yeterli olup olmadığını belirleyin.

Aşağıdaki olaylar zincirini ayarlayın:

1.  Mağaz theadaki Hesabına giriş yapan kullanıcı (`eventID = 1003`).
2.  Kullanıcı bir telefon arar (`eventID = 1007, product = 'phone'`).
3.  Kullanıcı sipariş verdi (`eventID = 1009`).
4.  Kullanıcı tekrar sipariş yaptı (`eventID = 1010`).

Giriş tablosu:

``` text
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-28 │       1 │ 2019-01-29 10:00:00 │    1003 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-31 │       1 │ 2019-01-31 09:00:00 │    1007 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-30 │       1 │ 2019-01-30 08:00:00 │    1009 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-02-01 │       1 │ 2019-02-01 08:00:00 │    1010 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
```

Kullanıcının ne kadar uzakta olduğunu öğrenin `user_id` 2019 yılının Ocak-Şubat aylarında bir dönemde zincirden geçebilir.

Sorgu:

``` sql
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM trend
    WHERE (event_date >= '2019-01-01') AND (event_date <= '2019-02-02')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC
```

Sonuç:

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

## saklama {#retention}

İşlev, bağımsız değişken olarak 1'den 32'ye kadar bir dizi koşul türünü alır `UInt8` bu, etkinlik için belirli bir koşulun karşılanıp karşılanmadığını gösterir.
Herhangi bir koşul bir argüman olarak belirtilebilir (aşağıdaki gibi [WHERE](../../sql-reference/statements/select/where.md#select-where)).

İlk hariç, koşullar çiftler halinde geçerlidir: birinci ve ikinci doğruysa, ikincinin sonucu, birinci ve fird doğruysa, üçüncüsü doğru olacaktır.

**Sözdizimi**

``` sql
retention(cond1, cond2, ..., cond32);
```

**Parametre**

-   `cond` — an expression that returns a `UInt8` sonuç (1 veya 0).

**Döndürülen değer**

1 veya 0 dizisi.

-   1 — condition was met for the event.
-   0 — condition wasn't met for the event.

Tür: `UInt8`.

**Örnek**

Hesaplamanın bir örneğini düşünelim `retention` site trafiğini belirlemek için işlev.

**1.** Сreate a table to illustrate an example.

``` sql
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

Giriş tablosu:

Sorgu:

``` sql
SELECT * FROM retention_test
```

Sonuç:

``` text
┌───────date─┬─uid─┐
│ 2020-01-01 │   0 │
│ 2020-01-01 │   1 │
│ 2020-01-01 │   2 │
│ 2020-01-01 │   3 │
│ 2020-01-01 │   4 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-02 │   0 │
│ 2020-01-02 │   1 │
│ 2020-01-02 │   2 │
│ 2020-01-02 │   3 │
│ 2020-01-02 │   4 │
│ 2020-01-02 │   5 │
│ 2020-01-02 │   6 │
│ 2020-01-02 │   7 │
│ 2020-01-02 │   8 │
│ 2020-01-02 │   9 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-03 │   0 │
│ 2020-01-03 │   1 │
│ 2020-01-03 │   2 │
│ 2020-01-03 │   3 │
│ 2020-01-03 │   4 │
│ 2020-01-03 │   5 │
│ 2020-01-03 │   6 │
│ 2020-01-03 │   7 │
│ 2020-01-03 │   8 │
│ 2020-01-03 │   9 │
│ 2020-01-03 │  10 │
│ 2020-01-03 │  11 │
│ 2020-01-03 │  12 │
│ 2020-01-03 │  13 │
│ 2020-01-03 │  14 │
└────────────┴─────┘
```

**2.** Kullanıcıları benzersiz kimliğe göre grupla `uid` kullanarak `retention` İşlev.

Sorgu:

``` sql
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

Sonuç:

``` text
┌─uid─┬─r───────┐
│   0 │ [1,1,1] │
│   1 │ [1,1,1] │
│   2 │ [1,1,1] │
│   3 │ [1,1,1] │
│   4 │ [1,1,1] │
│   5 │ [0,0,0] │
│   6 │ [0,0,0] │
│   7 │ [0,0,0] │
│   8 │ [0,0,0] │
│   9 │ [0,0,0] │
│  10 │ [0,0,0] │
│  11 │ [0,0,0] │
│  12 │ [0,0,0] │
│  13 │ [0,0,0] │
│  14 │ [0,0,0] │
└─────┴─────────┘
```

**3.** Günde toplam site ziyaret sayısını hesaplayın.

Sorgu:

``` sql
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
```

Sonuç:

``` text
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

Nerede:

-   `r1`- 2020-01-01 sırasında siteyi ziyaret eden tekil ziyaretçi sayısı ( `cond1` koşul).
-   `r2`- 2020-01-01 ve 2020-01-02 arasında belirli bir süre boyunca siteyi ziyaret eden tekil ziyaretçi sayısı (`cond1` ve `cond2` şartlar).
-   `r3`- 2020-01-01 ve 2020-01-03 arasında belirli bir süre boyunca siteyi ziyaret eden tekil ziyaretçi sayısı (`cond1` ve `cond3` şartlar).

## uniqUpTo(N) (x) {#uniquptonx}

Calculates the number of different argument values ​​if it is less than or equal to N. If the number of different argument values is greater than N, it returns N + 1.

Küçük Ns ile kullanım için tavsiye, kadar 10. N'nin maksimum değeri 100'dür.

Bir toplama işlevinin durumu için, 1 + n \* bir bayt değerinin boyutuna eşit bellek miktarını kullanır.
Dizeler için, 8 baytlık kriptografik olmayan bir karma saklar. Yani, hesaplama dizeler için yaklaşık olarak hesaplanır.

İşlev ayrıca birkaç argüman için de çalışır.

Büyük bir N değeri kullanıldığında ve benzersiz değerlerin sayısı n'den biraz daha az olduğu durumlar dışında mümkün olduğunca hızlı çalışır.

Kullanım örneği:

``` text
Problem: Generate a report that shows only keywords that produced at least 5 unique users.
Solution: Write in the GROUP BY query SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/agg_functions/parametric_functions/) <!--hide-->

## sumMapFiltered (keys\_to\_keep) (anahtarlar, değerler) {#summapfilteredkeys-to-keepkeys-values}

Aynı davranış [sumMap](reference.md#agg_functions-summap) dışında bir dizi anahtar parametre olarak geçirilir. Bu, özellikle yüksek bir Anahtarlık ile çalışırken yararlı olabilir.
