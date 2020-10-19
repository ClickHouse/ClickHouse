---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 66
toc_title: "Di\u011Fer"
---

# Diğer Fonksiyonlar {#other-functions}

## hostnamename() {#hostname}

Bu işlevin gerçekleştirildiği ana bilgisayarın adını içeren bir dize döndürür. Dağıtılmış işlem için, bu işlev uzak bir sunucuda gerçekleştirilirse, uzak sunucu ana bilgisayarının adıdır.

## getMacro {#getmacro}

Get as a nam AED value from the [makrolar](../../operations/server-configuration-parameters/settings.md#macros) sunucu yapılandırması bölümü.

**Sözdizimi**

``` sql
getMacro(name);
```

**Parametre**

-   `name` — Name to retrieve from the `macros` bölme. [Dize](../../sql-reference/data-types/string.md#string).

**Döndürülen değer**

-   Belirtilen makro değeri.

Tür: [Dize](../../sql-reference/data-types/string.md).

**Örnek**

Örnek `macros` sunucu yapılandırma dosyasındaki bölüm:

``` xml
<macros>
    <test>Value</test>
</macros>
```

Sorgu:

``` sql
SELECT getMacro('test');
```

Sonuç:

``` text
┌─getMacro('test')─┐
│ Value            │
└──────────────────┘
```

Aynı değeri elde etmenin alternatif bir yolu:

``` sql
SELECT * FROM system.macros
WHERE macro = 'test';
```

``` text
┌─macro─┬─substitution─┐
│ test  │ Value        │
└───────┴──────────────┘
```

## FQDN {#fqdn}

Tam etki alanı adını döndürür.

**Sözdizimi**

``` sql
fqdn();
```

Bu işlev büyük / küçük harf duyarsızdır.

**Döndürülen değer**

-   Tam etki alanı adı ile dize.

Tür: `String`.

**Örnek**

Sorgu:

``` sql
SELECT FQDN();
```

Sonuç:

``` text
┌─FQDN()──────────────────────────┐
│ clickhouse.ru-central1.internal │
└─────────────────────────────────┘
```

## basename {#basename}

Son eğik çizgi veya ters eğik çizgiden sonra bir dizenin sondaki kısmını ayıklar. Bu işlev, genellikle bir yoldan dosya adını ayıklamak için kullanılır.

``` sql
basename( expr )
```

**Parametre**

-   `expr` — Expression resulting in a [Dize](../../sql-reference/data-types/string.md) type value. Tüm ters eğik çizgilerin ortaya çıkan değerden kaçması gerekir.

**Döndürülen Değer**

İçeren bir dize:

-   Son eğik çizgi veya ters eğik çizgiden sonra bir dizenin sondaki kısmı.

        If the input string contains a path ending with slash or backslash, for example, `/` or `c:\`, the function returns an empty string.

-   Eğik çizgi veya ters eğik çizgi yoksa orijinal dize.

**Örnek**

``` sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some-file-name' AS a, basename(a)
```

``` text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## visibleWidth(x) {#visiblewidthx}

Değerleri konsola metin biçiminde (sekmeyle ayrılmış) çıkarırken yaklaşık genişliği hesaplar.
Bu işlev, sistem tarafından güzel formatların uygulanması için kullanılır.

`NULL` karşılık gelen bir dize olarak temsil edilir `NULL` içinde `Pretty` biçimliler.

``` sql
SELECT visibleWidth(NULL)
```

``` text
┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## toTypeName (x) {#totypenamex}

Geçirilen bağımsız değişken türü adını içeren bir dize döndürür.

Eğer `NULL` fonksiyona girdi olarak geçirilir, daha sonra `Nullable(Nothing)` bir iç karşılık gelen türü `NULL` Clickhouse'da temsil.

## blockSize() {#function-blocksize}

Bloğun boyutunu alır.
Clickhouse'da, sorgular her zaman bloklarda (sütun parçaları kümeleri) çalıştırılır. Bu işlev, aradığınız bloğun boyutunu almanızı sağlar.

## materialize (x) {#materializex}

Bir sabiti yalnızca bir değer içeren tam bir sütuna dönüştürür.
Clickhouse'da, tam sütunlar ve sabitler bellekte farklı şekilde temsil edilir. İşlevler, sabit argümanlar ve normal argümanlar için farklı şekilde çalışır (farklı kod yürütülür), ancak sonuç hemen hemen her zaman aynıdır. Bu işlev, bu davranış hata ayıklama içindir.

## ignore(…) {#ignore}

Dahil olmak üzere herhangi bir argümanı kabul eder `NULL`. Her zaman 0 döndürür.
Ancak, argüman hala değerlendirilir. Bu kriterler için kullanılabilir.

## uyku (saniye) {#sleepseconds}

Uykular ‘seconds’ her veri bloğunda saniye. Bir tamsayı veya kayan noktalı sayı belirtebilirsiniz.

## sleepEachRow (saniye) {#sleepeachrowseconds}

Uykular ‘seconds’ her satırda saniye. Bir tamsayı veya kayan noktalı sayı belirtebilirsiniz.

## currentDatabase() {#currentdatabase}

Geçerli veritabanının adını döndürür.
Bu işlevi, veritabanını belirtmeniz gereken bir tablo oluştur sorgusunda tablo altyapısı parametrelerinde kullanabilirsiniz.

## currentUser() {#other-function-currentuser}

Geçerli kullanıcının oturum açma döndürür. Kullanıcı girişi, bu başlatılan sorgu, durumda distibuted sorguda iade edilecektir.

``` sql
SELECT currentUser();
```

Takma ad: `user()`, `USER()`.

**Döndürülen değerler**

-   Geçerli kullanıcının girişi.
-   Disributed sorgu durumunda sorgu başlatılan kullanıcının giriş.

Tür: `String`.

**Örnek**

Sorgu:

``` sql
SELECT currentUser();
```

Sonuç:

``` text
┌─currentUser()─┐
│ default       │
└───────────────┘
```

## ısconstant {#is-constant}

Bağımsız değişken sabit bir ifade olup olmadığını denetler.

A constant expression means an expression whose resulting value is known at the query analysis (i.e. before execution). For example, expressions over [harfler](../syntax.md#literals) sabit ifadelerdir.

Fonksiyon geliştirme, hata ayıklama ve gösteri için tasarlanmıştır.

**Sözdizimi**

``` sql
isConstant(x)
```

**Parametre**

-   `x` — Expression to check.

**Döndürülen değerler**

-   `1` — `x` sabit istir.
-   `0` — `x` sabit olmayan.

Tür: [Uİnt8](../data-types/int-uint.md).

**Örnekler**

Sorgu:

``` sql
SELECT isConstant(x + 1) FROM (SELECT 43 AS x)
```

Sonuç:

``` text
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
```

Sorgu:

``` sql
WITH 3.14 AS pi SELECT isConstant(cos(pi))
```

Sonuç:

``` text
┌─isConstant(cos(pi))─┐
│                   1 │
└─────────────────────┘
```

Sorgu:

``` sql
SELECT isConstant(number) FROM numbers(1)
```

Sonuç:

``` text
┌─isConstant(number)─┐
│                  0 │
└────────────────────┘
```

## isFinite (x) {#isfinitex}

Float32 ve Float64 kabul eder ve bağımsız değişken sonsuz değilse ve bir NaN değilse, Uint8'i 1'e eşit olarak döndürür, aksi halde 0.

## isİnfinite (x) {#isinfinitex}

Float32 ve Float64 kabul eder ve bağımsız değişken sonsuz ise 1'e eşit Uİnt8 döndürür, aksi takdirde 0. Bir NaN için 0 döndürüldüğünü unutmayın.

## ifNotFinite {#ifnotfinite}

Kayan nokta değerinin sonlu olup olmadığını kontrol eder.

**Sözdizimi**

    ifNotFinite(x,y)

**Parametre**

-   `x` — Value to be checked for infinity. Type: [Yüzdürmek\*](../../sql-reference/data-types/float.md).
-   `y` — Fallback value. Type: [Yüzdürmek\*](../../sql-reference/data-types/float.md).

**Döndürülen değer**

-   `x` eğer `x` son isludur.
-   `y` eğer `x` sonlu değildir.

**Örnek**

Sorgu:

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

Sonuç:

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

Kullanarak benzer sonuç alabilirsiniz [üçlü operatör](conditional-functions.md#ternary-operator): `isFinite(x) ? x : y`.

## ısnan (x) {#isnanx}

Float32 ve Float64 kabul eder ve bağımsız değişken bir NaN, aksi takdirde 0 ise 1'e eşit uint8 döndürür.

## hasColumnİnTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’) {#hascolumnintablehostname-username-password-database-table-column}

Sabit dizeleri kabul eder: veritabanı adı, tablo adı ve sütun adı. Bir sütun varsa 1'e eşit bir uint8 sabit ifadesi döndürür, aksi halde 0. Hostname parametresi ayarlanmışsa, sınama uzak bir sunucuda çalışır.
Tablo yoksa, işlev bir özel durum atar.
İç içe veri yapısındaki öğeler için işlev, bir sütunun varlığını denetler. İç içe veri yapısının kendisi için işlev 0 döndürür.

## bar {#function-bar}

Unicode-art diyagramı oluşturmaya izin verir.

`bar(x, min, max, width)` genişliği orantılı olan bir bant çizer `(x - min)` ve eşit `width` karakterler ne zaman `x = max`.

Parametre:

-   `x` — Size to display.
-   `min, max` — Integer constants. The value must fit in `Int64`.
-   `width` — Constant, positive integer, can be fractional.

Bant, bir sembolün sekizde birine doğrulukla çizilir.

Örnek:

``` sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

``` text
┌──h─┬──────c─┬─bar────────────────┐
│  0 │ 292907 │ █████████▋         │
│  1 │ 180563 │ ██████             │
│  2 │ 114861 │ ███▋               │
│  3 │  85069 │ ██▋                │
│  4 │  68543 │ ██▎                │
│  5 │  78116 │ ██▌                │
│  6 │ 113474 │ ███▋               │
│  7 │ 170678 │ █████▋             │
│  8 │ 278380 │ █████████▎         │
│  9 │ 391053 │ █████████████      │
│ 10 │ 457681 │ ███████████████▎   │
│ 11 │ 493667 │ ████████████████▍  │
│ 12 │ 509641 │ ████████████████▊  │
│ 13 │ 522947 │ █████████████████▍ │
│ 14 │ 539954 │ █████████████████▊ │
│ 15 │ 528460 │ █████████████████▌ │
│ 16 │ 539201 │ █████████████████▊ │
│ 17 │ 523539 │ █████████████████▍ │
│ 18 │ 506467 │ ████████████████▊  │
│ 19 │ 520915 │ █████████████████▎ │
│ 20 │ 521665 │ █████████████████▍ │
│ 21 │ 542078 │ ██████████████████ │
│ 22 │ 493642 │ ████████████████▍  │
│ 23 │ 400397 │ █████████████▎     │
└────┴────────┴────────────────────┘
```

## dönüştürmek {#transform}

Bir değeri, bazı öğelerin açıkça tanımlanmış eşlemesine göre diğer öğelere dönüştürür.
Bu fonksiyonun iki varyasyonu vardır:

### transform (x, array\_from, array\_to, varsayılan) {#transformx-array-from-array-to-default}

`x` – What to transform.

`array_from` – Constant array of values for converting.

`array_to` – Constant array of values to convert the values in ‘from’ -e doğru.

`default` – Which value to use if ‘x’ değer anylerden hiçbir equaline eşit değildir. ‘from’.

`array_from` ve `array_to` – Arrays of the same size.

Türler:

`transform(T, Array(T), Array(U), U) -> U`

`T` ve `U` sayısal, dize veya tarih veya DateTime türleri olabilir.
Aynı harfin belirtildiği (t veya U), sayısal türler için bunlar eşleşen türler değil, ortak bir türe sahip türler olabilir.
Örneğin, ilk bağımsız değişken Int64 türüne sahip olabilir, ikincisi ise Array(Uİnt16) türüne sahiptir.

Eğer... ‘x’ değer, içindeki öğelerden birine eşittir. ‘array\_from’ array, varolan öğeyi döndürür (aynı numaralandırılır) ‘array\_to’ dizi. Aksi takdirde, döner ‘default’. İçinde birden fazla eşleşen öğe varsa ‘array\_from’, maçlardan birini döndürür.

Örnek:

``` sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

``` text
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

### transform (x, array\_from, array\_to) {#transformx-array-from-array-to}

İlk vary thatasyon differsdan farklıdır. ‘default’ argüman atlandı.
Eğer... ‘x’ değer, içindeki öğelerden birine eşittir. ‘array\_from’ array, eşleşen öğeyi (aynı numaralandırılmış) döndürür ‘array\_to’ dizi. Aksi takdirde, döner ‘x’.

Türler:

`transform(T, Array(T), Array(T)) -> T`

Örnek:

``` sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vk.com'], ['www.yandex', 'example.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

``` text
┌─s──────────────┬───────c─┐
│                │ 2906259 │
│ www.yandex     │  867767 │
│ ███████.ru     │  313599 │
│ mail.yandex.ru │  107147 │
│ ██████.ru      │  100355 │
│ █████████.ru   │   65040 │
│ news.yandex.ru │   64515 │
│ ██████.net     │   59141 │
│ example.com    │   57316 │
└────────────────┴─────────┘
```

## formatReadableSize (x) {#formatreadablesizex}

Boyutu (bayt sayısı) kabul eder. Bir sonek (KiB, MıB, vb.) ile yuvarlak bir boyut döndürür.) bir dize olarak.

Örnek:

``` sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

``` text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## en az (a, b) {#leasta-b}

A ve B'den en küçük değeri döndürür.

## en büyük (a, b) {#greatesta-b}

A ve B'nin en büyük değerini döndürür.

## çalışma süresi() {#uptime}

Sunucunun çalışma süresini saniyeler içinde döndürür.

## sürüm() {#version}

Sunucu sürümünü bir dize olarak döndürür.

## saat dilimi() {#timezone}

Sunucunun saat dilimini döndürür.

## blockNumber {#blocknumber}

Satırın bulunduğu veri bloğunun sıra numarasını döndürür.

## rowNumberİnBlock {#function-rownumberinblock}

Veri bloğundaki satırın sıra numarasını döndürür. Farklı veri blokları her zaman yeniden hesaplanır.

## rownumberınallblocks() {#rownumberinallblocks}

Veri bloğundaki satırın sıra numarasını döndürür. Bu işlev yalnızca etkilenen veri bloklarını dikkate alır.

## komşuluk {#neighbor}

Belirli bir sütunun geçerli satırından önce veya sonra gelen belirli bir ofsette bir satıra erişim sağlayan pencere işlevi.

**Sözdizimi**

``` sql
neighbor(column, offset[, default_value])
```

İşlevin sonucu, etkilenen veri bloklarına ve bloktaki veri sırasına bağlıdır.
ORDER BY ile bir alt sorgu yaparsanız ve alt sorgunun dışından işlevi çağırırsanız, beklenen sonucu alabilirsiniz.

**Parametre**

-   `column` — A column name or scalar expression.
-   `offset` — The number of rows forwards or backwards from the current row of `column`. [Int64](../../sql-reference/data-types/int-uint.md).
-   `default_value` — Optional. The value to be returned if offset goes beyond the scope of the block. Type of data blocks affected.

**Döndürülen değerler**

-   İçin değer `column` içinde `offset` eğer geçerli satırdan uzaklık `offset` değer blok sınırları dışında değil.
-   İçin varsayılan değer `column` eğer `offset` değer, blok sınırlarının dışındadır. Eğer `default_value` verilir, daha sonra kullanılacaktır.

Tür: etkilenen veri bloklarının türü veya varsayılan değer türü.

**Örnek**

Sorgu:

``` sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

Sonuç:

``` text
┌─number─┬─neighbor(number, 2)─┐
│      0 │                   2 │
│      1 │                   3 │
│      2 │                   4 │
│      3 │                   5 │
│      4 │                   6 │
│      5 │                   7 │
│      6 │                   8 │
│      7 │                   9 │
│      8 │                   0 │
│      9 │                   0 │
└────────┴─────────────────────┘
```

Sorgu:

``` sql
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
```

Sonuç:

``` text
┌─number─┬─neighbor(number, 2, 999)─┐
│      0 │                        2 │
│      1 │                        3 │
│      2 │                        4 │
│      3 │                        5 │
│      4 │                        6 │
│      5 │                        7 │
│      6 │                        8 │
│      7 │                        9 │
│      8 │                      999 │
│      9 │                      999 │
└────────┴──────────────────────────┘
```

Bu işlev, yıldan yıla metrik değeri hesaplamak için kullanılabilir:

Sorgu:

``` sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

Sonuç:

``` text
┌──────month─┬─money─┬─prev_year─┬─year_over_year─┐
│ 2018-01-01 │    32 │         0 │              0 │
│ 2018-02-01 │    63 │         0 │              0 │
│ 2018-03-01 │    91 │         0 │              0 │
│ 2018-04-01 │    22 │         0 │              0 │
│ 2018-05-01 │    52 │         0 │              0 │
│ 2018-06-01 │    83 │         0 │              0 │
│ 2018-07-01 │    13 │         0 │              0 │
│ 2018-08-01 │    44 │         0 │              0 │
│ 2018-09-01 │    75 │         0 │              0 │
│ 2018-10-01 │     5 │         0 │              0 │
│ 2018-11-01 │    36 │         0 │              0 │
│ 2018-12-01 │    66 │         0 │              0 │
│ 2019-01-01 │    97 │        32 │           0.33 │
│ 2019-02-01 │    28 │        63 │           2.25 │
│ 2019-03-01 │    56 │        91 │           1.62 │
│ 2019-04-01 │    87 │        22 │           0.25 │
└────────────┴───────┴───────────┴────────────────┘
```

## runningDifference (x) {#other_functions-runningdifference}

Calculates the difference between successive row values ​​in the data block.
İlk satır için 0 ve sonraki her satır için önceki satırdan farkı döndürür.

İşlevin sonucu, etkilenen veri bloklarına ve bloktaki veri sırasına bağlıdır.
ORDER BY ile bir alt sorgu yaparsanız ve alt sorgunun dışından işlevi çağırırsanız, beklenen sonucu alabilirsiniz.

Örnek:

``` sql
SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2016-11-24'
    ORDER BY EventTime ASC
    LIMIT 5
)
```

``` text
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2016-11-24 00:00:04 │     0 │
│    1107 │ 2016-11-24 00:00:05 │     1 │
│    1108 │ 2016-11-24 00:00:05 │     0 │
│    1109 │ 2016-11-24 00:00:09 │     4 │
│    1110 │ 2016-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
```

Lütfen dikkat - blok boyutu sonucu etkiler. Her yeni blok ile, `runningDifference` durum sıfırlandı.

``` sql
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
```

``` sql
set max_block_size=100000 -- default value is 65536!

SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## runningDifferenceStartingWithFirstvalue {#runningdifferencestartingwithfirstvalue}

İçin aynı [runningDifference](./other-functions.md#other_functions-runningdifference), fark ilk satırın değeridir, ilk satırın değerini döndürdü ve sonraki her satır önceki satırdan farkı döndürür.

## MACNumToString (num) {#macnumtostringnum}

Bir uınt64 numarasını kabul eder. Big endian'da bir MAC adresi olarak yorumlar. AA:BB:CC:DD:EE:FF biçiminde karşılık gelen MAC adresini içeren bir dize döndürür (onaltılık formda iki nokta üst üste ayrılmış sayılar).

## MACStringToNum (s) {#macstringtonums}

MACNumToString ters işlevi. MAC adresi geçersiz bir biçime sahipse, 0 döndürür.

## MACStringToOUİ (s) {#macstringtoouis}

AA:BB:CC:DD:EE:FF (onaltılık formda iki nokta üst üste ayrılmış sayılar) biçiminde bir MAC adresi kabul eder. İlk üç sekizli uint64 numarası olarak döndürür. MAC adresi geçersiz bir biçime sahipse, 0 döndürür.

## getSizeOfEnumType {#getsizeofenumtype}

Alan sayısını döndürür [Enum](../../sql-reference/data-types/enum.md).

``` sql
getSizeOfEnumType(value)
```

**Parametre:**

-   `value` — Value of type `Enum`.

**Döndürülen değerler**

-   İle alan sayısı `Enum` giriş değerleri.
-   Tür değilse bir istisna atılır `Enum`.

**Örnek**

``` sql
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## blockSerializedSize {#blockserializedsize}

Diskteki boyutu döndürür (sıkıştırmayı hesaba katmadan).

``` sql
blockSerializedSize(value[, value[, ...]])
```

**Parametre:**

-   `value` — Any value.

**Döndürülen değerler**

-   (Sıkıştırma olmadan) değerler bloğu için diske yazılacak bayt sayısı.

**Örnek**

``` sql
SELECT blockSerializedSize(maxState(1)) as x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## toColumnTypeName {#tocolumntypename}

RAM'DEKİ sütunun veri türünü temsil eden sınıfın adını döndürür.

``` sql
toColumnTypeName(value)
```

**Parametre:**

-   `value` — Any type of value.

**Döndürülen değerler**

-   Temsil etmek için kullanılan sınıfın adını içeren bir dize `value` RAM veri türü.

**Arasındaki fark örneği`toTypeName ' and ' toColumnTypeName`**

``` sql
SELECT toTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

``` sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

Örnek gösteriyor ki `DateTime` veri türü olarak bellekte saklanır `Const(UInt32)`.

## dumpColumnStructure {#dumpcolumnstructure}

Ram'deki veri yapılarının ayrıntılı bir açıklamasını verir

``` sql
dumpColumnStructure(value)
```

**Parametre:**

-   `value` — Any type of value.

**Döndürülen değerler**

-   Temsil etmek için kullanılan yapıyı açıklayan bir dize `value` RAM veri türü.

**Örnek**

``` sql
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))
```

``` text
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## defaultValueOfArgumentType {#defaultvalueofargumenttype}

Veri türü için varsayılan değeri verir.

Kullanıcı tarafından ayarlanan özel sütunlar için varsayılan değerleri içermez.

``` sql
defaultValueOfArgumentType(expression)
```

**Parametre:**

-   `expression` — Arbitrary type of value or an expression that results in a value of an arbitrary type.

**Döndürülen değerler**

-   `0` sayılar için.
-   Dizeler için boş dize.
-   `ᴺᵁᴸᴸ` için [Nullable](../../sql-reference/data-types/nullable.md).

**Örnek**

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## çoğaltmak {#other-functions-replicate}

Tek bir değere sahip bir dizi oluşturur.

İç uygulama için kullanılan [arrayJoin](array-join.md#functions_arrayjoin).

``` sql
SELECT replicate(x, arr);
```

**Parametre:**

-   `arr` — Original array. ClickHouse creates a new array of the same length as the original and fills it with the value `x`.
-   `x` — The value that the resulting array will be filled with.

**Döndürülen değer**

Değerle dolu bir dizi `x`.

Tür: `Array`.

**Örnek**

Sorgu:

``` sql
SELECT replicate(1, ['a', 'b', 'c'])
```

Sonuç:

``` text
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## filesystemAvailable {#filesystemavailable}

Veritabanlarının dosyalarının bulunduğu dosya sisteminde kalan alan miktarını döndürür. Her zaman toplam boş alandan daha küçüktür ([filesystemFree](#filesystemfree)) çünkü OS için biraz alan ayrılmıştır.

**Sözdizimi**

``` sql
filesystemAvailable()
```

**Döndürülen değer**

-   Bayt olarak kullanılabilir kalan alan miktarı.

Tür: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT formatReadableSize(filesystemAvailable()) AS "Available space", toTypeName(filesystemAvailable()) AS "Type";
```

Sonuç:

``` text
┌─Available space─┬─Type───┐
│ 30.75 GiB       │ UInt64 │
└─────────────────┴────────┘
```

## filesystemFree {#filesystemfree}

Veritabanlarının dosyalarının bulunduğu dosya sistemindeki boş alanın toplam miktarını döndürür. Ayrıca bakınız `filesystemAvailable`

**Sözdizimi**

``` sql
filesystemFree()
```

**Döndürülen değer**

-   Bayt cinsinden boş alan miktarı.

Tür: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT formatReadableSize(filesystemFree()) AS "Free space", toTypeName(filesystemFree()) AS "Type";
```

Sonuç:

``` text
┌─Free space─┬─Type───┐
│ 32.39 GiB  │ UInt64 │
└────────────┴────────┘
```

## filesystemCapacity {#filesystemcapacity}

Dosya sisteminin kapasitesini bayt cinsinden döndürür. Değerlendirme için, [yol](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) veri dizinine yapılandırılmalıdır.

**Sözdizimi**

``` sql
filesystemCapacity()
```

**Döndürülen değer**

-   Dosya sisteminin bayt cinsinden kapasite bilgisi.

Tür: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity", toTypeName(filesystemCapacity()) AS "Type"
```

Sonuç:

``` text
┌─Capacity──┬─Type───┐
│ 39.32 GiB │ UInt64 │
└───────────┴────────┘
```

## finalizeAggregation {#function-finalizeaggregation}

Toplama işlevinin durumunu alır. Toplama sonucunu döndürür (kesinleşmiş durum).

## runningAccumulate {#function-runningaccumulate}

Toplama işlevinin durumlarını alır ve değerleri olan bir sütun döndürür, bu durumların bir dizi blok satırı için ilk satırdan geçerli satıra birikmesinin sonucudur.
Örneğin, toplama işlevinin durumunu alır (örnek runningAccumulate (uniqState (Userıd))) ve her blok satırı için, önceki tüm Satırların ve geçerli satırın durumlarının birleştirilmesinde toplama işlevinin sonucunu döndürür.
Bu nedenle, işlevin sonucu, verilerin bloklara bölünmesine ve blok içindeki verilerin sırasına bağlıdır.

## joinGet {#joinget}

İşlev, tablodan verileri bir tablodan aynı şekilde ayıklamanızı sağlar [sözlük](../../sql-reference/dictionaries/index.md).

Veri alır [Katmak](../../engines/table-engines/special/join.md#creating-a-table) belirtilen birleştirme anahtarını kullanarak tablolar.

Sadece ile oluşturulan tabloları destekler `ENGINE = Join(ANY, LEFT, <join_keys>)` deyim.

**Sözdizimi**

``` sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**Parametre**

-   `join_storage_table_name` — an [tanıtıcı](../syntax.md#syntax-identifiers) aramanın nerede yapıldığını gösterir. Tanımlayıcı varsayılan veritabanında aranır (bkz. parametre `default_database` config dosyası). Varsayılan veritabanını geçersiz kılmak için `USE db_name` veya ayırıcı aracılığıyla veritabanını ve tabloyu belirtin `db_name.db_table` örnek bakın.
-   `value_column` — name of the column of the table that contains required data.
-   `join_keys` — list of keys.

**Döndürülen değer**

Anahtarların listesine karşılık gelen değerlerin listesini döndürür.

Kaynak tabloda kesin yoksa o zaman `0` veya `null` esas alınarak iade edilecektir [join\_use\_nulls](../../operations/settings/settings.md#join_use_nulls) ayar.

Hakkında daha fazla bilgi `join_use_nulls` içinde [Birleştirme işlemi](../../engines/table-engines/special/join.md).

**Örnek**

Giriş tablosu:

``` sql
CREATE DATABASE db_test
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 1
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13)
```

``` text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

Sorgu:

``` sql
SELECT joinGet(db_test.id_val,'val',toUInt32(number)) from numbers(4) SETTINGS join_use_nulls = 1
```

Sonuç:

``` text
┌─joinGet(db_test.id_val, 'val', toUInt32(number))─┐
│                                                0 │
│                                               11 │
│                                               12 │
│                                                0 │
└──────────────────────────────────────────────────┘
```

## modelEvaluate(model\_name, …) {#function-modelevaluate}

Dış modeli değerlendirin.
Bir model adı ve model bağımsız değişkenleri kabul eder. Float64 Döndürür.

## throwİf(x \[, custom\_message\]) {#throwifx-custom-message}

Argüman sıfır değilse bir istisna atın.
custom\_message-isteğe bağlı bir parametredir: sabit bir dize, bir hata mesajı sağlar

``` sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

``` text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## kimlik {#identity}

Bağımsız değişkeni olarak kullanılan aynı değeri döndürür. Hata ayıklama ve test için kullanılan, dizin kullanarak iptal ve tam bir tarama sorgu performansını almak için izin verir. Olası dizin kullanımı için sorgu analiz edildiğinde, analizör içeriye bakmaz `identity` işlevler.

**Sözdizimi**

``` sql
identity(x)
```

**Örnek**

Sorgu:

``` sql
SELECT identity(42)
```

Sonuç:

``` text
┌─identity(42)─┐
│           42 │
└──────────────┘
```

## randomPrintableASCİİ {#randomascii}

Rastgele bir dizi ile bir dize oluşturur [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) yazdırılabilir karakterler.

**Sözdizimi**

``` sql
randomPrintableASCII(length)
```

**Parametre**

-   `length` — Resulting string length. Positive integer.

        If you pass `length < 0`, behavior of the function is undefined.

**Döndürülen değer**

-   Rastgele bir dizi dize [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) yazdırılabilir karakterler.

Tür: [Dize](../../sql-reference/data-types/string.md)

**Örnek**

``` sql
SELECT number, randomPrintableASCII(30) as str, length(str) FROM system.numbers LIMIT 3
```

``` text
┌─number─┬─str────────────────────────────┬─length(randomPrintableASCII(30))─┐
│      0 │ SuiCOSTvC0csfABSw=UcSzp2.`rv8x │                               30 │
│      1 │ 1Ag NlJ &RCN:*>HVPG;PE-nO"SUFD │                               30 │
│      2 │ /"+<"wUTh:=LjJ Vm!c&hI*m#XTfzz │                               30 │
└────────┴────────────────────────────────┴──────────────────────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/other_functions/) <!--hide-->
