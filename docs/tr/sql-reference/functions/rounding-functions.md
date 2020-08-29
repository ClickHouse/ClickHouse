---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "D\xF6nm\xFC\u015F"
---

# Yuvarlama Fonksiyonları {#rounding-functions}

## kat(x \[, N\]) {#floorx-n}

Küçük veya eşit olan en büyük yuvarlak sayıyı döndürür `x`. Yuvarlak bir sayı, 1/10N'NİN katları veya 1 / 10N tam değilse, uygun veri türünün en yakın sayısıdır.
‘N’ bir tamsayı sabiti, isteğe bağlı parametredir. Varsayılan olarak sıfırdır, bu da bir tam sayıya yuvarlamak anlamına gelir.
‘N’ negatif olabilir.

Örnekler: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x` herhangi bir sayısal türüdür. Sonuç aynı türden bir sayıdır.
Tamsayı argümanları için, bir negatif ile yuvarlamak mantıklıdır `N` değer (negatif olmayan için `N`, işlev hiçbir şey yapmaz).
Yuvarlama taşmasına neden olursa (örneğin, floor (-128, -1)), uygulamaya özgü bir sonuç döndürülür.

## tavan(x \[, N\]), tavan (x \[, N\]) {#ceilx-n-ceilingx-n}

Büyük veya eşit olan en küçük yuvarlak sayıyı döndürür `x`. Diğer her şekilde, aynı `floor` (yukarıda) işlevi.

## trunc(x \[, N\]), truncate(x \[, N\]) {#truncx-n-truncatex-n}

Mutlak değeri küçük veya eşit olan en büyük mutlak değere sahip yuvarlak sayıyı döndürür `x`‘s. In every other way, it is the same as the ’floor’ (yukarıda) işlevi.

## Yuvarlak(x \[, N\]) {#rounding_functions-round}

Belirtilen sayıda ondalık basamak için bir değer yuvarlar.

İşlev, belirtilen siparişin en yakın numarasını döndürür. Verilen sayı çevreleyen sayılara eşit mesafeye sahip olduğunda, işlev, float sayı türleri için bankacının yuvarlamasını kullanır ve diğer sayı türleri için sıfırdan uzaklaşır.

``` sql
round(expression [, decimal_places])
```

**Parametre:**

-   `expression` — A number to be rounded. Can be any [ifade](../syntax.md#syntax-expressions) sayısal dönen [veri türü](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — An integer value.
    -   Eğer `decimal-places > 0` sonra işlev değeri ondalık noktanın sağına yuvarlar.
    -   Eğer `decimal-places < 0` ardından işlev değeri ondalık noktanın soluna yuvarlar.
    -   Eğer `decimal-places = 0` sonra işlev değeri tamsayı olarak yuvarlar. Bu durumda argüman ihmal edilebilir.

**Döndürülen değer:**

Giriş numarası ile aynı türden yuvarlatılmış sayı.

### Örnekler {#examples}

**Kullanım örneği**

``` sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3
```

``` text
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
└─────┴──────────────────────────┘
```

**Yuvarlama örnekleri**

En yakın numaraya yuvarlama.

``` text
round(3.2, 0) = 3
round(4.1267, 2) = 4.13
round(22,-1) = 20
round(467,-2) = 500
round(-467,-2) = -500
```

Bankacı yuvarlanıyor.

``` text
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

**Ayrıca Bakınız**

-   [roundBankers](#roundbankers)

## roundBankers {#roundbankers}

Bir sayıyı belirtilen ondalık konuma yuvarlar.

-   Yuvarlama sayısı iki sayı arasında yarıya ise, işlev banker yuvarlama kullanır.

        Banker's rounding is a method of rounding fractional numbers. When the rounding number is halfway between two numbers, it's rounded to the nearest even digit at the specified decimal position. For example: 3.5 rounds up to 4, 2.5 rounds down to 2.

        It's the default rounding method for floating point numbers defined in [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest). The [round](#rounding_functions-round) function performs the same rounding for floating point numbers. The `roundBankers` function also rounds integers the same way, for example, `roundBankers(45, -1) = 40`.

-   Diğer durumlarda, işlev sayıları en yakın tam sayıya yuvarlar.

Banker yuvarlama kullanarak, yuvarlama numaraları toplama veya bu sayıları çıkarma sonuçları üzerindeki etkisini azaltabilir.

Örneğin, farklı yuvarlama ile 1.5, 2.5, 3.5, 4.5 sayılarını topla:

-   Yuvarlama yok: 1.5 + 2.5 + 3.5 + 4.5 = 12.
-   Bankacı yuvarlama: 2 + 2 + 4 + 4 = 12.
-   En yakın tam sayıya yuvarlama: 2 + 3 + 4 + 5 = 14.

**Sözdizimi**

``` sql
roundBankers(expression [, decimal_places])
```

**Parametre**

-   `expression` — A number to be rounded. Can be any [ifade](../syntax.md#syntax-expressions) sayısal dönen [veri türü](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — Decimal places. An integer number.
    -   `decimal-places > 0` — The function rounds the number to the given position right of the decimal point. Example: `roundBankers(3.55, 1) = 3.6`.
    -   `decimal-places < 0` — The function rounds the number to the given position left of the decimal point. Example: `roundBankers(24.55, -1) = 20`.
    -   `decimal-places = 0` — The function rounds the number to an integer. In this case the argument can be omitted. Example: `roundBankers(2.5) = 2`.

**Döndürülen değer**

Banker yuvarlama yöntemi tarafından yuvarlanan bir değer.

### Örnekler {#examples-1}

**Kullanım örneği**

Sorgu:

``` sql
 SELECT number / 2 AS x, roundBankers(x, 0) AS b fROM system.numbers limit 10
```

Sonuç:

``` text
┌───x─┬─b─┐
│   0 │ 0 │
│ 0.5 │ 0 │
│   1 │ 1 │
│ 1.5 │ 2 │
│   2 │ 2 │
│ 2.5 │ 2 │
│   3 │ 3 │
│ 3.5 │ 4 │
│   4 │ 4 │
│ 4.5 │ 4 │
└─────┴───┘
```

**Bankacı yuvarlama örnekleri**

``` text
roundBankers(0.4) = 0
roundBankers(-3.5) = -4
roundBankers(4.5) = 4
roundBankers(3.55, 1) = 3.6
roundBankers(3.65, 1) = 3.6
roundBankers(10.35, 1) = 10.4
roundBankers(10.755, 2) = 11,76
```

**Ayrıca Bakınız**

-   [turlu](#rounding_functions-round)

## roundToExp2 (num) {#roundtoexp2num}

Bir sayı kabul eder. Sayı birden az ise, 0 döndürür. Aksi takdirde, sayıyı en yakın (negatif olmayan) iki dereceye yuvarlar.

## roundDuration (num) {#rounddurationnum}

Bir sayı kabul eder. Sayı birden az ise, 0 döndürür. Aksi takdirde, sayıyı kümeden sayılara yuvarlar: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. Bu fonksiyon (kayıt olmak için özeldir.Metrica ve oturum uzunluğu raporu uygulamak için kullanılır.

## roundAge (num) {#roundagenum}

Bir sayı kabul eder. Sayı 18'den küçükse, 0 döndürür. Aksi takdirde, sayıyı kümeden bir sayıya yuvarlar: 18, 25, 35, 45, 55. Bu fonksiyon (kayıt olmak için özeldir.Metrica ve kullanıcı yaş raporu uygulamak için kullanılır.

## roundDown (num, arr) {#rounddownnum-arr}

Bir sayıyı kabul eder ve belirtilen Dizideki bir öğeye yuvarlar. Değer en düşük sınırdan küçükse, en düşük sınır döndürülür.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/rounding_functions/) <!--hide-->
