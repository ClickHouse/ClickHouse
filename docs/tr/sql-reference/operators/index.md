---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 37
toc_title: "Operat\xF6rler"
---

# Operatörler {#operators}

ClickHouse onların öncelik, öncelik ve ilişkilendirme göre sorgu ayrıştırma aşamasında karşılık gelen işlevlere işleçleri dönüştürür.

## Erişim Operatörleri {#access-operators}

`a[N]` – Access to an element of an array. The `arrayElement(a, N)` işlev.

`a.N` – Access to a tuple element. The `tupleElement(a, N)` işlev.

## Sayısal Olumsuzlama Operatörü {#numeric-negation-operator}

`-a` – The `negate (a)` işlev.

## Çarpma Ve bölme operatörleri {#multiplication-and-division-operators}

`a * b` – The `multiply (a, b)` işlev.

`a / b` – The `divide(a, b)` işlev.

`a % b` – The `modulo(a, b)` işlev.

## Toplama Ve çıkarma operatörleri {#addition-and-subtraction-operators}

`a + b` – The `plus(a, b)` işlev.

`a - b` – The `minus(a, b)` işlev.

## Karşılaştırma Operatörleri {#comparison-operators}

`a = b` – The `equals(a, b)` işlev.

`a == b` – The `equals(a, b)` işlev.

`a != b` – The `notEquals(a, b)` işlev.

`a <> b` – The `notEquals(a, b)` işlev.

`a <= b` – The `lessOrEquals(a, b)` işlev.

`a >= b` – The `greaterOrEquals(a, b)` işlev.

`a < b` – The `less(a, b)` işlev.

`a > b` – The `greater(a, b)` işlev.

`a LIKE s` – The `like(a, b)` işlev.

`a NOT LIKE s` – The `notLike(a, b)` işlev.

`a BETWEEN b AND c` – The same as `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – The same as `a < b OR a > c`.

## Veri kümeleriyle çalışmak için operatörler {#operators-for-working-with-data-sets}

*Görmek [Operatör İNLERDE](statements/select.md#select-in-operators).*

`a IN ...` – The `in(a, b)` işlev.

`a NOT IN ...` – The `notIn(a, b)` işlev.

`a GLOBAL IN ...` – The `globalIn(a, b)` işlev.

`a GLOBAL NOT IN ...` – The `globalNotIn(a, b)` işlev.

## Tarih Ve Saatlerle çalışmak için operatörler {#operators-datetime}

### EXTRACT {#operator-extract}

``` sql
EXTRACT(part FROM date);
```

Belirli bir tarihten parçaları ayıklayın. Örneğin, belirli bir tarihten bir ay veya bir zamandan bir saniye alabilirsiniz.

Bu `part` parametre almak için tarihin hangi bölümünü belirtir. Aşağıdaki değerler kullanılabilir:

-   `DAY` — The day of the month. Possible values: 1–31.
-   `MONTH` — The number of a month. Possible values: 1–12.
-   `YEAR` — The year.
-   `SECOND` — The second. Possible values: 0–59.
-   `MINUTE` — The minute. Possible values: 0–59.
-   `HOUR` — The hour. Possible values: 0–23.

Bu `part` parametre büyük / küçük harf duyarsızdır.

Bu `date` parametre, işlenecek tarihi veya saati belirtir. Ya [Tarihli](../sql-reference/data-types/date.md) veya [DateTime](../sql-reference/data-types/datetime.md) türü desteklenir.

Örnekler:

``` sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

Aşağıdaki örnekte bir tablo oluşturuyoruz ve içine bir değer ekliyoruz `DateTime` tür.

``` sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
)
ENGINE = Log;
```

``` sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

``` sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

``` text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

Daha fazla örnek görebilirsiniz [testler](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#operator-interval}

Oluşturur bir [Aralıklı](../sql-reference/data-types/special-data-types/interval.md)- aritmetik işlemlerde kullanılması gereken tip değeri [Tarihli](../sql-reference/data-types/date.md) ve [DateTime](../sql-reference/data-types/datetime.md)- tip değerleri.

Aralık türleri:
- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `WEEK`
- `MONTH`
- `QUARTER`
- `YEAR`

!!! warning "Uyarıcı"
    Farklı tiplere sahip aralıklar birleştirilemez. Gibi ifadeler kullanamazsınız `INTERVAL 4 DAY 1 HOUR`. Aralıkların, örneğin aralığın en küçük birimine eşit veya daha küçük olan birimlerdeki aralıkları belirtin, `INTERVAL 25 HOUR`. Aşağıdaki örnekte olduğu gibi ardışık işlemleri kullanabilirsiniz.

Örnek:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

**Ayrıca Bakınız**

-   [Aralıklı](../sql-reference/data-types/special-data-types/interval.md) veri türü
-   [toİnterval](../sql-reference/functions/type-conversion-functions.md#function-tointerval) tip dönüştürme işlevleri

## Mantıksal Olumsuzlama Operatörü {#logical-negation-operator}

`NOT a` – The `not(a)` işlev.

## Mantıksal Ve operatör {#logical-and-operator}

`a AND b` – The`and(a, b)` işlev.

## Mantıksal Veya operatör {#logical-or-operator}

`a OR b` – The `or(a, b)` işlev.

## Koşullu Operatör {#conditional-operator}

`a ? b : c` – The `if(a, b, c)` işlev.

Not:

Koşullu işleç B ve c değerlerini hesaplar, ardından a koşulunun karşılanıp karşılanmadığını kontrol eder ve ardından karşılık gelen değeri döndürür. Eğer `b` veya `C` is an [arrayJoin()](../sql-reference/functions/array-join.md#functions_arrayjoin) işlev, her satır ne olursa olsun çoğaltılır “a” koşul.

## Koşullu İfade {#operator_case}

``` sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

Eğer `x` belirtilen sonra `transform(x, [a, ...], [b, ...], c)` function is used. Otherwise – `multiIf(a, b, ..., c)`.

Eğer herhangi bir `ELSE c` ifadedeki yan tümce, varsayılan değer `NULL`.

Bu `transform` fonksiyonu ile çalışmıyor `NULL`.

## Birleştirme Operatörü {#concatenation-operator}

`s1 || s2` – The `concat(s1, s2) function.`

## Lambda Oluşturma Operatörü {#lambda-creation-operator}

`x -> expr` – The `lambda(x, expr) function.`

Parantez oldukları için aşağıdaki operatörler bir önceliğe sahip değildir:

## Dizi Oluşturma Operatörü {#array-creation-operator}

`[x1, ...]` – The `array(x1, ...) function.`

## Tuple Oluşturma Operatörü {#tuple-creation-operator}

`(x1, x2, ...)` – The `tuple(x2, x2, ...) function.`

## İlişkisellik {#associativity}

Tüm ikili operatörler ilişkisellikten ayrıldı. Mesela, `1 + 2 + 3` dönüştür toülür `plus(plus(1, 2), 3)`.
Bazen bu beklediğiniz gibi çalışmaz. Mesela, `SELECT 4 > 2 > 3` 0 ile sonuç willlanır.

Verimlilik için, `and` ve `or` işlevler herhangi bir sayıda bağımsız değişkeni kabul eder. İlgili zincirler `AND` ve `OR` operatörler bu işlevlerin tek bir çağrısına dönüştürülür.

## İçin Kontrol `NULL` {#checking-for-null}

ClickHouse destekler `IS NULL` ve `IS NOT NULL` operatörler.

### IS NULL {#operator-is-null}

-   İçin [Nullable](../sql-reference/data-types/nullable.md) türü değerleri `IS NULL` operatör döner:
    -   `1` değeri ise `NULL`.
    -   `0` başka.
-   Diğer değerler için, `IS NULL` operatör her zaman döner `0`.

<!-- -->

``` sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

``` text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

### IS NOT NULL {#is-not-null}

-   İçin [Nullable](../sql-reference/data-types/nullable.md) türü değerleri `IS NOT NULL` operatör döner:
    -   `0` değeri ise `NULL`.
    -   `1` başka.
-   Diğer değerler için, `IS NOT NULL` operatör her zaman döner `1`.

<!-- -->

``` sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

``` text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/operators/) <!--hide-->
