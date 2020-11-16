---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 63
toc_title: "Null arg\xFCmanlarla \xE7al\u0131\u015Fma"
---

# Null Agregalarla çalışmak için işlevler {#functions-for-working-with-nullable-aggregates}

## isNull {#isnull}

Bağımsız değişken olup olmadığını denetler [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNull(x)
```

**Parametre**

-   `x` — A value with a non-compound data type.

**Döndürülen değer**

-   `1` eğer `x` oluyor `NULL`.
-   `0` eğer `x` değildir `NULL`.

**Örnek**

Giriş tablosu

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Sorgu

``` sql
SELECT x FROM t_null WHERE isNull(y)
```

``` text
┌─x─┐
│ 1 │
└───┘
```

## isNotNull {#isnotnull}

Bağımsız değişken olup olmadığını denetler [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNotNull(x)
```

**Parametre:**

-   `x` — A value with a non-compound data type.

**Döndürülen değer**

-   `0` eğer `x` oluyor `NULL`.
-   `1` eğer `x` değildir `NULL`.

**Örnek**

Giriş tablosu

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Sorgu

``` sql
SELECT x FROM t_null WHERE isNotNull(y)
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## birleşmek {#coalesce}

Olup olmadığını soldan sağa denetler `NULL` argümanlar geçti ve ilk olmayan döndürür-`NULL` tartışma.

``` sql
coalesce(x,...)
```

**Parametre:**

-   Bileşik olmayan tipte herhangi bir sayıda parametre. Tüm parametreler veri türüne göre uyumlu olmalıdır.

**Döndürülen değerler**

-   İlk sigara-`NULL` tartışma.
-   `NULL`, eğer tüm argümanlar `NULL`.

**Örnek**

Bir müşteriyle iletişim kurmak için birden çok yol belirtebilecek kişilerin listesini düşünün.

``` text
┌─name─────┬─mail─┬─phone─────┬──icq─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │  123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

Bu `mail` ve `phone` alanlar String tip ofindedir, ancak `icq` Fi fieldeld is `UInt32`, bu yüzden dönüştürülmesi gerekiyor `String`.

Müşteri için ilk kullanılabilir iletişim yöntemini kişi listesinden alın:

``` sql
SELECT coalesce(mail, phone, CAST(icq,'Nullable(String)')) FROM aBook
```

``` text
┌─name─────┬─coalesce(mail, phone, CAST(icq, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                            │
│ client 2 │ ᴺᵁᴸᴸ                                                 │
└──────────┴──────────────────────────────────────────────────────┘
```

## ifNull {#ifnull}

Ana bağımsız değişken ise alternatif bir değer döndürür `NULL`.

``` sql
ifNull(x,alt)
```

**Parametre:**

-   `x` — The value to check for `NULL`.
-   `alt` — The value that the function returns if `x` oluyor `NULL`.

**Döndürülen değerler**

-   Değer `x`, eğer `x` değildir `NULL`.
-   Değer `alt`, eğer `x` oluyor `NULL`.

**Örnek**

``` sql
SELECT ifNull('a', 'b')
```

``` text
┌─ifNull('a', 'b')─┐
│ a                │
└──────────────────┘
```

``` sql
SELECT ifNull(NULL, 'b')
```

``` text
┌─ifNull(NULL, 'b')─┐
│ b                 │
└───────────────────┘
```

## nullİf {#nullif}

Dönüşler `NULL` argümanlar eşitse.

``` sql
nullIf(x, y)
```

**Parametre:**

`x`, `y` — Values for comparison. They must be compatible types, or ClickHouse will generate an exception.

**Döndürülen değerler**

-   `NULL`, argümanlar eşitse.
-   Bu `x` bağımsız değişkenler eşit değilse, değer.

**Örnek**

``` sql
SELECT nullIf(1, 1)
```

``` text
┌─nullIf(1, 1)─┐
│         ᴺᵁᴸᴸ │
└──────────────┘
```

``` sql
SELECT nullIf(1, 2)
```

``` text
┌─nullIf(1, 2)─┐
│            1 │
└──────────────┘
```

## assumeNotNull {#assumenotnull}

Bir tür değeri ile sonuçlanır [Nullable](../../sql-reference/data-types/nullable.md) bir sigara için- `Nullable` eğer değer değil `NULL`.

``` sql
assumeNotNull(x)
```

**Parametre:**

-   `x` — The original value.

**Döndürülen değerler**

-   Olmayan orijinal değeri-`Nullable` tipi, değilse `NULL`.
-   Olmayan için varsayılan değer-`Nullable` özgün değer ise yazın `NULL`.

**Örnek**

Düşünün `t_null` Tablo.

``` sql
SHOW CREATE TABLE t_null
```

``` text
┌─statement─────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_null ( x Int8,  y Nullable(Int8)) ENGINE = TinyLog │
└───────────────────────────────────────────────────────────────────────────┘
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Uygula `assumeNotNull` fonksiyonu için `y` sütun.

``` sql
SELECT assumeNotNull(y) FROM t_null
```

``` text
┌─assumeNotNull(y)─┐
│                0 │
│                3 │
└──────────────────┘
```

``` sql
SELECT toTypeName(assumeNotNull(y)) FROM t_null
```

``` text
┌─toTypeName(assumeNotNull(y))─┐
│ Int8                         │
│ Int8                         │
└──────────────────────────────┘
```

## toNullable {#tonullable}

Bağımsız değişken türünü dönüştürür `Nullable`.

``` sql
toNullable(x)
```

**Parametre:**

-   `x` — The value of any non-compound type.

**Döndürülen değer**

-   Bir ile giriş değeri `Nullable` tür.

**Örnek**

``` sql
SELECT toTypeName(10)
```

``` text
┌─toTypeName(10)─┐
│ UInt8          │
└────────────────┘
```

``` sql
SELECT toTypeName(toNullable(10))
```

``` text
┌─toTypeName(toNullable(10))─┐
│ Nullable(UInt8)            │
└────────────────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/functions_for_nulls/) <!--hide-->
