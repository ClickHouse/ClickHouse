---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "Toplama Fonksiyonlar\u0131"
toc_priority: 33
toc_title: "Giri\u015F"
---

# Toplama Fonksiyonları {#aggregate-functions}

Toplama fonksiyonları [normal](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) veritabanı uzmanları tarafından beklendiği gibi.

ClickHouse da destekler:

-   [Parametrik agrega fonksiyonları](parametric-functions.md#aggregate_functions_parametric), sütunlara ek olarak diğer parametreleri kabul eder.
-   [Birleştiriciler](combinators.md#aggregate_functions_combinators) toplama işlevlerinin davranışını değiştiren.

## NULL işleme {#null-processing}

Toplama sırasında, tüm `NULL`s atlanır.

**Örnekler:**

Bu tabloyu düşünün:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Diyelim ki değerleri toplamanız gerekiyor `y` sütun:

``` sql
SELECT sum(y) FROM t_null_big
```

    ┌─sum(y)─┐
    │      7 │
    └────────┘

Bu `sum` fonksiyon yorumlar `NULL` olarak `0`. Özellikle, bu, işlevin tüm değerlerin bulunduğu bir seçimin girişini aldığı anlamına gelir `NULL`, sonra sonuç olacak `0`, değil `NULL`.

Şimdi kullanabilirsiniz `groupArray` bir dizi oluşturmak için işlev `y` sütun:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` içermez `NULL` elde edilen dizi.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
