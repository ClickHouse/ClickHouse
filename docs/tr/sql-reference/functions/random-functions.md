---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: "S\xF6zde Rasgele Say\u0131lar Olu\u015Fturma"
---

# Sözde rasgele sayılar üretmek için fonksiyonlar {#functions-for-generating-pseudo-random-numbers}

Sözde rasgele sayıların kriptografik olmayan jeneratörleri kullanılır.

Tüm işlevler sıfır bağımsız değişkeni veya bir bağımsız değişkeni kabul eder.
Bir argüman geçirilirse, herhangi bir tür olabilir ve değeri hiçbir şey için kullanılmaz.
Bu argümanın tek amacı, aynı işlevin iki farklı örneğinin farklı rasgele sayılarla farklı sütunlar döndürmesi için ortak alt ifade eliminasyonunu önlemektir.

## Güney Afrika parası {#rand}

Tüm uint32 tipi sayılar arasında eşit olarak dağıtılan bir sözde rasgele uint32 numarası döndürür.
Doğrusal bir uyumlu jeneratör kullanır.

## rand64 {#rand64}

Tüm uint64 tipi sayılar arasında eşit olarak dağıtılan sözde rasgele bir uint64 numarası döndürür.
Doğrusal bir uyumlu jeneratör kullanır.

## randConstant {#randconstant}

Rasgele bir değere sahip sabit bir sütun üretir.

**Sözdizimi**

``` sql
randConstant([x])
```

**Parametre**

-   `x` — [İfade](../syntax.md#syntax-expressions) sonuç olarak herhangi bir [desteklenen veri türleri](../data-types/index.md#data_types). Elde edilen değer atılır, ancak baypas için kullanıldığında ifadenin kendisi [ortak subexpression eliminasyonu](index.md#common-subexpression-elimination) işlev bir sorguda birden çok kez çağrılırsa. İsteğe bağlı parametre.

**Döndürülen değer**

-   Sözde rasgele sayı.

Tür: [Uİnt32](../data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT rand(), rand(1), rand(number), randConstant(), randConstant(1), randConstant(number)
FROM numbers(3)
```

Sonuç:

``` text
┌─────rand()─┬────rand(1)─┬─rand(number)─┬─randConstant()─┬─randConstant(1)─┬─randConstant(number)─┐
│ 3047369878 │ 4132449925 │   4044508545 │     2740811946 │      4229401477 │           1924032898 │
│ 2938880146 │ 1267722397 │   4154983056 │     2740811946 │      4229401477 │           1924032898 │
│  956619638 │ 4238287282 │   1104342490 │     2740811946 │      4229401477 │           1924032898 │
└────────────┴────────────┴──────────────┴────────────────┴─────────────────┴──────────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/random_functions/) <!--hide-->
