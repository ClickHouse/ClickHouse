---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: "Ko\u015Fullu "
---

# Koşullu Fonksiyonlar {#conditional-functions}

## eğer {#if}

Koşullu dallanmayı kontrol eder. Çoğu sistemin aksine, ClickHouse her zaman her iki ifadeyi de değerlendirir `then` ve `else`.

**Sözdizimi**

``` sql
SELECT if(cond, then, else)
```

Eğer durum `cond` sıfır olmayan bir değere değerlendirir, ifadenin sonucunu döndürür `then` ve ifad andenin sonucu `else` varsa, atlanır. Eğer... `cond` sıfır veya `NULL` fakat daha sonra sonucu `then` ifade atlanır ve sonucu `else` Ifade, varsa, döndürülür.

**Parametre**

-   `cond` – The condition for evaluation that can be zero or not. The type is UInt8, Nullable(UInt8) or NULL.
-   `then` - Koşul karşılanırsa dönmek için ifade.
-   `else` - Koşul karşılanmazsa dönmek için ifade.

**Döndürülen değerler**

İşlev yürütür `then` ve `else` ifadeler ve koşulun olup olmadığına bağlı olarak sonucunu döndürür `cond` sıfır ya da değil.

**Örnek**

Sorgu:

``` sql
SELECT if(1, plus(2, 2), plus(2, 6))
```

Sonuç:

``` text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

Sorgu:

``` sql
SELECT if(0, plus(2, 2), plus(2, 6))
```

Sonuç:

``` text
┌─plus(2, 6)─┐
│          8 │
└────────────┘
```

-   `then` ve `else` en düşük ortak türe sahip olmalıdır.

**Örnek:**

Bunu al `LEFT_RIGHT` Tablo:

``` sql
SELECT *
FROM LEFT_RIGHT

┌─left─┬─right─┐
│ ᴺᵁᴸᴸ │     4 │
│    1 │     3 │
│    2 │     2 │
│    3 │     1 │
│    4 │  ᴺᵁᴸᴸ │
└──────┴───────┘
```

Aşağıdaki sorgu karşılaştırır `left` ve `right` değerler:

``` sql
SELECT
    left,
    right,
    if(left < right, 'left is smaller than right', 'right is greater or equal than left') AS is_smaller
FROM LEFT_RIGHT
WHERE isNotNull(left) AND isNotNull(right)

┌─left─┬─right─┬─is_smaller──────────────────────────┐
│    1 │     3 │ left is smaller than right          │
│    2 │     2 │ right is greater or equal than left │
│    3 │     1 │ right is greater or equal than left │
└──────┴───────┴─────────────────────────────────────┘
```

Not: `NULL` bu örnekte değerler kullanılmaz, kontrol edin [Koşullardaki boş değerler](#null-values-in-conditionals) bölme.

## Üçlü Operatör {#ternary-operator}

Aynı gibi çalışıyor. `if` İşlev.

Sözdizimi: `cond ? then : else`

Dönüşler `then` eğer... `cond` true (sıfırdan büyük) olarak değerlendirir, aksi takdirde döndürür `else`.

-   `cond` türü olmalıdır `UInt8`, ve `then` ve `else` en düşük ortak türe sahip olmalıdır.

-   `then` ve `else` olabilir `NULL`

**Ayrıca bakınız**

-   [ifNotFinite](other-functions.md#ifnotfinite).

## multiİf {#multiif}

Yaz allowsmanızı sağlar [CASE](../operators/index.md#operator_case) operatör sorguda daha kompakt.

Sözdizimi: `multiIf(cond_1, then_1, cond_2, then_2, ..., else)`

**Parametre:**

-   `cond_N` — The condition for the function to return `then_N`.
-   `then_N` — The result of the function when executed.
-   `else` — The result of the function if none of the conditions is met.

İşlev kabul eder `2N+1` parametre.

**Döndürülen değerler**

İşlev, değerlerden birini döndürür `then_N` veya `else` bu koşullara bağlı olarak `cond_N`.

**Örnek**

Yine kullanarak `LEFT_RIGHT` Tablo.

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'left is greater', left = right, 'Both equal', 'Null value') AS result
FROM LEFT_RIGHT

┌─left─┬─right─┬─result──────────┐
│ ᴺᵁᴸᴸ │     4 │ Null value      │
│    1 │     3 │ left is smaller │
│    2 │     2 │ Both equal      │
│    3 │     1 │ left is greater │
│    4 │  ᴺᵁᴸᴸ │ Null value      │
└──────┴───────┴─────────────────┘
```

## Koşullu Sonuçları Doğrudan Kullanma {#using-conditional-results-directly}

Koşullar her zaman sonuç `0`, `1` veya `NULL`. Böylece koşullu sonuçları doğrudan bu şekilde kullanabilirsiniz:

``` sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

## Koşullardaki boş değerler {#null-values-in-conditionals}

Ne zaman `NULL` değerler koşullarla ilgilidir, sonuç da olacaktır `NULL`.

``` sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

Bu nedenle, sorgularınızı türleri dikkatli bir şekilde oluşturmalısınız `Nullable`.

Aşağıdaki örnek, eşittir koşulu eklemek başarısız tarafından bu gösterir `multiIf`.

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/conditional_functions/) <!--hide-->
