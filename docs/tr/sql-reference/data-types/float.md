---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: Float32, Float64
---

# Float32, Float64 {#float32-float64}

[Kayan nokta numaraları](https://en.wikipedia.org/wiki/IEEE_754).

Türleri C türlerine eşdeğerdir:

-   `Float32` - `float`
-   `Float64` - `double`

Verileri mümkün olduğunda tamsayı biçiminde saklamanızı öneririz. Örneğin, sabit hassas sayıları parasal tutarlar veya sayfa yükleme süreleri gibi milisaniye cinsinden tamsayı değerlerine dönüştürün.

## Kayan noktalı sayıları kullanma {#using-floating-point-numbers}

-   Kayan noktalı sayılarla yapılan hesaplamalar yuvarlama hatası oluşturabilir.

<!-- -->

``` sql
SELECT 1 - 0.9
```

``` text
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

-   Hesaplamanın sonucu hesaplama yöntemine (bilgisayar sisteminin işlemci tipi ve mimarisi) bağlıdır.
-   Kayan nokta hesaplamaları, sonsuzluk gibi sayılarla sonuçlanabilir (`Inf`) ve “not-a-number” (`NaN`). Hesaplamaların sonuçlarını işlerken bu dikkate alınmalıdır.
-   Kayan noktalı sayıları metinden ayrıştırırken, sonuç en yakın makine tarafından temsil edilebilir sayı olmayabilir.

## N andan ve In andf {#data_type-float-nan-inf}

Standart SQL aksine, ClickHouse kayan noktalı sayılar aşağıdaki kategorileri destekler:

-   `Inf` – Infinity.

<!-- -->

``` sql
SELECT 0.5 / 0
```

``` text
┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

-   `-Inf` – Negative infinity.

<!-- -->

``` sql
SELECT -0.5 / 0
```

``` text
┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

-   `NaN` – Not a number.

<!-- -->

``` sql
SELECT 0 / 0
```

``` text
┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

    See the rules for `NaN` sorting in the section [ORDER BY clause](../sql_reference/statements/select/order-by.md).

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/float/) <!--hide-->
