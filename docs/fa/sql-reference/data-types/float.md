---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: Float32, Float64
---

# Float32, Float64 {#float32-float64}

[اعداد ممیز شناور](https://en.wikipedia.org/wiki/IEEE_754).

انواع معادل انواع ج هستند:

-   `Float32` - `float`
-   `Float64` - `double`

ما توصیه می کنیم که شما ذخیره داده ها در فرم صحیح در هر زمان ممکن است. مثلا, تبدیل اعداد دقت ثابت به ارزش عدد صحیح, مانند مقدار پولی و یا بار بار صفحه در میلی ثانیه.

## با استفاده از اعداد ممیز شناور {#using-floating-point-numbers}

-   محاسبات با اعداد ممیز شناور ممکن است یک خطای گرد کردن تولید.

<!-- -->

``` sql
SELECT 1 - 0.9
```

``` text
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

-   نتیجه محاسبه بستگی به روش محاسبه (نوع پردازنده و معماری سیستم کامپیوتری).
-   محاسبات ممیز شناور ممکن است در اعداد مانند بی نهایت منجر شود (`Inf`) و “not-a-number” (`NaN`). این را باید در نظر گرفته شود در هنگام پردازش نتایج محاسبات.
-   هنگامی که تجزیه اعداد ممیز شناور از متن, نتیجه ممکن است نزدیکترین شماره ماشین نمایندگی.

## هشدار داده می شود {#data_type-float-nan-inf}

در مقابل به گذاشتن استاندارد, خانه رعیتی پشتیبانی از مقوله های زیر است از اعداد ممیز شناور:

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

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/float/) <!--hide-->
