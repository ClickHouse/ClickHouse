<div markdown="1" markdown="1" dir="rtl">

# Float32, Float64 {#float32-float64}

[اعداد Float](https://en.wikipedia.org/wiki/IEEE_754).

Type های float در ClickHouse مشابه C می باشد:

-   `Float32` - `float`
-   `Float64` - `double`

توصیه می کنیم که داده ها را هرزمان که امکان پذیره است به جای float به صورت int ذخیره کنید. برای مثال: تبدیل دقت اعداد به یک مقدار int، مثل سرعت page load در قالب میلی ثانیه.

## استفاده از اعداد Float {#stfdh-z-dd-float}

-   محاسبات با اعداد با Float ممکن است خطای round شدن را ایجاد کنند.

</div>

``` sql
SELECT 1 - 0.9
```

    ┌───────minus(1, 0.9)─┐
    │ 0.09999999999999998 │
    └─────────────────────┘

<div markdown="1" markdown="1" dir="rtl">

-   نتایج محاسبات بسته به متد محاسباتی می باشد (نوع processor و معماری سیستم).
-   محاسبات Float ممکن اسن نتایجی مثل infinity (`inf`) و «Not-a-number» (`Nan`) داشته باشد. این در هنگام پردازش نتایج محاسبات باید مورد توجه قرار گیرد.
-   هنگام خواندن اعداد float از سطر ها، نتایج ممکن است نزدیک به اعداد machine-representable نباشد.

## NaN و Inf {#data-type-float-nan-inf}

در مقابل استاندارد SQL، ClickHouse از موارد زیر مربوط به اعداد float پشتیبانی می کند:

-   `Inf` – Infinity.

</div>

``` sql
SELECT 0.5 / 0
```

    ┌─divide(0.5, 0)─┐
    │            inf │
    └────────────────┘

<div markdown="1" markdown="1" dir="rtl">

-   `-Inf` – Negative infinity.

</div>

``` sql
SELECT -0.5 / 0
```

    ┌─divide(-0.5, 0)─┐
    │            -inf │
    └─────────────────┘

<div markdown="1" markdown="1" dir="rtl">

-   `NaN` – Not a number.

</div>

    SELECT 0 / 0

    ┌─divide(0, 0)─┐
    │          nan │
    └──────────────┘

<div markdown="1" markdown="1" dir="rtl">

قوانین مربوط به مرتب سازی `Nan` را در بخش [ORDER BY clause](../query_language/select.md) ببینید.

</div>

[مقاله اصلی](https://clickhouse.tech/docs/fa/data_types/float/) <!--hide-->
