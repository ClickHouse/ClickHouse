---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "\u06A9\u0627\u0631\u06A9\u0631\u062F(\u0646\u0627\u0645 \u0648 \u0646\u0627\
  \u0645 \u062E\u0627\u0646\u0648\u0627\u062F\u06AF\u06CC..)"
---

# AggregateFunction(name, types_of_arguments…) {#data-type-aggregatefunction}

Aggregate functions can have an implementation-defined intermediate state that can be serialized to an AggregateFunction(…) data type and stored in a table, usually, by means of [مشاهده محقق](../../sql-reference/statements/create.md#create-view). راه معمول برای تولید یک دولت تابع جمع است با فراخوانی تابع جمع با `-State` پسوند. برای دریافت نتیجه نهایی از تجمع در اینده, شما باید همان تابع کل با استفاده از `-Merge`پسوند.

`AggregateFunction` — parametric data type.

**پارامترها**

-   نام تابع جمع.

        If the function is parametric, specify its parameters too.

-   انواع استدلال تابع جمع.

**مثال**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[دانشگاه](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq). ([هر](../../sql-reference/aggregate-functions/reference.md#agg_function-any)+[اگر](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-if)) و [quantiles](../../sql-reference/aggregate-functions/reference.md) توابع مجموع پشتیبانی در خانه کلیک می باشد.

## استفاده {#usage}

### درج داده {#data-insertion}

برای وارد کردن داده ها استفاده کنید `INSERT SELECT` با مجموع `-State`- توابع .

**نمونه تابع**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

در مقایسه با توابع مربوطه `uniq` و `quantiles`, `-State`- توابع بازگشت به دولت, به جای ارزش نهایی. به عبارت دیگر ارزش بازگشت `AggregateFunction` نوع.

در نتایج `SELECT` پرس و جو, ارزش `AggregateFunction` نوع اجرای خاص نمایندگی دودویی برای همه فرمت های خروجی کلیک کنید. اگر کمپرسی داده ها به, مثلا, `TabSeparated` قالب با `SELECT` پرس و جو, سپس این روگرفت را می توان با استفاده از لود `INSERT` پرس و جو.

### گزینش داده {#data-selection}

هنگام انتخاب داده ها از `AggregatingMergeTree` جدول استفاده کنید `GROUP BY` بند و توابع کل همان هنگام قرار دادن داده, اما با استفاده از `-Merge`پسوند.

یک تابع جمع با `-Merge` پسوند مجموعه ای از ایالت ها را ترکیب می کند و نتیجه تجمع کامل داده ها را باز می گرداند.

مثلا, دو نمایش داده شد زیر بازگشت به همان نتیجه:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## مثال طریقه استفاده {#usage-example}

ببینید [ریزدانه](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) موتور باشرکت.

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/nested_data_structures/aggregatefunction/) <!--hide-->
