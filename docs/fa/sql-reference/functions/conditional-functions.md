---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: "\u0634\u0631\u0637\u06CC "
---

# توابع شرطی {#conditional-functions}

## اگر {#if}

کنترل انشعاب مشروط. بر خلاف اکثر سیستم های تاتر همیشه هر دو عبارت را ارزیابی کنید `then` و `else`.

**نحو**

``` sql
SELECT if(cond, then, else)
```

اگر شرایط `cond` ارزیابی به یک مقدار غیر صفر, می گرداند در نتیجه بیان `then` و نتیجه بیان `else`, اگر در حال حاضر, قلم است. اگر `cond` صفر یا `NULL` سپس نتیجه `then` بیان نادیده گرفته شده است و در نتیجه `else` عبارت, در صورت حاضر, بازگشته است.

**پارامترها**

-   `cond` – The condition for evaluation that can be zero or not. The type is UInt8, Nullable(UInt8) or NULL.
-   `then` - بیان به بازگشت اگر شرایط ملاقات کرده است.
-   `else` - بیان به بازگشت اگر شرایط ملاقات نکرده است.

**مقادیر بازگشتی**

تابع اجرا می شود `then` و `else` عبارات و نتیجه خود را بر می گرداند, بسته به اینکه شرایط `cond` به پایان رسید تا صفر یا نه.

**مثال**

پرسوجو:

``` sql
SELECT if(1, plus(2, 2), plus(2, 6))
```

نتیجه:

``` text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

پرسوجو:

``` sql
SELECT if(0, plus(2, 2), plus(2, 6))
```

نتیجه:

``` text
┌─plus(2, 6)─┐
│          8 │
└────────────┘
```

-   `then` و `else` باید کمترین نوع مشترک دارند.

**مثال:**

اینو بگیر `LEFT_RIGHT` جدول:

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

پرس و جو زیر مقایسه می کند `left` و `right` مقادیر:

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

یادداشت: `NULL` ارزش ها در این مثال استفاده نمی شود, بررسی [ارزشهای پوچ در شرطی](#null-values-in-conditionals) بخش.

## اپراتور سه تایی {#ternary-operator}

این همان کار می کند `if` تابع.

نحو: `cond ? then : else`

بازگشت `then` اگر `cond` ارزیابی درست باشد (بیشتر از صفر), در غیر این صورت بازده `else`.

-   `cond` باید از نوع باشد `UInt8` و `then` و `else` باید کمترین نوع مشترک دارند.

-   `then` و `else` می تواند باشد `NULL`

**همچنین نگاه کنید به**

-   [اطلاعات دقیق](other-functions.md#ifnotfinite).

## چندف {#multiif}

اجازه می دهد تا شما را به نوشتن [CASE](../operators/index.md#operator_case) اپراتور فشرده تر در پرس و جو.

نحو: `multiIf(cond_1, then_1, cond_2, then_2, ..., else)`

**پارامترها:**

-   `cond_N` — The condition for the function to return `then_N`.
-   `then_N` — The result of the function when executed.
-   `else` — The result of the function if none of the conditions is met.

تابع می پذیرد `2N+1` پارامترها

**مقادیر بازگشتی**

تابع یکی از مقادیر را برمی گرداند `then_N` یا `else`, بسته به شرایط `cond_N`.

**مثال**

دوباره با استفاده از `LEFT_RIGHT` جدول

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

## با استفاده از نتایج شرطی به طور مستقیم {#using-conditional-results-directly}

شرطی همیشه به نتیجه `0`, `1` یا `NULL`. بنابراین شما می توانید نتایج شرطی به طور مستقیم مثل این استفاده کنید:

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

## ارزشهای پوچ در شرطی {#null-values-in-conditionals}

چه زمانی `NULL` ارزش ها در شرطی درگیر, نتیجه نیز خواهد بود `NULL`.

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

بنابراین شما باید نمایش داده شد خود را با دقت ساخت اگر انواع هستند `Nullable`.

مثال زیر نشان می دهد این شکست برای اضافه کردن شرایط برابر به `multiIf`.

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

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/conditional_functions/) <!--hide-->
