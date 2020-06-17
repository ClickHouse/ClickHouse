---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 63
toc_title: "\u06A9\u0627\u0631 \u0628\u0627 Nullable \u0627\u0633\u062A\u062F\u0644\
  \u0627\u0644"
---

# توابع برای کار با Nullable مصالح {#functions-for-working-with-nullable-aggregates}

## isNull {#isnull}

بررسی اینکه بحث چیست [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNull(x)
```

**پارامترها**

-   `x` — A value with a non-compound data type.

**مقدار بازگشتی**

-   `1` اگر `x` هست `NULL`.
-   `0` اگر `x` نیست `NULL`.

**مثال**

جدول ورودی

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

پرسوجو

``` sql
SELECT x FROM t_null WHERE isNull(y)
```

``` text
┌─x─┐
│ 1 │
└───┘
```

## اینترنت {#isnotnull}

بررسی اینکه بحث چیست [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNotNull(x)
```

**پارامترها:**

-   `x` — A value with a non-compound data type.

**مقدار بازگشتی**

-   `0` اگر `x` هست `NULL`.
-   `1` اگر `x` نیست `NULL`.

**مثال**

جدول ورودی

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

پرسوجو

``` sql
SELECT x FROM t_null WHERE isNotNull(y)
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## فلز کاری {#coalesce}

چک از چپ به راست چه `NULL` استدلال به تصویب رسید و اولین غیر گرداند-`NULL` استدلال کردن.

``` sql
coalesce(x,...)
```

**پارامترها:**

-   هر تعداد از پارامترهای یک نوع غیر مرکب. تمام پارامترها باید با نوع داده سازگار باشند.

**مقادیر بازگشتی**

-   اولین غیر-`NULL` استدلال کردن.
-   `NULL`, اگر همه استدلال ها `NULL`.

**مثال**

یک لیست از مخاطبین است که ممکن است راه های متعدد برای تماس با مشتری مشخص را در نظر بگیرید.

``` text
┌─name─────┬─mail─┬─phone─────┬──icq─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │  123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

این `mail` و `phone` زمینه های رشته نوع هستند, اما `icq` زمینه است `UInt32` بنابراین نیاز به تبدیل شدن به `String`.

دریافت اولین روش تماس در دسترس برای مشتری از لیست تماس:

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

بازگرداندن یک مقدار جایگزین اگر استدلال اصلی است `NULL`.

``` sql
ifNull(x,alt)
```

**پارامترها:**

-   `x` — The value to check for `NULL`.
-   `alt` — The value that the function returns if `x` هست `NULL`.

**مقادیر بازگشتی**

-   مقدار `x` اگر `x` نیست `NULL`.
-   مقدار `alt` اگر `x` هست `NULL`.

**مثال**

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

## nullIf {#nullif}

بازگشت `NULL` اگر استدلال برابر هستند.

``` sql
nullIf(x, y)
```

**پارامترها:**

`x`, `y` — Values for comparison. They must be compatible types, or ClickHouse will generate an exception.

**مقادیر بازگشتی**

-   `NULL`, اگر استدلال برابر هستند.
-   این `x` ارزش, اگر استدلال برابر نیست.

**مثال**

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

## قابل قبول {#assumenotnull}

نتایج در ارزش نوع [Nullable](../../sql-reference/data-types/nullable.md) برای یک غیر- `Nullable`, اگر مقدار است `NULL`.

``` sql
assumeNotNull(x)
```

**پارامترها:**

-   `x` — The original value.

**مقادیر بازگشتی**

-   مقدار اصلی از غیر-`Nullable` نوع, اگر نیست `NULL`.
-   مقدار پیش فرض برای غیر-`Nullable` نوع اگر مقدار اصلی بود `NULL`.

**مثال**

در نظر بگیرید که `t_null` جدول

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

درخواست `assumeNotNull` تابع به `y` ستون.

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

## قابل تنظیم {#tonullable}

تبدیل نوع استدلال به `Nullable`.

``` sql
toNullable(x)
```

**پارامترها:**

-   `x` — The value of any non-compound type.

**مقدار بازگشتی**

-   مقدار ورودی با یک `Nullable` نوع.

**مثال**

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

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/functions_for_nulls/) <!--hide-->
