---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u06AF\u0631\u062F \u06A9\u0631\u062F\u0646"
---

# گرد کردن توابع {#rounding-functions}

## طبقه (ایکس\]) {#floorx-n}

بازگرداندن بیشترین تعداد دور است که کمتر از یا مساوی `x`. تعداد دور چند تن از 1/10 و یا نزدیکترین تعداد داده های مناسب نوع اگر 1 / 10 دقیق نیست.
‘N’ ثابت عدد صحیح است, پارامتر اختیاری. به طور پیش فرض صفر است, که به معنی به دور به یک عدد صحیح.
‘N’ ممکن است منفی باشد.

مثالها: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x` هر نوع عددی است. نتیجه تعدادی از همان نوع است.
برای استدلال عدد صحیح را حس می کند به دور با منفی `N` ارزش (برای غیر منفی `N` تابع هیچ کاری نمی کند).
اگر گرد باعث سرریز (مثلا, کف سازی(-128, -1)), نتیجه اجرای خاص بازگشته است.

## هشدار داده می شود\]) {#ceilx-n-ceilingx-n}

بازگرداندن کوچکترین عدد دور است که بیشتر از یا مساوی `x`. در هر راه دیگر, این همان است که `floor` تابع (بالا را ببینید).

## هشدار داده می شود\]) {#truncx-n-truncatex-n}

بازگرداندن تعداد دور با بزرگترین ارزش مطلق است که ارزش مطلق کمتر یا مساوی `x`‘s. In every other way, it is the same as the ’floor’ تابع (بالا را ببینید).

## دور (ایکس\]) {#rounding_functions-round}

دور یک مقدار به تعداد مشخصی از رقم اعشار.

تابع نزدیکترین تعداد از سفارش مشخص شده را برمی گرداند. در صورتی که تعداد داده شده است فاصله برابر با شماره های اطراف, تابع با استفاده از گرد کردن بانکدار برای انواع شماره شناور و دور به دور از صفر برای انواع شماره های دیگر.

``` sql
round(expression [, decimal_places])
```

**پارامترها:**

-   `expression` — A number to be rounded. Can be any [عبارت](../syntax.md#syntax-expressions) بازگشت عددی [نوع داده](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — An integer value.
    -   اگر `decimal-places > 0` سپس تابع دور ارزش به سمت راست از نقطه اعشار.
    -   اگر `decimal-places < 0` سپس تابع دور ارزش به سمت چپ نقطه اعشار.
    -   اگر `decimal-places = 0` سپس تابع دور ارزش به عدد صحیح. در این مورد استدلال را می توان حذف.

**مقدار بازگشتی:**

تعداد گرد از همان نوع به عنوان شماره ورودی.

### مثالها {#examples}

**مثال استفاده**

``` sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3
```

``` text
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
└─────┴──────────────────────────┘
```

**نمونه هایی از گرد کردن**

گرد کردن به نزدیکترین شماره.

``` text
round(3.2, 0) = 3
round(4.1267, 2) = 4.13
round(22,-1) = 20
round(467,-2) = 500
round(-467,-2) = -500
```

گرد کردن بانکدار.

``` text
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

**همچنین نگاه کنید به**

-   [سرباز](#roundbankers)

## سرباز {#roundbankers}

دور یک عدد به یک موقعیت دهدهی مشخص شده است.

-   اگر تعداد گرد کردن در نیمه راه بین دو عدد است, تابع با استفاده از گرد کردن بانکدار.

        Banker's rounding is a method of rounding fractional numbers. When the rounding number is halfway between two numbers, it's rounded to the nearest even digit at the specified decimal position. For example: 3.5 rounds up to 4, 2.5 rounds down to 2.

        It's the default rounding method for floating point numbers defined in [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest). The [round](#rounding_functions-round) function performs the same rounding for floating point numbers. The `roundBankers` function also rounds integers the same way, for example, `roundBankers(45, -1) = 40`.

-   در موارد دیگر تابع دور اعداد به نزدیکترین عدد صحیح.

با استفاده از گرد کردن بانکدار, شما می توانید اثر است که گرد کردن اعداد در نتایج حاصل از جمع و یا کم کردن این اعداد را کاهش می دهد.

مثلا, تعداد مجموع 1.5, 2.5, 3.5, 4.5 با گرد کردن متفاوت:

-   بدون گرد کردن: 1.5 + 2.5 + 3.5 + 4.5 = 12.
-   گرد کردن بانکدار: 2 + 2 + 4 + 4 = 12.
-   گرد کردن به نزدیکترین عدد صحیح: 2 + 3 + 4 + 5 = 14.

**نحو**

``` sql
roundBankers(expression [, decimal_places])
```

**پارامترها**

-   `expression` — A number to be rounded. Can be any [عبارت](../syntax.md#syntax-expressions) بازگشت عددی [نوع داده](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — Decimal places. An integer number.
    -   `decimal-places > 0` — The function rounds the number to the given position right of the decimal point. Example: `roundBankers(3.55, 1) = 3.6`.
    -   `decimal-places < 0` — The function rounds the number to the given position left of the decimal point. Example: `roundBankers(24.55, -1) = 20`.
    -   `decimal-places = 0` — The function rounds the number to an integer. In this case the argument can be omitted. Example: `roundBankers(2.5) = 2`.

**مقدار بازگشتی**

ارزش گرد شده توسط روش گرد کردن بانکدار.

### مثالها {#examples-1}

**مثال استفاده**

پرسوجو:

``` sql
 SELECT number / 2 AS x, roundBankers(x, 0) AS b fROM system.numbers limit 10
```

نتیجه:

``` text
┌───x─┬─b─┐
│   0 │ 0 │
│ 0.5 │ 0 │
│   1 │ 1 │
│ 1.5 │ 2 │
│   2 │ 2 │
│ 2.5 │ 2 │
│   3 │ 3 │
│ 3.5 │ 4 │
│   4 │ 4 │
│ 4.5 │ 4 │
└─────┴───┘
```

**نمونه هایی از گرد کردن بانکدار**

``` text
roundBankers(0.4) = 0
roundBankers(-3.5) = -4
roundBankers(4.5) = 4
roundBankers(3.55, 1) = 3.6
roundBankers(3.65, 1) = 3.6
roundBankers(10.35, 1) = 10.4
roundBankers(10.755, 2) = 11,76
```

**همچنین نگاه کنید به**

-   [گرد](#rounding_functions-round)

## توسعه پایدار2) {#roundtoexp2num}

می پذیرد تعداد. اگر تعداد کمتر از یک است, باز می گردد 0. در غیر این صورت, این دور تعداد پایین به نزدیکترین (مجموع غیر منفی) درجه دو.

## طول عمر (تعداد) {#rounddurationnum}

می پذیرد تعداد. اگر تعداد کمتر از یک است, باز می گردد 0. در غیر این صورت, این دور تعداد را به اعداد از مجموعه: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. این تابع خاص به یاندکس است.متریکا و مورد استفاده برای اجرای گزارش در طول جلسه.

## عدد) {#roundagenum}

می پذیرد تعداد. اگر تعداد کمتر از است 18, باز می گردد 0. در غیر این صورت, این دور تعداد را به یک عدد از مجموعه: 18, 25, 35, 45, 55. این تابع خاص به یاندکس است.متریکا و مورد استفاده برای اجرای گزارش در سن کاربر.

## roundDown(num arr) {#rounddownnum-arr}

یک عدد را می پذیرد و به یک عنصر در مجموعه مشخص شده منتقل می کند. اگر مقدار کمتر از پایین ترین حد محدود است, پایین ترین حد بازگشته است.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/rounding_functions/) <!--hide-->
