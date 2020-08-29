---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: "\u0628\u06CC\u062A"
---

# توابع بیت {#bit-functions}

بیت توابع کار برای هر جفت از انواع از UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32 یا Float64.

نوع نتیجه یک عدد صحیح با بیت به حداکثر بیت از استدلال خود را برابر است. اگر حداقل یکی از استدلال امضا شده است, نتیجه یک شماره امضا شده است. اگر استدلال یک عدد ممیز شناور است, این است که به درون بازیگران64.

## بیت و ب) {#bitanda-b}

## bitOr(a, b) {#bitora-b}

## هشدار داده می شود) {#bitxora-b}

## bitNot(یک) {#bitnota}

## اطلاعات دقیق) {#bitshiftlefta-b}

## باز کردن پنجره روی برنامههای دیگر) {#bitshiftrighta-b}

## هشدار داده می شود) {#bitrotatelefta-b}

## حفاظت از بیت) {#bitrotaterighta-b}

## بیتترین {#bittest}

طول می کشد هر عدد صحیح و تبدیل به [شکل دودویی](https://en.wikipedia.org/wiki/Binary_number), بازگرداندن ارزش کمی در موقعیت مشخص. شمارش معکوس از 0 از سمت راست به سمت چپ شروع می شود.

**نحو**

``` sql
SELECT bitTest(number, index)
```

**پارامترها**

-   `number` – integer number.
-   `index` – position of bit.

**مقادیر بازگشتی**

بازگرداندن مقدار کمی در موقعیت مشخص.

نوع: `UInt8`.

**مثال**

مثلا, تعداد 43 در پایه-2 (دودویی) سیستم اعداد است 101011.

پرسوجو:

``` sql
SELECT bitTest(43, 1)
```

نتیجه:

``` text
┌─bitTest(43, 1)─┐
│              1 │
└────────────────┘
```

مثال دیگر:

پرسوجو:

``` sql
SELECT bitTest(43, 2)
```

نتیجه:

``` text
┌─bitTest(43, 2)─┐
│              0 │
└────────────────┘
```

## تماس {#bittestall}

بازده نتیجه [ساخت منطقی](https://en.wikipedia.org/wiki/Logical_conjunction) (و اپراتور) از تمام بیت در موقعیت های داده شده. شمارش معکوس از 0 از سمت راست به سمت چپ شروع می شود.

ساخت و ساز برای عملیات بیتی:

0 AND 0 = 0

0 AND 1 = 0

1 AND 0 = 0

1 AND 1 = 1

**نحو**

``` sql
SELECT bitTestAll(number, index1, index2, index3, index4, ...)
```

**پارامترها**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit. For example, for set of positions (`index1`, `index2`, `index3`, `index4`) درست است اگر و تنها اگر تمام موقعیت خود را درست هستند (`index1` ⋀ `index2`, ⋀ `index3` ⋀ `index4`).

**مقادیر بازگشتی**

بازده نتیجه منطقی conjuction.

نوع: `UInt8`.

**مثال**

مثلا, تعداد 43 در پایه-2 (دودویی) سیستم اعداد است 101011.

پرسوجو:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5)
```

نتیجه:

``` text
┌─bitTestAll(43, 0, 1, 3, 5)─┐
│                          1 │
└────────────────────────────┘
```

مثال دیگر:

پرسوجو:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5, 2)
```

نتیجه:

``` text
┌─bitTestAll(43, 0, 1, 3, 5, 2)─┐
│                             0 │
└───────────────────────────────┘
```

## بیتستانی {#bittestany}

بازده نتیجه [حکم منطقی](https://en.wikipedia.org/wiki/Logical_disjunction) (یا اپراتور) از تمام بیت در موقعیت های داده شده. شمارش معکوس از 0 از سمت راست به سمت چپ شروع می شود.

دستور برای عملیات بیتی:

0 OR 0 = 0

0 OR 1 = 1

1 OR 0 = 1

1 OR 1 = 1

**نحو**

``` sql
SELECT bitTestAny(number, index1, index2, index3, index4, ...)
```

**پارامترها**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit.

**مقادیر بازگشتی**

بازده نتیجه ساخت و ساز منطقی.

نوع: `UInt8`.

**مثال**

مثلا, تعداد 43 در پایه-2 (دودویی) سیستم اعداد است 101011.

پرسوجو:

``` sql
SELECT bitTestAny(43, 0, 2)
```

نتیجه:

``` text
┌─bitTestAny(43, 0, 2)─┐
│                    1 │
└──────────────────────┘
```

مثال دیگر:

پرسوجو:

``` sql
SELECT bitTestAny(43, 4, 2)
```

نتیجه:

``` text
┌─bitTestAny(43, 4, 2)─┐
│                    0 │
└──────────────────────┘
```

## شمارش {#bitcount}

محاسبه تعداد بیت را به یکی در نمایندگی دودویی از یک عدد است.

**نحو**

``` sql
bitCount(x)
```

**پارامترها**

-   `x` — [عدد صحیح](../../sql-reference/data-types/int-uint.md) یا [شناور نقطه](../../sql-reference/data-types/float.md) شماره. تابع با استفاده از نمایندگی ارزش در حافظه. این اجازه می دهد تا حمایت از اعداد ممیز شناور.

**مقدار بازگشتی**

-   تعداد بیت را به یکی در تعداد ورودی.

تابع مقدار ورودی را به یک نوع بزرگتر تبدیل نمی کند ([ثبت نام پسوند](https://en.wikipedia.org/wiki/Sign_extension)). بنابراین, مثلا, `bitCount(toUInt8(-1)) = 8`.

نوع: `UInt8`.

**مثال**

نگاهی به عنوان مثال تعداد 333. نمایندگی دودویی: 00000001001101.

پرسوجو:

``` sql
SELECT bitCount(333)
```

نتیجه:

``` text
┌─bitCount(333)─┐
│             5 │
└───────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/bit_functions/) <!--hide-->
