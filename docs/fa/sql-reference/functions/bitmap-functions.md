---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: "\u0646\u06AF\u0627\u0634\u062A \u0628\u06CC\u062A"
---

# توابع نگاشت بیت {#bitmap-functions}

توابع بیت مپ برای دو بیت مپ محاسبه ارزش شی کار, این است که بازگشت بیت مپ جدید و یا کارتیت در حالی که با استفاده از محاسبه فرمول, مانند و, یا, صخره نوردی, و نه, و غیره.

2 نوع از روش های ساخت و ساز برای شی بیت مپ وجود دارد. یکی این است که توسط گروه بیت مپ تابع تجمع با دولت ساخته شود, دیگر این است که توسط شی مجموعه ای ساخته شود. این نیز برای تبدیل شی بیت مپ به مجموعه شی.

نقشه شهری روارینگ به یک ساختار داده در حالی که ذخیره سازی واقعی از اجسام بیت مپ پیچیده شده است. هنگامی که کارتیت کمتر از یا برابر است 32, با استفاده از عینیت مجموعه. هنگامی که کارتیت بیشتر از است 32, با استفاده از شی نقشه شهری روارینگ. به همین دلیل است ذخیره سازی مجموعه کارتیت کم سریع تر است.

برای کسب اطلاعات بیشتر در مورد نقشه شهری روارینگ: [پرورش دهنده](https://github.com/RoaringBitmap/CRoaring).

## طراحی بیت مپ {#bitmap_functions-bitmapbuild}

ساخت یک بیت مپ از مجموعه عدد صحیح بدون علامت.

``` sql
bitmapBuild(array)
```

**پارامترها**

-   `array` – unsigned integer array.

**مثال**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res)
```

``` text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)    │
└─────┴──────────────────────────────────────────────┘
```

## بیت مپوری {#bitmaptoarray}

تبدیل بیت مپ به مجموعه عدد صحیح.

``` sql
bitmapToArray(bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## اطلاعات دقیق {#bitmap-functions-bitmapsubsetinrange}

زیرمجموعه بازگشت در محدوده مشخص شده (دامنه را شامل نمی شود).

``` sql
bitmapSubsetInRange(bitmap, range_start, range_end)
```

**پارامترها**

-   `bitmap` – [شی نگاشت بیت](#bitmap_functions-bitmapbuild).
-   `range_start` – range start point. Type: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `range_end` – range end point(excluded). Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**مثال**

``` sql
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

``` text
┌─res───────────────┐
│ [30,31,32,33,100] │
└───────────────────┘
```

## نمایش سایت {#bitmapsubsetlimit}

ایجاد یک زیر مجموعه از بیت مپ با عناصر نفر گرفته شده بین `range_start` و `cardinality_limit`.

**نحو**

``` sql
bitmapSubsetLimit(bitmap, range_start, cardinality_limit)
```

**پارامترها**

-   `bitmap` – [شی نگاشت بیت](#bitmap_functions-bitmapbuild).
-   `range_start` – The subset starting point. Type: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `cardinality_limit` – The subset cardinality upper limit. Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**مقدار بازگشتی**

زیرمجموعه.

نوع: `Bitmap object`.

**مثال**

پرسوجو:

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

نتیجه:

``` text
┌─res───────────────────────┐
│ [30,31,32,33,100,200,500] │
└───────────────────────────┘
```

## اطلاعات دقیق {#bitmap_functions-bitmapcontains}

بررسی اینکه نگاشت بیت شامل یک عنصر است.

``` sql
bitmapContains(haystack, needle)
```

**پارامترها**

-   `haystack` – [شی نگاشت بیت](#bitmap_functions-bitmapbuild), جایی که تابع جستجو.
-   `needle` – Value that the function searches. Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**مقادیر بازگشتی**

-   0 — If `haystack` شامل نمی شود `needle`.
-   1 — If `haystack` شامل `needle`.

نوع: `UInt8`.

**مثال**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## بیتمافاسانی {#bitmaphasany}

بررسی اینکه دو بیت مپ دارند تقاطع توسط برخی از عناصر.

``` sql
bitmapHasAny(bitmap1, bitmap2)
```

اگر شما اطمینان حاصل کنید که `bitmap2` حاوی شدت یک عنصر, در نظر با استفاده از [اطلاعات دقیق](#bitmap_functions-bitmapcontains) تابع. این کار موثر تر است.

**پارامترها**

-   `bitmap*` – bitmap object.

**مقادیر بازگشتی**

-   `1` اگر `bitmap1` و `bitmap2` حداقل یک عنصر مشابه داشته باشید.
-   `0` وگرنه

**مثال**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## بیتمافاسال {#bitmaphasall}

مشابه به `hasAll(array, array)` بازده 1 اگر بیت مپ اول شامل تمام عناصر از یک ثانیه, 0 در غیر این صورت.
اگر استدلال دوم بیت مپ خالی است و سپس باز می گردد 1.

``` sql
bitmapHasAll(bitmap,bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  0  │
└─────┘
```

## هشدار داده می شود {#bitmapcardinality}

Retrun بیت مپ cardinality از نوع UInt64.

``` sql
bitmapCardinality(bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## بیت مپمن {#bitmapmin}

Retrun کوچکترین مقدار از نوع UInt64 در مجموعه UINT32_MAX اگر این مجموعه خالی است.

    bitmapMin(bitmap)

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   1 │
    └─────┘

## جرم اتمی {#bitmapmax}

جابجایی بزرگترین ارزش نوع اوینت64 در مجموعه, 0 اگر مجموعه ای خالی است.

    bitmapMax(bitmap)

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   5 │
    └─────┘

## ترجمههای بیت مپ {#bitmaptransform}

تبدیل مجموعه ای از ارزش ها در بیت مپ به مجموعه ای دیگر از ارزش, نتیجه یک بیت مپ جدید است.

    bitmapTransform(bitmap, from_array, to_array)

**پارامترها**

-   `bitmap` – bitmap object.
-   `from_array` – UInt32 array. For idx in range \[0, from_array.size()), if bitmap contains from_array\[idx\], then replace it with to_array\[idx\]. Note that the result depends on array ordering if there are common elements between from_array and to_array.
-   `to_array` – UInt32 array, its size shall be the same to from_array.

**مثال**

``` sql
SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cast([5,999,2] as Array(UInt32)), cast([2,888,20] as Array(UInt32)))) AS res
```

    ┌─res───────────────────┐
    │ [1,3,4,6,7,8,9,10,20] │
    └───────────────────────┘

## بیت مپند {#bitmapand}

دو بیت مپ و محاسبه, نتیجه یک بیت مپ جدید است.

``` sql
bitmapAnd(bitmap,bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─┐
│ [3] │
└─────┘
```

## نگاشت بیت {#bitmapor}

دو بیت مپ و یا محاسبه, نتیجه یک بیت مپ جدید است.

``` sql
bitmapOr(bitmap,bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## بیت مپکسور {#bitmapxor}

دو محاسبه گز بیت مپ, نتیجه یک بیت مپ جدید است.

``` sql
bitmapXor(bitmap,bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## بیت مپندو {#bitmapandnot}

دو محاسبه بیت مپ اندنوت, نتیجه یک بیت مپ جدید است.

``` sql
bitmapAndnot(bitmap,bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## اطلاعات دقیق {#bitmapandcardinality}

دو بیت مپ و محاسبه بازگشت cardinality از نوع UInt64.

``` sql
bitmapAndCardinality(bitmap,bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## کمبود سیگار {#bitmaporcardinality}

دو بیت مپ و یا محاسبه بازگشت cardinality از نوع UInt64.

``` sql
bitmapOrCardinality(bitmap,bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## هشدار داده می شود {#bitmapxorcardinality}

دو بیت مپ xor محاسبه بازگشت cardinality از نوع UInt64.

``` sql
bitmapXorCardinality(bitmap,bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   4 │
└─────┘
```

## اطلاعات دقیق {#bitmapandnotcardinality}

دو بیت مپ andnot محاسبه بازگشت cardinality از نوع UInt64.

``` sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**پارامترها**

-   `bitmap` – bitmap object.

**مثال**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   2 │
└─────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/bitmap_functions/) <!--hide-->
