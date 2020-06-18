---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# مجموعه پیوستن بند {#select-array-join-clause}

این یک عملیات مشترک برای جداول است که حاوی یک ستون جداگانه برای تولید یک جدول جدید است که دارای یک ستون با هر عنصر جداگانه ای از ستون اولیه است در حالی که مقادیر ستون های دیگر تکرار می شوند. این مورد اساسی چه چیزی است `ARRAY JOIN` بند کند.

نام خود را از این واقعیت است که می تواند در به عنوان اجرای نگاه `JOIN` با یک مجموعه یا ساختار داده های تو در تو. قصد شبیه به است [ارریجین](../../functions/array-join.md#functions_arrayjoin) تابع, اما قابلیت بند گسترده تر است.

نحو:

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

شما می توانید تنها یک مشخص `ARRAY JOIN` بند در یک `SELECT` پرس و جو.

انواع پشتیبانی شده از `ARRAY JOIN` به شرح زیر است:

-   `ARRAY JOIN` - در مورد پایه, بند خالی در نتیجه شامل نمی شود `JOIN`.
-   `LEFT ARRAY JOIN` - نتیجه `JOIN` شامل ردیف با ارریس خالی است. مقدار برای یک مجموعه خالی است به مقدار پیش فرض برای نوع عنصر مجموعه (معمولا 0, رشته خالی و یا تهی).

## پایه صف پیوستن به نمونه {#basic-array-join-examples}

نمونه های زیر نشان می دهد استفاده از `ARRAY JOIN` و `LEFT ARRAY JOIN` بند. بیایید یک جدول با یک [& حذف](../../../sql-reference/data-types/array.md) ستون را تایپ کنید و مقادیر را وارد کنید:

``` sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

``` text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

مثال زیر از `ARRAY JOIN` بند:

``` sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

``` text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

مثال بعدی با استفاده از `LEFT ARRAY JOIN` بند:

``` sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

``` text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

## استفاده از نام مستعار {#using-aliases}

یک نام مستعار می تواند برای مجموعه ای در `ARRAY JOIN` بند بند. در این مورد, یک مورد مجموعه ای را می توان با این نام مستعار دیده, اما مجموعه خود را با نام اصلی قابل دسترسی. مثال:

``` sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

``` text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

با استفاده از نام مستعار, شما می توانید انجام `ARRAY JOIN` با یک مجموعه خارجی. به عنوان مثال:

``` sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

``` text
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

ارریس های متعدد را می توان با کاما از هم جدا در `ARRAY JOIN` بند بند. در این مورد, `JOIN` به طور همزمان (مجموع مستقیم و نه محصول دکارتی) انجام می شود. توجه داشته باشید که تمام ارریس باید به همان اندازه. مثال:

``` sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

مثال زیر از [شناسه بسته:](../../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) تابع:

``` sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

## مجموعه پیوستن با ساختار داده های تو در تو {#array-join-with-nested-data-structure}

`ARRAY JOIN` همچنین با این نسخهها کار [ساختارهای داده تو در تو](../../../sql-reference/data-types/nested-data-structures/nested.md):

``` sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

``` text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

هنگام مشخص کردن نام ساختارهای داده های تو در تو در `ARRAY JOIN`, معنای همان است که `ARRAY JOIN` با تمام عناصر مجموعه ای که شامل. نمونه به شرح زیر است:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

این تنوع نیز حس می کند:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

نام مستعار ممکن است برای یک ساختار داده های تو در تو استفاده می شود, به منظور انتخاب هر دو `JOIN` نتیجه یا مجموعه منبع. مثال:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

نمونه ای از استفاده از [شناسه بسته:](../../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) تابع:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

## پیاده سازی اطلاعات {#implementation-details}

سفارش اجرای پرس و جو در هنگام اجرا بهینه شده است `ARRAY JOIN`. اگرچه `ARRAY JOIN` همیشه باید قبل از مشخص شود [WHERE](where.md)/[PREWHERE](prewhere.md) بند در پرس و جو, از لحاظ فنی می تواند در هر سفارش انجام, مگر اینکه نتیجه `ARRAY JOIN` برای فیلتر کردن استفاده می شود. سفارش پردازش توسط بهینه ساز پرس و جو کنترل می شود.
