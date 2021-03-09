---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "\u0628\u0627\u0644\u0627\u062A\u0631 \u0633\u0641\u0627\u0631\u0634"
---

# توابع مرتبه بالاتر {#higher-order-functions}

## `->` اپراتور, لامبدا (پارامترهای, اکسپر) تابع {#operator-lambdaparams-expr-function}

Allows describing a lambda function for passing to a higher-order function. The left side of the arrow has a formal parameter, which is any ID, or multiple formal parameters – any IDs in a tuple. The right side of the arrow has an expression that can use these formal parameters, as well as any table columns.

مثالها: `x -> 2 * x, str -> str != Referer.`

توابع مرتبه بالاتر تنها می تواند توابع لامبدا به عنوان استدلال عملکردی خود را قبول.

یک تابع لامبدا که استدلال های متعدد می پذیرد را می توان به یک تابع مرتبه بالاتر منتقل می شود. در این مورد, تابع مرتبه بالاتر به تصویب می رسد چند مجموعه ای از طول یکسان است که این استدلال به مطابقت.

برای برخی از توابع مانند [پیاده کردن](#higher_order_functions-array-count) یا [ارریسوم](#higher_order_functions-array-count), استدلال اول (تابع لامبدا) را می توان حذف. در این مورد, نقشه برداری یکسان فرض شده است.

یک تابع لامبدا را نمی توان برای توابع زیر حذف کرد:

-   [اررایماپ](#higher_order_functions-array-map)
-   [شلوار لی](#higher_order_functions-array-filter)
-   [شلوار لی](#higher_order_functions-array-fill)
-   [نمایش سایت](#higher_order_functions-array-reverse-fill)
-   [لرزش دستگاه](#higher_order_functions-array-split)
-   [نمایش سایت](#higher_order_functions-array-reverse-split)
-   [دریافیرست](#higher_order_functions-array-first)
-   [نمایش سایت](#higher_order_functions-array-first-index)

### arrayMap(func, arr1, …) {#higher_order_functions-array-map}

بازگرداندن مجموعه ای از برنامه اصلی `func` عملکرد به هر عنصر در `arr` صف کردن.

مثالها:

``` sql
SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,4,5] │
└─────────┘
```

مثال زیر نشان می دهد که چگونه برای ایجاد یک تاپل از عناصر از مجموعه های مختلف:

``` sql
SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res
```

``` text
┌─res─────────────────┐
│ [(1,4),(2,5),(3,6)] │
└─────────────────────┘
```

توجه داشته باشید که استدلال اول (تابع لامبدا) را نمی توان در حذف `arrayMap` تابع.

### arrayFilter(func, arr1, …) {#higher_order_functions-array-filter}

بازگرداندن مجموعه ای حاوی تنها عناصر در `arr1` برای کدام `func` چیزی غیر از 0 را برمی گرداند.

مثالها:

``` sql
SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res
```

``` text
┌─res───────────┐
│ ['abc World'] │
└───────────────┘
```

``` sql
SELECT
    arrayFilter(
        (i, x) -> x LIKE '%World%',
        arrayEnumerate(arr),
        ['Hello', 'abc World'] AS arr)
    AS res
```

``` text
┌─res─┐
│ [2] │
└─────┘
```

توجه داشته باشید که استدلال اول (تابع لامبدا) را نمی توان در حذف `arrayFilter` تابع.

### arrayFill(func, arr1, …) {#higher_order_functions-array-fill}

پویش از طریق `arr1` از عنصر اول به عنصر گذشته و جایگزین `arr1[i]` توسط `arr1[i - 1]` اگر `func` بازده 0. اولین عنصر `arr1` جایگزین نخواهد شد.

مثالها:

``` sql
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res──────────────────────────────┐
│ [1,1,3,11,12,12,12,5,6,14,14,14] │
└──────────────────────────────────┘
```

توجه داشته باشید که استدلال اول (تابع لامبدا) را نمی توان در حذف `arrayFill` تابع.

### arrayReverseFill(func, arr1, …) {#higher_order_functions-array-reverse-fill}

پویش از طریق `arr1` از عنصر گذشته به عنصر اول و جایگزین `arr1[i]` توسط `arr1[i + 1]` اگر `func` بازده 0. عنصر گذشته از `arr1` جایگزین نخواهد شد.

مثالها:

``` sql
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res────────────────────────────────┐
│ [1,3,3,11,12,5,5,5,6,14,NULL,NULL] │
└────────────────────────────────────┘
```

توجه داشته باشید که استدلال اول (تابع لامبدا) را نمی توان در حذف `arrayReverseFill` تابع.

### arraySplit(func, arr1, …) {#higher_order_functions-array-split}

شکافتن `arr1` به چندین ردیف چه زمانی `func` بازگرداندن چیزی غیر از 0, مجموعه خواهد شد در سمت چپ عنصر تقسیم. مجموعه قبل از اولین عنصر تقسیم نخواهد شد.

مثالها:

``` sql
SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res─────────────┐
│ [[1,2,3],[4,5]] │
└─────────────────┘
```

توجه داشته باشید که استدلال اول (تابع لامبدا) را نمی توان در حذف `arraySplit` تابع.

### arrayReverseSplit(func, arr1, …) {#higher_order_functions-array-reverse-split}

شکافتن `arr1` به چندین ردیف چه زمانی `func` بازگرداندن چیزی غیر از 0, مجموعه خواهد شد در سمت راست عنصر تقسیم. مجموعه پس از عنصر گذشته تقسیم نخواهد شد.

مثالها:

``` sql
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res───────────────┐
│ [[1],[2,3,4],[5]] │
└───────────────────┘
```

توجه داشته باشید که استدلال اول (تابع لامبدا) را نمی توان در حذف `arraySplit` تابع.

### arrayCount(\[func,\] arr1, …) {#higher_order_functions-array-count}

بازگرداندن تعدادی از عناصر در مجموعه ارر که فارک چیزی غیر از گرداند 0. اگر ‘func’ مشخص نشده است, تعداد عناصر غیر صفر گرداند در مجموعه.

### arrayExists(\[func,\] arr1, …) {#arrayexistsfunc-arr1}

بازده 1 اگر حداقل یک عنصر در وجود دارد ‘arr’ برای کدام ‘func’ چیزی غیر از 0 را برمی گرداند. در غیر این صورت, باز می گردد 0.

### arrayAll(\[func,\] arr1, …) {#arrayallfunc-arr1}

بازده 1 اگر ‘func’ چیزی غیر از 0 را برای تمام عناصر در ‘arr’. در غیر این صورت, باز می گردد 0.

### arraySum(\[func,\] arr1, …) {#higher-order-functions-array-sum}

بازگرداندن مجموع ‘func’ ارزشهای خبری عبارتند از: اگر تابع حذف شده است, فقط می گرداند مجموع عناصر مجموعه.

### arrayFirst(func, arr1, …) {#higher_order_functions-array-first}

بازگرداندن اولین عنصر در ‘arr1’ تنظیم برای کدام ‘func’ چیزی غیر از 0 را برمی گرداند.

توجه داشته باشید که استدلال اول (تابع لامبدا) را نمی توان در حذف `arrayFirst` تابع.

### arrayFirstIndex(func, arr1, …) {#higher_order_functions-array-first-index}

بازگرداندن شاخص عنصر اول در ‘arr1’ تنظیم برای کدام ‘func’ چیزی غیر از 0 را برمی گرداند.

توجه داشته باشید که استدلال اول (تابع لامبدا) را نمی توان در حذف `arrayFirstIndex` تابع.

### arrayCumSum(\[func,\] arr1, …) {#arraycumsumfunc-arr1}

بازگرداندن مجموعه ای از مبالغ بخشی از عناصر در مجموعه منبع (مجموع در حال اجرا). اگر `func` تابع مشخص شده است, سپس ارزش عناصر مجموعه توسط این تابع قبل از جمع تبدیل.

مثال:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

``` text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

### هشدار داده می شود) {#arraycumsumnonnegativearr}

مثل `arrayCumSum`, بازگرداندن مجموعه ای از مبالغ بخشی از عناصر در مجموعه منبع (مجموع در حال اجرا). متفاوت `arrayCumSum`, هنگامی که ارزش سپس بازگشت شامل یک مقدار کمتر از صفر, ارزش است جایگزین با صفر و محاسبه بعدی با صفر پارامترهای انجام. به عنوان مثال:

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

``` text
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```

### arraySort(\[func,\] arr1, …) {#arraysortfunc-arr1}

بازگرداندن مجموعه ای به عنوان نتیجه مرتب سازی عناصر `arr1` به ترتیب صعودی. اگر `func` تابع مشخص شده است, مرتب سازی سفارش توسط نتیجه تابع تعیین `func` اعمال شده به عناصر مجموعه (ارریس)

این [تبدیل شوارتز](https://en.wikipedia.org/wiki/Schwartzian_transform) برای بهبود کارایی مرتب سازی استفاده می شود.

مثال:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

برای کسب اطلاعات بیشتر در مورد `arraySort` روش, دیدن [توابع برای کار با ارریس](array-functions.md#array_functions-sort) بخش.

### arrayReverseSort(\[func,\] arr1, …) {#arrayreversesortfunc-arr1}

بازگرداندن مجموعه ای به عنوان نتیجه مرتب سازی عناصر `arr1` به ترتیب نزولی. اگر `func` تابع مشخص شده است, مرتب سازی سفارش توسط نتیجه تابع تعیین `func` اعمال شده به عناصر مجموعه ای (ارریس).

مثال:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

برای کسب اطلاعات بیشتر در مورد `arrayReverseSort` روش, دیدن [توابع برای کار با ارریس](array-functions.md#array_functions-reverse-sort) بخش.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/higher_order_functions/) <!--hide-->
