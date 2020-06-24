---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 47
toc_title: "\u062A\u0642\u0633\u06CC\u0645 \u0648 \u0627\u062F\u063A\u0627\u0645 \u0631\
  \u0634\u062A\u0647 \u0647\u0627 \u0648 \u0627\u0631\u0631\u06CC\u0633"
---

# توابع برای تقسیم و ادغام رشته ها و ارریس {#functions-for-splitting-and-merging-strings-and-arrays}

## اسپلیت بیچار (جداساز) {#splitbycharseparator-s}

انشعابات یک رشته به بسترهای جدا شده توسط یک شخصیت مشخص شده است. با استفاده از یک رشته ثابت `separator` که متشکل از دقیقا یک شخصیت.
بازگرداندن مجموعه ای از بسترهای انتخاب. بسترهای خالی ممکن است انتخاب شود اگر جدا در ابتدا یا انتهای رشته رخ می دهد, و یا اگر چند جداکننده متوالی وجود دارد.

**نحو**

``` sql
splitByChar(<separator>, <s>)
```

**پارامترها**

-   `separator` — The separator which should contain exactly one character. [رشته](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [رشته](../../sql-reference/data-types/string.md).

**مقدار بازگشتی)**

بازگرداندن مجموعه ای از بسترهای انتخاب. بسترهای خالی ممکن است انتخاب شود که:

-   جداساز در ابتدا یا انتهای رشته رخ می دهد;
-   چندین جداکننده متوالی وجود دارد;
-   رشته اصلی `s` خالیه

نوع: [& حذف](../../sql-reference/data-types/array.md) از [رشته](../../sql-reference/data-types/string.md).

**مثال**

``` sql
SELECT splitByChar(',', '1,2,3,abcde')
```

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## رشته اسپلیتبیست (جداساز) {#splitbystringseparator-s}

انشعابات یک رشته به بسترهای جدا شده توسط یک رشته. با استفاده از یک رشته ثابت `separator` از شخصیت های متعدد به عنوان جدا کننده. اگر رشته `separator` خالی است, این رشته تقسیم `s` به مجموعه ای از شخصیت های تک.

**نحو**

``` sql
splitByString(<separator>, <s>)
```

**پارامترها**

-   `separator` — The separator. [رشته](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [رشته](../../sql-reference/data-types/string.md).

**مقدار بازگشتی)**

بازگرداندن مجموعه ای از بسترهای انتخاب. بسترهای خالی ممکن است انتخاب شود که:

نوع: [& حذف](../../sql-reference/data-types/array.md) از [رشته](../../sql-reference/data-types/string.md).

-   جدا کننده غیر خالی در ابتدا یا انتهای رشته رخ می دهد;
-   چند جدا متوالی غیر خالی وجود دارد;
-   رشته اصلی `s` خالی است در حالی که جدا خالی نیست.

**مثال**

``` sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde')
```

``` text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

``` sql
SELECT splitByString('', 'abcde')
```

``` text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## حذف میانبر در صفحه خانه\]) {#arraystringconcatarr-separator}

رشته های ذکر شده در مجموعه را با جداساز مطابقت می دهد.'جدا کننده' پارامتر اختیاری است: یک رشته ثابت, مجموعه ای به یک رشته خالی به طور پیش فرض.
رشته را برمی گرداند.

## اطلاعات دقیق) {#alphatokenss}

انتخاب substrings متوالی بایت از محدوده a-z و A-Z. بازگرداندن یک آرایه از substrings.

**مثال**

``` sql
SELECT alphaTokens('abca1abc')
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/splitting_merging_functions/) <!--hide-->
