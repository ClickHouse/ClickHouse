---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "\u0628\u0631\u0627\u06CC \u062C\u0633\u062A\u062C\u0648\u06CC \u0631\u0634\
  \u062A\u0647\u0647\u0627"
---

# توابع برای جستجوی رشته ها {#functions-for-searching-strings}

جستجو به طور پیش فرض در تمام این توابع حساس به حروف است. انواع جداگانه ای برای جستجوی غیر حساس مورد وجود دارد.

## موقعیت (انبار کاه, سوزن), تعیین محل (انبار کاه, سوزن) {#position}

بازگرداندن موقعیت (به بایت) از رشته پیدا شده است در رشته, با شروع از 1.

این نسخهها کار میکند با این فرض که رشته شامل مجموعه ای از بایت به نمایندگی از یک متن کد گذاری تک بایت. اگر این فرض ملاقات کرد و یک شخصیت نمی تواند با استفاده از یک بایت تنها نشان داده شود, تابع یک استثنا پرتاب نمی کند و برخی از نتیجه غیر منتظره را برمی گرداند. اگر شخصیت را می توان با استفاده از دو بایت نشان, این دو بایت و غیره استفاده.

برای یک جستجو غیر حساس به حالت, استفاده از تابع [حساس به حالت](#positioncaseinsensitive).

**نحو**

``` sql
position(haystack, needle[, start_pos])
```

نام مستعار: `locate(haystack, needle[, start_pos])`.

**پارامترها**

-   `haystack` — string, in which substring will to be searched. [رشته](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [رشته](../syntax.md#syntax-string-literal).
-   `start_pos` – Optional parameter, position of the first character in the string to start search. [UInt](../../sql-reference/data-types/int-uint.md)

**مقادیر بازگشتی**

-   موقعیت شروع در بایت (شمارش از 1), اگر زیر رشته پیدا شد.
-   0, اگر زیر رشته یافت نشد.

نوع: `Integer`.

**مثالها**

عبارت “Hello, world!” شامل مجموعه ای از بایت به نمایندگی از یک متن کد گذاری تک بایت. تابع بازده برخی از نتیجه انتظار می رود:

پرسوجو:

``` sql
SELECT position('Hello, world!', '!')
```

نتیجه:

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

همان عبارت در روسیه شامل شخصیت های که نمی تواند با استفاده از یک بایت نشان داده شود. تابع بازده برخی از نتیجه غیر منتظره (استفاده [موقعیت 8](#positionutf8) تابع برای متن چند بایت کد گذاری):

پرسوجو:

``` sql
SELECT position('Привет, мир!', '!')
```

نتیجه:

``` text
┌─position('Привет, мир!', '!')─┐
│                            21 │
└───────────────────────────────┘
```

## حساس به حالت {#positioncaseinsensitive}

همان [موقعیت](#position) بازگرداندن موقعیت (به بایت) از رشته پیدا شده است در رشته, با شروع از 1. استفاده از تابع برای یک جستجو غیر حساس به حالت.

این نسخهها کار میکند با این فرض که رشته شامل مجموعه ای از بایت به نمایندگی از یک متن کد گذاری تک بایت. اگر این فرض ملاقات کرد و یک شخصیت نمی تواند با استفاده از یک بایت تنها نشان داده شود, تابع یک استثنا پرتاب نمی کند و برخی از نتیجه غیر منتظره را برمی گرداند. اگر شخصیت را می توان با استفاده از دو بایت نشان, این دو بایت و غیره استفاده.

**نحو**

``` sql
positionCaseInsensitive(haystack, needle[, start_pos])
```

**پارامترها**

-   `haystack` — string, in which substring will to be searched. [رشته](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [رشته](../syntax.md#syntax-string-literal).
-   `start_pos` – Optional parameter, position of the first character in the string to start search. [UInt](../../sql-reference/data-types/int-uint.md)

**مقادیر بازگشتی**

-   موقعیت شروع در بایت (شمارش از 1), اگر زیر رشته پیدا شد.
-   0, اگر زیر رشته یافت نشد.

نوع: `Integer`.

**مثال**

پرسوجو:

``` sql
SELECT positionCaseInsensitive('Hello, world!', 'hello')
```

نتیجه:

``` text
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
```

## موقعیت 8 {#positionutf8}

بازگرداندن موقعیت (در نقاط یونیکد) از رشته پیدا شده است در رشته, با شروع از 1.

این نسخهها کار میکند با این فرض که رشته شامل مجموعه ای از بایت به نمایندگی از یک متن کد گذاری شده وزارت مخابرات 8. اگر این فرض ملاقات نمی, تابع یک استثنا پرتاب نمی کند و برخی از نتیجه غیر منتظره را برمی گرداند. اگر شخصیت را می توان با استفاده از دو نقطه یونیکد نشان, این دو و غیره استفاده.

برای یک جستجو غیر حساس به حالت, استفاده از تابع [در حال بارگذاری](#positioncaseinsensitiveutf8).

**نحو**

``` sql
positionUTF8(haystack, needle[, start_pos])
```

**پارامترها**

-   `haystack` — string, in which substring will to be searched. [رشته](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [رشته](../syntax.md#syntax-string-literal).
-   `start_pos` – Optional parameter, position of the first character in the string to start search. [UInt](../../sql-reference/data-types/int-uint.md)

**مقادیر بازگشتی**

-   موقعیت شروع در یونیکد امتیاز (شمارش از 1), اگر زیر رشته پیدا شد.
-   0, اگر زیر رشته یافت نشد.

نوع: `Integer`.

**مثالها**

عبارت “Hello, world!” در روسیه شامل مجموعه ای از نقاط یونیکد نمایندگی یک متن کد گذاری تک نقطه. تابع بازده برخی از نتیجه انتظار می رود:

پرسوجو:

``` sql
SELECT positionUTF8('Привет, мир!', '!')
```

نتیجه:

``` text
┌─positionUTF8('Привет, мир!', '!')─┐
│                                12 │
└───────────────────────────────────┘
```

عبارت “Salut, étudiante!”, جایی که شخصیت `é` می توان با استفاده از یک نقطه نشان داد (`U+00E9`) یا دو نقطه (`U+0065U+0301`) تابع را می توان بازگشت برخی از نتیجه غیر منتظره:

پرسوجو برای نامه `é` که نشان داده شده است یک نقطه یونیکد `U+00E9`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

نتیجه:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     17 │
└────────────────────────────────────────┘
```

پرسوجو برای نامه `é` که به نمایندگی از دو نقطه یونیکد `U+0065U+0301`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

نتیجه:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     18 │
└────────────────────────────────────────┘
```

## در حال بارگذاری {#positioncaseinsensitiveutf8}

همان [موقعیت 8](#positionutf8), اما غیر حساس به حروف است. بازگرداندن موقعیت (در نقاط یونیکد) از رشته پیدا شده است در رشته, با شروع از 1.

این نسخهها کار میکند با این فرض که رشته شامل مجموعه ای از بایت به نمایندگی از یک متن کد گذاری شده وزارت مخابرات 8. اگر این فرض ملاقات نمی, تابع یک استثنا پرتاب نمی کند و برخی از نتیجه غیر منتظره را برمی گرداند. اگر شخصیت را می توان با استفاده از دو نقطه یونیکد نشان, این دو و غیره استفاده.

**نحو**

``` sql
positionCaseInsensitiveUTF8(haystack, needle[, start_pos])
```

**پارامترها**

-   `haystack` — string, in which substring will to be searched. [رشته](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [رشته](../syntax.md#syntax-string-literal).
-   `start_pos` – Optional parameter, position of the first character in the string to start search. [UInt](../../sql-reference/data-types/int-uint.md)

**مقدار بازگشتی**

-   موقعیت شروع در یونیکد امتیاز (شمارش از 1), اگر زیر رشته پیدا شد.
-   0, اگر زیر رشته یافت نشد.

نوع: `Integer`.

**مثال**

پرسوجو:

``` sql
SELECT positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')
```

نتیجه:

``` text
┌─positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')─┐
│                                                  9 │
└────────────────────────────────────────────────────┘
```

## چند ضلعی {#multisearchallpositions}

همان [موقعیت](string-search-functions.md#position) اما بازگشت `Array` از موقعیت (به بایت) از بسترهای مربوطه موجود در رشته. موقعیت ها با شروع از نمایه 1.

جستجو در دنباله ای از بایت بدون توجه به رمزگذاری رشته و میترا انجام می شود.

-   برای جستجو اسکی غیر حساس به حالت, استفاده از تابع `multiSearchAllPositionsCaseInsensitive`.
-   برای جستجو در یوتف-8, استفاده از تابع [چند ضلعی پایگاه داده های8](#multiSearchAllPositionsUTF8).
-   برای غیر حساس به حالت جستجو-8, استفاده از تابع چند تخصیص چندگانه 18.

**نحو**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needlen])
```

**پارامترها**

-   `haystack` — string, in which substring will to be searched. [رشته](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [رشته](../syntax.md#syntax-string-literal).

**مقادیر بازگشتی**

-   مجموعه ای از موقعیت های شروع در بایت (شمارش از 1), اگر زیر رشته مربوطه پیدا شد و 0 اگر یافت نشد.

**مثال**

پرسوجو:

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])
```

نتیجه:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```

## چند ضلعی پایگاه داده های8 {#multiSearchAllPositionsUTF8}

ببینید `multiSearchAllPositions`.

## ترکیب چندجفتاری (هیستک, \[سوزن<sub>1</sub> سوزن<sub>2</sub>, …, needle<sub>نه</sub>\]) {#multisearchfirstposition}

همان `position` اما بازده سمت چپ افست از رشته `haystack` که به برخی از سوزن همسان.

برای یک جستجوی غیر حساس مورد و یا / و در توابع استفاده از فرمت جی تی اف 8 `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8`.

## مقالههای جدید مرتبط با تحقیق این نویسنده<sub>1</sub> سوزن<sub>2</sub>, …, needle<sub>نه</sub>\]) {#multisearchfirstindexhaystack-needle1-needle2-needlen}

بازگرداندن شاخص `i` (شروع از 1) سوزن چپ پیدا شده است<sub>من ... </sub> در رشته `haystack` و 0 در غیر این صورت.

برای یک جستجوی غیر حساس مورد و یا / و در توابع استفاده از فرمت جی تی اف 8 `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8`.

## مولسیرچانی (هیستک, \[سوزن<sub>1</sub> سوزن<sub>2</sub>, …, needle<sub>نه</sub>\]) {#function-multisearchany}

بازده 1, اگر حداقل یک سوزن رشته<sub>من ... </sub> مسابقات رشته `haystack` و 0 در غیر این صورت.

برای یک جستجوی غیر حساس مورد و یا / و در توابع استفاده از فرمت جی تی اف 8 `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

!!! note "یادداشت"
    در همه `multiSearch*` توابع تعداد سوزن ها باید کمتر از 2 باشد<sup>8</sup> به دلیل مشخصات پیاده سازی.

## همخوانی داشتن (کومه علف خشک, الگو) {#matchhaystack-pattern}

بررسی اینکه رشته با `pattern` عبارت منظم. یک `re2` عبارت منظم. این [نحو](https://github.com/google/re2/wiki/Syntax) از `re2` عبارات منظم محدود تر از نحو عبارات منظم پرل است.

بازده 0 اگر مطابقت ندارد, یا 1 اگر منطبق.

توجه داشته باشید که نماد ممیز (`\`) برای فرار در عبارت منظم استفاده می شود. همان نماد برای فرار در لیتر رشته استفاده می شود. بنابراین به منظور فرار از نماد در یک عبارت منظم, شما باید دو بک اسلش ارسال (\\) در یک رشته تحت اللفظی.

عبارت منظم با رشته کار می کند به عنوان اگر مجموعه ای از بایت است. عبارت منظم می تواند بایت پوچ نیست.
برای الگوهای به جستجو برای بسترهای در یک رشته, بهتر است به استفاده از مانند و یا ‘position’ از اونجایی که خیلی سریعتر کار میکنن

## ملتمتچانی (کومه علف خشک, \[الگو<sub>1</sub> الگو<sub>2</sub>, …, pattern<sub>نه</sub>\]) {#multimatchanyhaystack-pattern1-pattern2-patternn}

همان `match`, اما بازده 0 اگر هیچ یک از عبارات منظم همسان و 1 اگر هر یک از الگوهای مسابقات. این استفاده می کند [hyperscan](https://github.com/intel/hyperscan) کتابخونه. برای الگوهای به جستجو بسترهای در یک رشته, بهتر است به استفاده از `multiSearchAny` چون خیلی سریعتر کار میکنه

!!! note "یادداشت"
    طول هر یک از `haystack` رشته باید کمتر از 2 باشد<sup>32</sup> بایت در غیر این صورت استثنا پرتاب می شود. این محدودیت صورت می گیرد به دلیل hyperscan API.

## مقالههای جدید مرتبط با تحقیق این نویسنده<sub>1</sub> الگو<sub>2</sub>, …, pattern<sub>نه</sub>\]) {#multimatchanyindexhaystack-pattern1-pattern2-patternn}

همان `multiMatchAny`, اما بازگرداندن هر شاخص که منطبق بر انبار کاه.

## اطلاعات دقیق<sub>1</sub> الگو<sub>2</sub>, …, pattern<sub>نه</sub>\]) {#multimatchallindiceshaystack-pattern1-pattern2-patternn}

همان `multiMatchAny`, اما بازگرداندن مجموعه ای از تمام شاخص که مطابقت انبار کاه در هر سفارش.

## چندبازیماتچانی (هیستک, فاصله, \[الگو<sub>1</sub> الگو<sub>2</sub>, …, pattern<sub>نه</sub>\]) {#multifuzzymatchanyhaystack-distance-pattern1-pattern2-patternn}

همان `multiMatchAny`, اما بازده 1 اگر هر الگوی منطبق انبار کاه در یک ثابت [ویرایش فاصله](https://en.wikipedia.org/wiki/Edit_distance). این تابع نیز در حالت تجربی است و می تواند بسیار کند باشد. برای اطلاعات بیشتر نگاه کنید به [hyperscan مستندات](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching).

## چند شکلی (هیستاک, فاصله, \[الگو<sub>1</sub> الگو<sub>2</sub>, …, pattern<sub>نه</sub>\]) {#multifuzzymatchanyindexhaystack-distance-pattern1-pattern2-patternn}

همان `multiFuzzyMatchAny`, اما می گرداند هر شاخص که منطبق بر انبار کاه در فاصله ویرایش ثابت.

## بازهای چندگانه (انبار کاه, فاصله, \[الگو<sub>1</sub> الگو<sub>2</sub>, …, pattern<sub>نه</sub>\]) {#multifuzzymatchallindiceshaystack-distance-pattern1-pattern2-patternn}

همان `multiFuzzyMatchAny`, اما بازگرداندن مجموعه ای از تمام شاخص در هر منظور که مطابقت با انبار کاه در فاصله ویرایش ثابت.

!!! note "یادداشت"
    `multiFuzzyMatch*` توابع از عبارات منظم یونایتد-8 پشتیبانی نمی کنند و چنین عبارات به عنوان بایت به دلیل محدودیت هیپراکسان درمان می شوند.

!!! note "یادداشت"
    برای خاموش کردن تمام توابع است که با استفاده از بیش از حد اسکان, استفاده از تنظیمات `SET allow_hyperscan = 0;`.

## عصاره (انبار کاه, الگو) {#extracthaystack-pattern}

عصاره یک قطعه از یک رشته با استفاده از یک عبارت منظم. اگر ‘haystack’ با ‘pattern’ عبارت منظم, یک رشته خالی بازگشته است. اگر عبارت منظم حاوی وسترن نیست, طول می کشد قطعه که منطبق بر کل عبارت منظم. در غیر این صورت قطعه ای را می گیرد که با اولین زیر دست ساز مطابقت دارد.

## خارج تماس بگیرید) {#extractallhaystack-pattern}

عصاره تمام قطعات از یک رشته با استفاده از یک عبارت منظم. اگر ‘haystack’ با ‘pattern’ عبارت منظم, یک رشته خالی بازگشته است. بازگرداندن مجموعه ای از رشته متشکل از تمام مسابقات به عبارت منظم. به طور کلی, رفتار همان است که ‘extract’ تابع (در آن طول می کشد برای اولین بار subpattern یا کل بیان اگر وجود ندارد subpattern).

## مانند (کومه علف خشک, الگو), کومه علف خشک مانند اپراتور الگوی {#function-like}

بررسی اینکه یک رشته منطبق یک عبارت ساده به طور منظم.
عبارت منظم می تواند حاوی متسیمبلس باشد `%` و `_`.

`%` نشان می دهد هر مقدار از هر بایت (از جمله صفر شخصیت).

`_` نشان می دهد هر یک بایت.

از بک اسلش استفاده کنید (`\`) برای فرار از متسیمبلس . توجه داشته باشید در فرار در شرح ‘match’ تابع.

برای عبارات منظم مانند `%needle%`, کد مطلوب تر است و کار می کند به همان سرعتی که `position` تابع.
برای دیگر عبارات منظم, کد همان است که برای است ‘match’ تابع.

## notLike(انبار کاه pattern), انبار کاه نیست مانند الگوی اپراتور {#function-notlike}

همان چیزی که به عنوان ‘like’, اما منفی.

## نمک زدایی (انبار کاه, سوزن) {#ngramdistancehaystack-needle}

محاسبه فاصله 4 گرم بین `haystack` و `needle`: counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns float number from 0 to 1 – the closer to zero, the more strings are similar to each other. If the constant `needle` یا `haystack` بیش از 32 کیلوبایت است, می اندازد یک استثنا. اگر برخی از غیر ثابت `haystack` یا `needle` رشته ها بیش از 32 کیلوبایت, فاصله است که همیشه یکی.

برای جستجوی غیر حساس به حالت یا / و در توابع استفاده از فرمت جی تی اف 8 `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8`.

## نگراماسراچ (هیستک سوزن) {#ngramsearchhaystack-needle}

مثل `ngramDistance` اما محاسبه تفاوت غیر متقارن بین `needle` و `haystack` – the number of n-grams from needle minus the common number of n-grams normalized by the number of `needle` مامان بزرگ نزدیک تر به یک, بیشتر احتمال دارد `needle` در `haystack`. می تواند برای جستجو رشته فازی مفید باشد.

برای جستجوی غیر حساس به حالت یا / و در توابع استفاده از فرمت جی تی اف 8 `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8`.

!!! note "یادداشت"
    For UTF-8 case we use 3-gram distance. All these are not perfectly fair n-gram distances. We use 2-byte hashes to hash n-grams and then calculate the (non-)symmetric difference between these hash tables – collisions may occur. With UTF-8 case-insensitive format we do not use fair `tolower` function – we zero the 5-th bit (starting from zero) of each codepoint byte and first bit of zeroth byte if bytes more than one – this works for Latin and mostly for all Cyrillic letters.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/string_search_functions/) <!--hide-->
