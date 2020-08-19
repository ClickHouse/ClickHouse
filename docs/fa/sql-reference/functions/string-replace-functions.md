---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "\u0628\u0631\u0627\u06CC \u062C\u0627\u06CC\u06AF\u0632\u06CC\u0646\u06CC\
  \ \u062F\u0631 \u0631\u0634\u062A\u0647\u0647\u0627"
---

# توابع برای جستجو و جایگزینی در رشته ها {#functions-for-searching-and-replacing-in-strings}

## جایگزینی جایگزین) {#replaceonehaystack-pattern-replacement}

جایگزین وقوع اول, در صورت وجود, از ‘pattern’ زیر رشته در ‘haystack’ با ‘replacement’ زیر رشته.
از این پس, ‘pattern’ و ‘replacement’ حتما ثابته

## replaceAll(haystack, الگوی جایگزینی) جایگزین(haystack, الگوی جایگزینی) {#replaceallhaystack-pattern-replacement-replacehaystack-pattern-replacement}

جایگزین تمام اتفاقات ‘pattern’ زیر رشته در ‘haystack’ با ‘replacement’ زیر رشته.

## جایگزین کردن الگوی جایگزین) {#replaceregexponehaystack-pattern-replacement}

جایگزینی با استفاده از ‘pattern’ عبارت منظم. دوباره2 عبارت منظم.
جایگزین تنها وقوع اول, در صورت وجود.
یک الگو را می توان به عنوان ‘replacement’. این الگو می تواند شامل تعویض `\0-\9`.
جایگزینی `\0` شامل کل عبارت منظم. درحال جایگزینی `\1-\9` مربوط به زیرخط numbers.To استفاده از `\` شخصیت در قالب, فرار با استفاده از `\`.
همچنین در نظر داشته باشید که یک رشته تحت اللفظی نیاز به فرار اضافی نگه دارید.

مثال 1. تبدیل تاریخ به فرمت امریکایی:

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

``` text
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
```

مثال 2. کپی کردن یک رشته ده بار:

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

``` text
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## جایگزین کردن الگوی جایگزین) {#replaceregexpallhaystack-pattern-replacement}

این کار همان چیزی, اما جایگزین همه وقوع. مثال:

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

``` text
┌─res────────────────────────┐
│ HHeelllloo,,  WWoorrlldd!! │
└────────────────────────────┘
```

به عنوان یک استثنا, اگر یک عبارت منظم در زیر رشته خالی کار می کرد, جایگزینی بیش از یک بار ساخته شده است.
مثال:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

``` text
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
```

## سرویس پرداخت درونبرنامهای پلی) {#regexpquotemetas}

تابع می افزاید: یک بک اسلش قبل از برخی از شخصیت های از پیش تعریف شده در رشته.
نویسههای از پیش تعریفشده: ‘0’, ‘\\’, ‘\|’, ‘(’, ‘)’, ‘^’, ‘$’, ‘.’, ‘\[’, '\]', ‘?’, '\*‘,’+‘,’{‘,’:‘,’-'.
این اجرای کمی از دوباره متفاوت2::پاسخ2:: نقل قول. این فرار صفر بایت به عنوان \\ 0 بجای 00 و فرار شخصیت تنها مورد نیاز.
برای کسب اطلاعات بیشتر به لینک مراجعه کنید: [RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473)

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/string_replace_functions/) <!--hide-->
