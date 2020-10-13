---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u0631\u0634\u062A\u0647 \u062B\u0627\u0628\u062A)"
---

# رشته ثابت {#fixedstring}

یک رشته ثابت طول `N` بایت (نه شخصیت و نه نقاط کد).

برای اعلام یک ستون از `FixedString` نوع, استفاده از نحو زیر:

``` sql
<column_name> FixedString(N)
```

کجا `N` یک عدد طبیعی است.

این `FixedString` نوع زمانی موثر است که داده ها طول دقیق داشته باشند `N` بایت در تمام موارد دیگر, این احتمال وجود دارد به کاهش بهره وری.

نمونه هایی از مقادیر است که می تواند موثر در ذخیره می شود `FixedString`- ستون های تایپ شده:

-   نمایندگی دودویی نشانی های اینترنتی (`FixedString(16)` برای ایپو6).
-   Language codes (ru_RU, en_US … ).
-   Currency codes (USD, RUB … ).
-   نمایش دودویی رشته هش (`FixedString(16)` برای ام دی 5, `FixedString(32)` برای شی256).

برای ذخیره مقادیر یوید از [UUID](uuid.md) نوع داده.

هنگام قرار دادن داده ها, تاتر:

-   مکمل یک رشته با بایت پوچ اگر رشته شامل کمتر از `N` بایت
-   پرتاب `Too large value for FixedString(N)` استثنا اگر رشته شامل بیش از `N` بایت

در هنگام انتخاب داده, تاتر می کند بایت پوچ در پایان رشته را حذف کنید. اگر شما استفاده از `WHERE` بند, شما باید بایت پوچ دستی اضافه برای مطابقت با `FixedString` ارزش. مثال زیر نشان میدهد که چگونه به استفاده از `WHERE` بند با `FixedString`.

بیایید جدول زیر را با تک در نظر بگیریم `FixedString(2)` ستون:

``` text
┌─name──┐
│ b     │
└───────┘
```

پرسوجو `SELECT * FROM FixedStringTable WHERE a = 'b'` هیچ داده به عنوان یک نتیجه نمی گرداند. ما باید الگوی فیلتر با بایت پوچ تکمیل.

``` sql
SELECT * FROM FixedStringTable
WHERE a = 'b\0'
```

``` text
┌─a─┐
│ b │
└───┘
```

این رفتار از خروجی زیر برای متفاوت `CHAR` نوع (جایی که رشته ها با فضاهای خالی, و فضاهای برای خروجی حذف).

توجه داشته باشید که طول `FixedString(N)` ارزش ثابت است. این [طول](../../sql-reference/functions/array-functions.md#array_functions-length) بازده عملکرد `N` حتی اگر `FixedString(N)` ارزش تنها با بایت پوچ پر, اما [خالی](../../sql-reference/functions/string-functions.md#empty) بازده عملکرد `1` در این مورد.

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/fixedstring/) <!--hide-->
