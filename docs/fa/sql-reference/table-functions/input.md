---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "\u0648\u0631\u0648\u062F\u06CC"
---

# ورودی {#input}

`input(structure)` - تابع جدول که اجازه می دهد تا به طور موثر تبدیل و قرار دادن داده های ارسال شده به
سرور با ساختار داده شده به جدول با ساختار دیگر.

`structure` - ساختار داده ها در قالب زیر به سرور ارسال می شود `'column1_name column1_type, column2_name column2_type, ...'`.
به عنوان مثال, `'id UInt32, name String'`.

این تابع را می توان تنها در `INSERT SELECT` پرس و جو و تنها یک بار اما در غیر این صورت مانند تابع جدول معمولی رفتار می کنند
(مثلا, این را می توان در زیرخاکری مورد استفاده قرار, و غیره.).

داده ها را می توان به هیچ وجه مانند عادی ارسال می شود `INSERT` پرس و جو و گذشت در هر موجود [قالب](../../interfaces/formats.md#formats)
که باید در پایان پرس و جو مشخص شود (بر خلاف عادی `INSERT SELECT`).

ویژگی اصلی این تابع این است که وقتی سرور داده ها را از مشتری دریافت می کند به طور همزمان تبدیل می کند
با توجه به لیست عبارات در `SELECT` بند و درج به جدول هدف. جدول موقت
با تمام داده های منتقل شده ایجاد نمی شود.

**مثالها**

-   اجازه دهید `test` جدول دارای ساختار زیر است `(a String, b String)`
    و داده ها در `data.csv` دارای ساختار متفاوت `(col1 String, col2 Date, col3 Int32)`. پرسوجو برای درج
    داده ها از `data.csv` به `test` جدول با تبدیل همزمان به نظر می رسد مثل این:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   اگر `data.csv` حاوی اطلاعات از ساختار مشابه `test_structure` به عنوان جدول `test` سپس این دو نمایش داده شد برابر هستند:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/input/) <!--hide-->
