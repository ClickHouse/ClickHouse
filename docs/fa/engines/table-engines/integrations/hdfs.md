---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: HDFS
---

# HDFS {#table_engines-hdfs}

این موتور ادغام با فراهم می کند [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) اکوسیستم با اجازه دادن به مدیریت داده ها در [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)از طریق کلیکهاوس. این موتور مشابه است
به [پرونده](../special/file.md#table_engines-file) و [URL](../special/url.md#table_engines-url) موتورهای, اما فراهم می کند ویژگی های هادوپ خاص.

## استفاده {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

این `URI` پارامتر تمام فایل نشانی اینترنتی در اچ دی است.
این `format` پارامتر یکی از فرمت های فایل های موجود را مشخص می کند. برای انجام
`SELECT` نمایش داده شد, فرمت باید برای ورودی پشتیبانی می شود, و به انجام
`INSERT` queries – for output. The available formats are listed in the
[فرشها](../../../interfaces/formats.md#formats) بخش.
قسمت مسیر `URI` ممکن است حاوی دل تنگی. در این مورد جدول قابل خواندن خواهد بود.

**مثال:**

**1.** تنظیم `hdfs_engine_table` جدول:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** پر کردن پرونده:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** پرسوجوی داده:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## پیاده سازی اطلاعات {#implementation-details}

-   می خواند و می نویسد می تواند موازی
-   پشتیبانی نمیشود:
    -   `ALTER` و `SELECT...SAMPLE` عملیات.
    -   شاخص.
    -   تکرار.

**دل تنگی در مسیر**

اجزای مسیر چندگانه می تواند دل تنگی دارند. برای پردازش فایل باید وجود داشته باشد و مسابقات به الگوی کل مسیر. لیست فایل های تعیین در طول `SELECT` (نه در `CREATE` لحظه).

-   `*` — Substitutes any number of any characters except `/` از جمله رشته خالی.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

سازه با `{}` شبیه به [دور](../../../sql-reference/table-functions/remote.md) تابع جدول.

**مثال**

1.  فرض کنید ما چندین فایل را در قالب فیلم با اوریس زیر در اچ دی ها داریم:

-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_3’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_3’

1.  راه های مختلفی برای ایجاد یک جدول متشکل از تمام شش فایل وجود دارد:

<!-- -->

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

راه دیگر:

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

جدول شامل تمام فایل ها در هر دو دایرکتوری (تمام فایل ها باید فرمت و طرح توصیف شده در پرس و جو راضی):

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

!!! warning "اخطار"
    اگر فهرستی از فایل های حاوی محدوده تعداد با صفر پیشرو, استفاده از ساخت و ساز با پرانتز برای هر رقم به طور جداگانه و یا استفاده `?`.

**مثال**

ایجاد جدول با فایل های به نام `file000`, `file001`, … , `file999`:

``` sql
CREARE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

## ستونهای مجازی {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**همچنین نگاه کنید به**

-   [ستونهای مجازی](../index.md#table_engines-virtual_columns)

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/hdfs/) <!--hide-->
