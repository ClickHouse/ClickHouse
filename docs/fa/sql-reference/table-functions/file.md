---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: "\u067E\u0631\u0648\u0646\u062F\u0647"
---

# پرونده {#file}

ایجاد یک جدول از یک فایل. این تابع جدول شبیه به [نشانی وب](url.md) و [hdfs](hdfs.md) یکی

``` sql
file(path, format, structure)
```

**پارامترهای ورودی**

-   `path` — The relative path to the file from [\_مخفی کردن \_صفحه](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). مسیر به فایل پشتیبانی پس از دل تنگی در حالت فقط خواندنی: `*`, `?`, `{abc,def}` و `{N..M}` کجا `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [قالب](../../interfaces/formats.md#formats) پرونده
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**مقدار بازگشتی**

یک جدول با ساختار مشخص شده برای خواندن یا نوشتن داده ها در فایل مشخص شده است.

**مثال**

تنظیم `user_files_path` و محتویات فایل `test.csv`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

جدول از`test.csv` و انتخاب دو ردیف اول:

``` sql
SELECT *
FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

``` sql
-- getting the first 10 lines of a table that contains 3 columns of UInt32 type from a CSV file
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```

**دل تنگی در مسیر**

اجزای مسیر چندگانه می تواند دل تنگی دارند. برای پردازش فایل باید وجود داشته باشد و مسابقات به الگوی کل مسیر (نه تنها پسوند یا پیشوند).

-   `*` — Substitutes any number of any characters except `/` از جمله رشته خالی.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

سازه با `{}` شبیه به [عملکرد جدول از راه دور](../../sql-reference/table-functions/remote.md)).

**مثال**

1.  فرض کنید ما چندین فایل با مسیرهای نسبی زیر داریم:

-   ‘some\_dir/some\_file\_1’
-   ‘some\_dir/some\_file\_2’
-   ‘some\_dir/some\_file\_3’
-   ‘another\_dir/some\_file\_1’
-   ‘another\_dir/some\_file\_2’
-   ‘another\_dir/some\_file\_3’

1.  پرس و جو مقدار ردیف در این فایل ها:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

1.  پرس و جو مقدار ردیف در تمام فایل های این دو دایرکتوری:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "اخطار"
    اگر لیست خود را از فایل های حاوی محدوده تعداد با صفر پیشرو, استفاده از ساخت و ساز با پرانتز برای هر رقم به طور جداگانه و یا استفاده `?`.

**مثال**

پرس و جو داده ها از فایل های به نام `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## ستونهای مجازی {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**همچنین نگاه کنید به**

-   [ستونهای مجازی](https://clickhouse.tech/docs/en/operations/table_engines/#table_engines-virtual_columns)

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/file/) <!--hide-->
