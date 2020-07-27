---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_folder_title: Table Engines
toc_priority: 26
toc_title: "\u0645\u0639\u0631\u0641\u06CC \u0634\u0631\u06A9\u062A"
---

# موتورهای جدول {#table_engines}

موتور جدول (نوع جدول) تعیین می کند:

-   چگونه و در کجا اطلاعات ذخیره شده است, جایی که برای نوشتن به, و از کجا به خواندن از.
-   که نمایش داده شد پشتیبانی می شوند, و چگونه.
-   همزمان دسترسی به داده ها.
-   استفاده از شاخص, در صورت وجود.
-   این که اجرای درخواست چند رشته ای امکان پذیر باشد.
-   پارامترهای تکرار داده.

## خانواده موتور {#engine-families}

### ادغام {#mergetree}

موتورهای جدول جهانی ترین و کاربردی برای وظایف بار بالا. اموال به اشتراک گذاشته شده توسط این موتور درج داده های سریع با پردازش داده های پس زمینه های بعدی است. `MergeTree` موتورهای خانواده از تکرار داده ها پشتیبانی می کنند (با [تکرار\*](mergetree_family/replication.md) نسخه موتورهای) پارتیشن بندی و ویژگی های دیگر در موتورهای دیگر پشتیبانی نمی شود.

موتورهای در خانواده:

-   [ادغام](mergetree_family/mergetree.md)
-   [جایگزینی](mergetree_family/replacingmergetree.md)
-   [سامینگمرگتری](mergetree_family/summingmergetree.md)
-   [ریزدانه](mergetree_family/aggregatingmergetree.md)
-   [سقوط غذای اصلی](mergetree_family/collapsingmergetree.md)
-   [در حال بارگذاری](mergetree_family/versionedcollapsingmergetree.md)
-   [نمودار](mergetree_family/graphitemergetree.md)

### ثبت {#log}

سبک [موتورها](log_family/index.md) با حداقل قابلیت. هنگامی که شما نیاز به سرعت نوشتن بسیاری از جداول کوچک (تا حدود 1 میلیون ردیف) و خواندن بعد به عنوان یک کل موثر ترین هستند.

موتورهای در خانواده:

-   [جمع شدن](log_family/tinylog.md)
-   [خط زدن](log_family/stripelog.md)
-   [ثبت](log_family/log.md)

### موتورهای یکپارچه سازی {#integration-engines}

موتورهای برای برقراری ارتباط با دیگر ذخیره سازی داده ها و سیستم های پردازش.

موتورهای در خانواده:

-   [کافکا](integrations/kafka.md)
-   [MySQL](integrations/mysql.md)
-   [ODBC](integrations/odbc.md)
-   [JDBC](integrations/jdbc.md)
-   [HDFS](integrations/hdfs.md)

### موتورهای ویژه {#special-engines}

موتورهای در خانواده:

-   [توزیع شده](special/distributed.md)
-   [ماده بینی](special/materializedview.md)
-   [واژهنامه](special/dictionary.md)
-   [ادغام](special/merge.md)
-   [پرونده](special/file.md)
-   [خالی](special/null.md)
-   [تنظیم](special/set.md)
-   [پیوستن](special/join.md)
-   [URL](special/url.md)
-   [نما](special/view.md)
-   [حافظه](special/memory.md)
-   [بافر](special/buffer.md)

## ستونهای مجازی {#table_engines-virtual-columns}

ستون مجازی یک ویژگی موتور جدول انتگرال است که در کد منبع موتور تعریف شده است.

شما باید ستون مجازی در مشخص نیست `CREATE TABLE` پرس و جو کنید و نمی توانید ببینید `SHOW CREATE TABLE` و `DESCRIBE TABLE` نتایج پرس و جو. ستون مجازی نیز فقط خواندنی, بنابراین شما می توانید داده ها را به ستون مجازی وارد کنید.

برای انتخاب داده ها از یک ستون مجازی, شما باید نام خود را در مشخص `SELECT` پرس و جو. `SELECT *` مقادیر از ستون های مجازی بازگشت نیست.

اگر شما یک جدول با یک ستون است که به همین نام به عنوان یکی از ستون های مجازی جدول ایجاد, ستون مجازی غیر قابل دسترس می شود. ما توصیه نمی انجام این کار. برای کمک به جلوگیری از درگیری, نام ستون مجازی معمولا با تاکید پیشوند.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
