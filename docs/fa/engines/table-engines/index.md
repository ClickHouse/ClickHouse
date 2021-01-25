---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u0645\u0648\u062A\u0648\u0631\u0647\u0627\u06CC \u062C\u062F\u0648\
  \u0644"
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

موتورهای جدول جهانی ترین و کاربردی برای وظایف بار بالا. اموال به اشتراک گذاشته شده توسط این موتور درج داده های سریع با پردازش داده های پس زمینه های بعدی است. `MergeTree` موتورهای خانواده از تکرار داده ها پشتیبانی می کنند (با [تکرار\*](mergetree-family/replication.md#table_engines-replication) نسخه موتورهای) پارتیشن بندی و ویژگی های دیگر در موتورهای دیگر پشتیبانی نمی شود.

موتورهای در خانواده:

-   [ادغام](mergetree-family/mergetree.md#mergetree)
-   [جایگزینی](mergetree-family/replacingmergetree.md#replacingmergetree)
-   [سامینگمرگتری](mergetree-family/summingmergetree.md#summingmergetree)
-   [ریزدانه](mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
-   [سقوط غذای اصلی](mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
-   [در حال بارگذاری](mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
-   [نمودار](mergetree-family/graphitemergetree.md#graphitemergetree)

### ثبت {#log}

سبک [موتورها](log-family/index.md) با حداقل قابلیت. هنگامی که شما نیاز به سرعت نوشتن بسیاری از جداول کوچک (تا حدود 1 میلیون ردیف) و خواندن بعد به عنوان یک کل موثر ترین هستند.

موتورهای در خانواده:

-   [جمع شدن](log-family/tinylog.md#tinylog)
-   [خط زدن](log-family/stripelog.md#stripelog)
-   [ثبت](log-family/log.md#log)

### موتورهای یکپارچه سازی {#integration-engines}

موتورهای برای برقراری ارتباط با دیگر ذخیره سازی داده ها و سیستم های پردازش.

موتورهای در خانواده:

-   [کافکا](integrations/kafka.md#kafka)
-   [MySQL](integrations/mysql.md#mysql)
-   [ODBC](integrations/odbc.md#table-engine-odbc)
-   [JDBC](integrations/jdbc.md#table-engine-jdbc)
-   [HDFS](integrations/hdfs.md#hdfs)

### موتورهای ویژه {#special-engines}

موتورهای در خانواده:

-   [توزیع شده](special/distributed.md#distributed)
-   [ماده بینی](special/materializedview.md#materializedview)
-   [واژهنامه](special/dictionary.md#dictionary)
-   پردازشگر پشتیبانی شده:
-   [پرونده](special/file.md#file)
-   [خالی](special/null.md#null)
-   [تنظیم](special/set.md#set)
-   [پیوستن](special/join.md#join)
-   [URL](special/url.md#table_engines-url)
-   [نما](special/view.md#table_engines-view)
-   [حافظه](special/memory.md#memory)
-   [بافر](special/buffer.md#buffer)

## ستونهای مجازی {#table_engines-virtual_columns}

ستون مجازی یک ویژگی موتور جدول انتگرال است که در کد منبع موتور تعریف شده است.

شما باید ستون مجازی در مشخص نیست `CREATE TABLE` پرس و جو کنید و نمی توانید ببینید `SHOW CREATE TABLE` و `DESCRIBE TABLE` نتایج پرس و جو. ستون مجازی نیز فقط خواندنی, بنابراین شما می توانید داده ها را به ستون مجازی وارد کنید.

برای انتخاب داده ها از یک ستون مجازی, شما باید نام خود را در مشخص `SELECT` پرس و جو. `SELECT *` مقادیر از ستون های مجازی بازگشت نیست.

اگر شما یک جدول با یک ستون است که به همین نام به عنوان یکی از ستون های مجازی جدول ایجاد, ستون مجازی غیر قابل دسترس می شود. ما توصیه نمی انجام این کار. برای کمک به جلوگیری از درگیری, نام ستون مجازی معمولا با تاکید پیشوند.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
