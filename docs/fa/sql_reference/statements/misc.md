---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 39
toc_title: "\u063A\u06CC\u0631\u0647"
---

# متفرقه نمایش داده شد {#miscellaneous-queries}

## ATTACH {#attach}

این پرس و جو دقیقا همان است `CREATE` اما

-   به جای کلمه `CREATE` با استفاده از این کلمه `ATTACH`.
-   پرس و جو می کند داده ها بر روی دیسک ایجاد کنید, اما فرض می شود که داده ها در حال حاضر در مکان های مناسب, و فقط می افزاید: اطلاعات در مورد جدول به سرور.
    پس از اجرای یک ضمیمه پرس و جو در سرور خواهد شد در مورد وجود جدول.

اگر جدول قبلا جدا شد (`DETACH`), به این معنی که ساختار خود شناخته شده است, شما می توانید مختصر بدون تعریف ساختار استفاده.

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

این پرس و جو در هنگام شروع سرور استفاده می شود. سرور ذخیره ابرداده جدول به عنوان فایل های با `ATTACH` نمایش داده شد, که به سادگی در راه اندازی اجرا می شود (به غیر از جداول سیستم, که به صراحت بر روی سرور ایجاد).

## CHECK TABLE {#check-table}

چک اگر داده ها در جدول خراب شده است.

``` sql
CHECK TABLE [db.]name
```

این `CHECK TABLE` پرس و جو اندازه فایل واقعی با مقادیر مورد انتظار که بر روی سرور ذخیره می شود مقایسه می کند. اگر اندازه فایل انجام مقادیر ذخیره شده مطابقت ندارد, به این معنی که داده ها خراب شده است. این می تواند باعث, مثلا, توسط یک تصادف سیستم در طول اجرای پرس و جو.

پاسخ پرس و جو شامل `result` ستون با یک ردیف. ردیف دارای ارزش
[بولی](../../sql_reference/data_types/boolean.md) نوع:

-   0-داده ها در جدول خراب شده است.
-   1-داده حفظ یکپارچگی.

این `CHECK TABLE` پرس و جو از موتورهای جدول زیر پشتیبانی می کند:

-   [ثبت](../../engines/table_engines/log_family/log.md)
-   [جمع شدن](../../engines/table_engines/log_family/tinylog.md)
-   [خط زدن](../../engines/table_engines/log_family/stripelog.md)
-   [ادغام خانواده](../../engines/table_engines/mergetree_family/mergetree.md)

انجام بیش از جداول با موتورهای جدول دیگر باعث یک استثنا.

موتورهای از `*Log` خانواده بازیابی اطلاعات خودکار در شکست را فراهم نمی کند. استفاده از `CHECK TABLE` پرس و جو برای پیگیری از دست دادن داده ها به موقع.

برای `MergeTree` موتورهای خانواده `CHECK TABLE` پرس و جو نشان می دهد وضعیت چک برای هر بخش داده های فردی از یک جدول بر روی سرور محلی.

**اگر داده ها خراب شده است**

اگر جدول خراب شده است, شما می توانید داده های غیر خراب به جدول دیگر کپی کنید. برای انجام این کار:

1.  ایجاد یک جدول جدید با ساختار همان جدول صدمه دیده است. برای انجام این کار پرس و جو را اجرا کنید `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  تنظیم [\_مخفی کردن](../../operations/settings/settings.md#settings-max_threads) ارزش به 1 برای پردازش پرس و جو بعدی در یک موضوع واحد. برای انجام این کار پرس و جو را اجرا کنید `SET max_threads = 1`.
3.  اجرای پرسوجو `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. این درخواست داده های غیر خراب شده را از جدول خراب شده به جدول دیگر کپی می کند. فقط داده ها قبل از قسمت خراب کپی خواهد شد.
4.  راه اندازی مجدد `clickhouse-client` برای تنظیم مجدد `max_threads` ارزش.

## DESCRIBE TABLE {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

بازگرداندن موارد زیر `String` نوع ستونها:

-   `name` — Column name.
-   `type`— Column type.
-   `default_type` — Clause that is used in [عبارت پیشفرض](create.md#create-default-values) (`DEFAULT`, `MATERIALIZED` یا `ALIAS`). ستون شامل یک رشته خالی, اگر عبارت پیش فرض مشخص نشده است.
-   `default_expression` — Value specified in the `DEFAULT` بند بند.
-   `comment_expression` — Comment text.

ساختارهای داده تو در تو خروجی در “expanded” قالب. هر ستون به طور جداگانه نشان داده شده است, با نام بعد از یک نقطه.

## DETACH {#detach}

حذف اطلاعات در مورد ‘name’ جدول از سرور. سرور متوقف می شود دانستن در مورد وجود جدول.

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

این داده ها و یا ابرداده جدول را حذف کنید. در راه اندازی سرور بعدی, سرور خواهد ابرداده به عنوان خوانده شده و پیدا کردن در مورد جدول دوباره.
به طور مشابه “detached” جدول را می توان دوباره متصل با استفاده از `ATTACH` پرس و جو (به غیر از جداول سیستم که لازم نیست metadata ذخیره شده برای آنها).

وجود ندارد `DETACH DATABASE` پرس و جو.

## DROP {#drop}

این پرسوجو دارای دو نوع است: `DROP DATABASE` و `DROP TABLE`.

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

حذف تمام جداول در داخل ‘db’ پایگاه داده, سپس حذف ‘db’ پایگاه داده خود را.
اگر `IF EXISTS` مشخص شده است, این خطا بازگشت نیست اگر پایگاه داده وجود ندارد.

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

حذف جدول.
اگر `IF EXISTS` مشخص شده است, این خطا را نمی گرداند اگر جدول وجود ندارد و یا پایگاه داده وجود ندارد.

    DROP DICTIONARY [IF EXISTS] [db.]name

دلس فرهنگ لغت.
اگر `IF EXISTS` مشخص شده است, این خطا را نمی گرداند اگر جدول وجود ندارد و یا پایگاه داده وجود ندارد.

## EXISTS {#exists}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

بازگرداندن یک `UInt8`- نوع ستون, که شامل ارزش واحد `0` اگر جدول یا پایگاه داده وجود ندارد, یا `1` اگر جدول در پایگاه داده مشخص شده وجود دارد.

## KILL QUERY {#kill-query}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

تلاش برای به زور خاتمه نمایش داده شد در حال حاضر در حال اجرا.
نمایش داده شد به فسخ از سیستم انتخاب شده است.جدول پردازش ها با استفاده از معیارهای تعریف شده در `WHERE` بند از `KILL` پرس و جو.

مثالها:

``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

فقط خواندنی کاربران تنها می تواند نمایش داده شد خود را متوقف کند.

به طور پیش فرض, نسخه ناهمزمان از نمایش داده شد استفاده شده است (`ASYNC`), که برای تایید است که نمایش داده شد را متوقف کرده اند منتظر نیست.

نسخه همزمان (`SYNC`) منتظر تمام نمایش داده شد برای متوقف کردن و نمایش اطلاعات در مورد هر فرایند به عنوان متوقف می شود.
پاسخ شامل `kill_status` ستون, که می تواند مقادیر زیر را:

1.  ‘finished’ – The query was terminated successfully.
2.  ‘waiting’ – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can't be stopped.

پرسوجوی تست (`TEST`) فقط چک حقوق کاربر و نمایش یک لیست از نمایش داده شد برای متوقف کردن.

## KILL MUTATION {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

تلاش برای لغو و حذف [جهشها](alter.md#alter-mutations) که در حال حاضر اجرای. جهش به لغو از انتخاب [`system.mutations`](../../operations/system_tables.md#system_tables-mutations) جدول با استفاده از فیلتر مشخص شده توسط `WHERE` بند از `KILL` پرس و جو.

آزمون پرس و جو (`TEST`) فقط چک حقوق کاربر و نمایش یک لیست از نمایش داده شد برای متوقف کردن.

مثالها:

``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

The query is useful when a mutation is stuck and cannot finish (e.g. if some function in the mutation query throws an exception when applied to the data contained in the table).

تغییرات در حال حاضر توسط جهش ساخته شده به عقب نورد نیست.

## OPTIMIZE {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

این پرس و جو تلاش می کند تا ادغام برنامه ریزی نشده از قطعات داده برای جداول با یک موتور جدول از [ادغام](../../engines/table_engines/mergetree_family/mergetree.md) خانواده

این `OPTMIZE` پرس و جو نیز برای پشتیبانی [ماده بینی](../../engines/table_engines/special/materializedview.md) و [بافر](../../engines/table_engines/special/buffer.md) موتورها. دیگر موتورهای جدول پشتیبانی نمی شوند.

زمانی که `OPTIMIZE` با استفاده از [تکرار غذای اصلی](../../engines/table_engines/mergetree_family/replication.md) خانواده از موتورهای جدول, تاتر ایجاد یک کار برای ادغام و منتظر اعدام در تمام گره (در صورتی که `replication_alter_partitions_sync` تنظیم فعال است).

-   اگر `OPTIMIZE` یک ادغام به هر دلیلی انجام نمی, این کار مشتری اطلاع نیست. برای فعال کردن اعلان ها از [ا\_فزون\_ف\_کوپ](../../operations/settings/settings.md#setting-optimize_throw_if_noop) تنظیمات.
-   اگر شما یک مشخص `PARTITION` فقط پارتیشن مشخص شده بهینه شده است. [نحوه تنظیم بیان پارتیشن](alter.md#alter-how-to-specify-part-expr).
-   اگر شما مشخص کنید `FINAL` حتی زمانی که تمام داده ها در حال حاضر در یک بخش بهینه سازی انجام شده است.
-   اگر شما مشخص کنید `DEDUPLICATE` و سپس به طور کامل یکسان ردیف خواهد بود deduplicated (تمام ستون ها در مقایسه با) آن را حس می کند تنها برای MergeTree موتور.

!!! warning "اخطار"
    `OPTIMIZE` می توانید رفع نیست “Too many parts” خطا.

## RENAME {#misc_operations-rename}

تغییر نام یک یا چند جدول.

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

همه جداول تحت قفل جهانی تغییر نام داد. تغییر نام جداول یک عملیات نور است. اگر شما یک پایگاه داده دیگر نشان داد پس از به, جدول خواهد شد به این پایگاه داده منتقل. اما, دایرکتوری ها با پایگاه داده باید اقامت در همان فایل سیستم (در غیر این صورت یک خطا بازگشته است).

## SET {#query-set}

``` sql
SET param = value
```

انتساب `value` به `param` [تنظیم](../../operations/settings/index.md) برای جلسه فعلی. شما نمی توانید تغییر دهید [تنظیمات سرور](../../operations/server_configuration_parameters/index.md) از این طرف

شما همچنین می توانید تمام مقادیر را از مشخصات تنظیمات مشخص شده در یک پرس و جو واحد تنظیم کنید.

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

برای کسب اطلاعات بیشتر, دیدن [تنظیمات](../../operations/settings/settings.md).

## TRUNCATE {#truncate}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

حذف تمام داده ها را از یک جدول. هنگامی که بند `IF EXISTS` حذف شده است, پرس و جو یک خطا می گرداند اگر جدول وجود ندارد.

این `TRUNCATE` پرسوجو برای پشتیبانی نمیشود [مشاهده](../../engines/table_engines/special/view.md), [پرونده](../../engines/table_engines/special/file.md), [URL](../../engines/table_engines/special/url.md) و [خالی](../../engines/table_engines/special/null.md) موتورهای جدول.

## USE {#use}

``` sql
USE db
```

به شما اجازه می دهد پایگاه داده فعلی را برای جلسه تنظیم کنید.
پایگاه داده فعلی برای جستجو برای جداول استفاده می شود اگر پایگاه داده به صراحت در پرس و جو با یک نقطه قبل از نام جدول تعریف نشده است.
این پرس و جو را نمی توان در هنگام استفاده از پروتکل قام ساخته شده, از هیچ مفهوم یک جلسه وجود دارد.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/misc/) <!--hide-->
