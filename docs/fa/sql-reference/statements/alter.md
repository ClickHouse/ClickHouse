---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

این `ALTER` پرسوجو فقط برای پشتیبانی `*MergeTree` جداول و همچنین `Merge`و`Distributed`. پرس و جو دارای چندین تغییرات.

### دستکاری ستون {#column-manipulations}

تغییر ساختار جدول.

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

در پرس و جو, مشخص یک لیست از یک یا چند اقدامات کاما از هم جدا.
هر عمل یک عملیات بر روی یک ستون است.

اقدامات زیر پشتیبانی می شوند:

-   [ADD COLUMN](#alter_add-column) — Adds a new column to the table.
-   [DROP COLUMN](#alter_drop-column) — Deletes the column.
-   [CLEAR COLUMN](#alter_clear-column) — Resets column values.
-   [COMMENT COLUMN](#alter_comment-column) — Adds a text comment to the column.
-   [MODIFY COLUMN](#alter_modify-column) — Changes column's type, default expression and TTL.

این اقدامات در زیر توضیح داده شده است.

#### ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

یک ستون جدید به جدول با مشخص اضافه می کند `name`, `type`, [`codec`](create.md#codecs) و `default_expr` (نگاه کنید به بخش [عبارتهای پیشفرض](create.md#create-default-values)).

اگر `IF NOT EXISTS` بند گنجانده شده است, پرس و جو یک خطا بازگشت نیست اگر ستون در حال حاضر وجود دارد. اگر شما مشخص کنید `AFTER name_after` (نام ستون دیگر), ستون پس از یک مشخص شده در لیست ستون جدول اضافه. در غیر این صورت, ستون به پایان جدول اضافه. توجه داشته باشید که هیچ راهی برای اضافه کردن یک ستون به ابتدای جدول وجود دارد. برای زنجیره ای از اقدامات, `name_after` می تواند نام یک ستون است که در یکی از اقدامات قبلی اضافه شده است.

اضافه کردن یک ستون فقط تغییر ساختار جدول, بدون انجام هر گونه اقدامات با داده. داده ها بر روی دیسک پس از ظاهر نمی شود `ALTER`. اگر داده ها برای یک ستون از دست رفته در هنگام خواندن از جدول, این است که در با مقادیر پیش فرض پر (با انجام عبارت پیش فرض اگر یکی وجود دارد, و یا با استفاده از صفر یا رشته های خالی). ستون بر روی دیسک به نظر می رسد پس از ادغام قطعات داده (دیدن [ادغام](../../engines/table-engines/mergetree-family/mergetree.md)).

این رویکرد اجازه می دهد تا ما را به تکمیل `ALTER` پرس و جو فورا, بدون افزایش حجم داده های قدیمی.

مثال:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

ستون را با نام حذف می کند `name`. اگر `IF EXISTS` بند مشخص شده است, پرس و جو یک خطا بازگشت نیست اگر ستون وجود ندارد.

حذف داده ها از سیستم فایل. از این حذف تمام فایل های پرس و جو تقریبا بلافاصله تکمیل شده است.

مثال:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

بازنشانی تمام داده ها در یک ستون برای یک پارتیشن مشخص. اطلاعات بیشتر در مورد تنظیم نام پارتیشن در بخش [نحوه مشخص کردن عبارت پارتیشن](#alter-how-to-specify-part-expr).

اگر `IF EXISTS` بند مشخص شده است, پرس و جو یک خطا بازگشت نیست اگر ستون وجود ندارد.

مثال:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

می افزاید: نظر به ستون. اگر `IF EXISTS` بند مشخص شده است, پرس و جو یک خطا بازگشت نیست اگر ستون وجود ندارد.

هر ستون می تواند یک نظر داشته باشد. اگر یک نظر در حال حاضر برای ستون وجود دارد, یک نظر جدید رونویسی نظر قبلی.

نظرات در ذخیره می شود `comment_expression` ستون توسط [DESCRIBE TABLE](misc.md#misc-describe-table) پرس و جو.

مثال:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

#### MODIFY COLUMN {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL]
```

این پرسوجو تغییر میکند `name` ویژگیهای ستون:

-   نوع

-   عبارت پیشفرض

-   TTL

        For examples of columns TTL modifying, see [Column TTL](../engines/table_engines/mergetree_family/mergetree.md#mergetree-column-ttl).

اگر `IF EXISTS` بند مشخص شده است, پرس و جو یک خطا بازگشت نیست اگر ستون وجود ندارد.

هنگام تغییر نوع, ارزش ها به عنوان اگر تبدیل [نوع](../../sql-reference/functions/type-conversion-functions.md) توابع به کار گرفته شد. اگر تنها عبارت پیش فرض تغییر می کند, پرس و جو می کند هر چیزی پیچیده نیست, و تقریبا بلافاصله تکمیل.

مثال:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

چندین مرحله پردازش وجود دارد:

-   تهیه فایل های موقت (جدید) با داده های اصلاح شده.
-   تغییر نام فایل های قدیمی.
-   تغییر نام فایل های موقت (جدید) به نام های قدیمی.
-   حذف فایل های قدیمی.

فقط مرحله اول زمان طول می کشد. در صورتی که یک شکست در این مرحله وجود دارد, داده ها تغییر نکرده است.
در صورتی که یک شکست در یکی از مراحل پی در پی وجود دارد, داده ها را می توان به صورت دستی ترمیم. استثنا است اگر فایل های قدیمی از سیستم فایل حذف شد اما داده ها را برای فایل های جدید را به دیسک نوشته شده است و از دست داده بود.

این `ALTER` پرس و جو برای تغییر ستون تکرار شده است. دستورالعمل ها در باغ وحش ذخیره می شوند و سپس هر ماکت اعمال می شود. همه `ALTER` نمایش داده شد در همان جهت اجرا شود. پرس و جو منتظر اقدامات مناسب برای در کپی دیگر تکمیل شود. با این حال, پرس و جو برای تغییر ستون در یک جدول تکرار می تواند قطع, و تمام اقدامات غیر همزمان انجام خواهد شد.

#### تغییر محدودیت پرس و جو {#alter-query-limitations}

این `ALTER` پرس و جو به شما امکان ایجاد و حذف عناصر جداگانه (ستون) در ساختارهای داده های تو در تو, اما نه کل ساختارهای داده تو در تو. برای اضافه کردن یک ساختار داده های تو در تو, شما می توانید ستون با یک نام مانند اضافه `name.nested_name` و نوع `Array(T)`. ساختار داده های تو در تو معادل ستون های چندگانه با یک نام است که پیشوند مشابه قبل از نقطه است.

هیچ پشتیبانی برای حذف ستون ها در کلید اصلی یا کلید نمونه برداری (ستون هایی که در استفاده می شود) وجود ندارد. `ENGINE` عبارت). تغییر نوع ستون که در کلید اصلی گنجانده شده است تنها ممکن است اگر این تغییر باعث نمی شود داده ها به اصلاح شود (مثلا, شما مجاز به اضافه کردن مقادیر به شمارشی و یا برای تغییر یک نوع از `DateTime` به `UInt32`).

اگر `ALTER` پرس و جو برای ایجاد تغییرات جدول مورد نیاز کافی نیست شما می توانید یک جدول جدید ایجاد کنید و داده ها را با استفاده از داده ها کپی کنید [INSERT SELECT](insert-into.md#insert_query_insert-select) پرس و جو, سپس جداول با استفاده از تغییر [RENAME](misc.md#misc_operations-rename) پرس و جو و حذف جدول قدیمی. شما می توانید از [تاتر-کپی](../../operations/utilities/clickhouse-copier.md) به عنوان یک جایگزین برای `INSERT SELECT` پرس و جو.

این `ALTER` بلوک پرس و جو همه می خواند و می نویسد برای جدول. به عبارت دیگر, اگر طولانی `SELECT` در حال اجرا است در زمان `ALTER` پرس و جو `ALTER` پرس و جو منتظر خواهد ماند تا کامل شود. همزمان, تمام نمایش داده شد جدید به همان جدول صبر کنید در حالی که این `ALTER` در حال اجرا است.

برای جداول که داده های خود را ذخیره کنید (مانند `Merge` و `Distributed`), `ALTER` فقط ساختار جدول را تغییر می دهد و ساختار جداول تابع را تغییر نمی دهد. مثلا, زمانی که در حال اجرا را تغییر دهید برای یک `Distributed` جدول, شما همچنین نیاز به اجرا `ALTER` برای جداول در تمام سرور از راه دور.

### دستکاری با عبارات کلیدی {#manipulations-with-key-expressions}

دستور زیر پشتیبانی می شود:

``` sql
MODIFY ORDER BY new_expression
```

این فقط برای جداول در کار می کند [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) خانواده (از جمله
[تکرار](../../engines/table-engines/mergetree-family/replication.md) جدول). فرمان تغییر
[کلید مرتب سازی](../../engines/table-engines/mergetree-family/mergetree.md) از جدول
به `new_expression` (بیان و یا یک تاپل از عبارات). کلید اصلی یکسان باقی می ماند.

فرمان سبک وزن به این معنا است که تنها ابرداده را تغییر می دهد. برای حفظ اموال که بخش داده ها
ردیف ها توسط عبارت کلیدی مرتب سازی شما می توانید عبارات حاوی ستون های موجود اضافه کنید دستور داد
به کلید مرتب سازی (فقط ستون اضافه شده توسط `ADD COLUMN` فرمان در همان `ALTER` پرسوجو).

### دستکاری با شاخص های پرش داده {#manipulations-with-data-skipping-indices}

این فقط برای جداول در کار می کند [`*MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) خانواده (از جمله
[تکرار](../../engines/table-engines/mergetree-family/replication.md) جدول). عملیات زیر
در دسترس هستند:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` - می افزاید توضیحات شاخص به ابرداده جداول .

-   `ALTER TABLE [db].name DROP INDEX name` - حذف شرح شاخص از ابرداده جداول و حذف فایل های شاخص از دیسک.

این دستورات سبک وزن هستند به این معنا که فقط فراداده را تغییر می دهند یا فایل ها را حذف می کنند.
همچنین تکرار میشوند (همگامسازی فرادادههای شاخص از طریق باغ وحش).

### دستکاری با محدودیت {#manipulations-with-constraints}

مشاهده بیشتر در [قیدها](create.md#constraints)

محدودیت ها می توانند با استفاده از نحو زیر اضافه یا حذف شوند:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

نمایش داده شد اضافه خواهد شد و یا حذف ابرداده در مورد محدودیت از جدول به طوری که بلافاصله پردازش شده است.

بررسی قید *اعدام نخواهد شد* در داده های موجود اگر اضافه شد.

همه تغییرات در جداول تکرار در حال پخش به باغ وحش بنابراین خواهد شد در دیگر کپی اعمال می شود.

### دستکاری با پارتیشن ها و قطعات {#alter_manipulations-with-partitions}

عملیات زیر را با [پارتیشن ها](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) در دسترس هستند:

-   [DETACH PARTITION](#alter_detach-partition) – Moves a partition to the `detached` دایرکتوری و فراموش کرده ام.
-   [DROP PARTITION](#alter_drop-partition) – Deletes a partition.
-   [ATTACH PART\|PARTITION](#alter_attach-partition) – Adds a part or partition from the `detached` دایرکتوری به جدول.
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) – Copies the data partition from one table to another and adds.
-   [REPLACE PARTITION](#alter_replace-partition) - پارتیشن داده ها را از یک جدول به دیگری کپی می کند و جایگزین می شود.
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition)(\#تغییر\_موف\_ قابل تنظیم-پارتیشن) - پارتیشن داده را از یک جدول به دیگری حرکت دهید.
-   [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) - بازنشانی ارزش یک ستون مشخص شده در یک پارتیشن.
-   [CLEAR INDEX IN PARTITION](#alter_clear-index-partition) - بازنشانی شاخص ثانویه مشخص شده در یک پارتیشن.
-   [FREEZE PARTITION](#alter_freeze-partition) – Creates a backup of a partition.
-   [FETCH PARTITION](#alter_fetch-partition) – Downloads a partition from another server.
-   [MOVE PARTITION\|PART](#alter_move-partition) – Move partition/data part to another disk or volume.

<!-- -->

#### DETACH PARTITION {#alter_detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```

تمام داده ها را برای پارتیشن مشخص شده به `detached` فهرست راهنما. سرور فراموش در مورد پارتیشن داده جدا به عنوان اگر وجود ندارد. سرور نمی خواهد در مورد این داده ها می دانم تا زمانی که شما را به [ATTACH](#alter_attach-partition) پرس و جو.

مثال:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

اطلاعات بیشتر در مورد تنظیم بیان پارتیشن در یک بخش [نحوه مشخص کردن عبارت پارتیشن](#alter-how-to-specify-part-expr).

پس از پرس و جو اجرا شده است, شما می توانید هر کاری که می خواهید با داده ها در انجام `detached` directory — delete it from the file system, or just leave it.

This query is replicated – it moves the data to the `detached` دایرکتوری در تمام کپی. توجه داشته باشید که شما می توانید این پرس و جو تنها در یک ماکت رهبر را اجرا کند. برای پیدا کردن اگر یک ماکت یک رهبر است, انجام `SELECT` پرسوجو به [سیستم.تکرار](../../operations/system-tables.md#system_tables-replicas) جدول متناوبا, راحت تر به یک است `DETACH` پرس و جو در تمام کپی - همه کپی پرتاب یک استثنا, به جز ماکت رهبر.

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

پارتیشن مشخص شده را از جدول حذف می کند. این برچسب ها پرس و جو پارتیشن به عنوان غیر فعال و حذف داده ها به طور کامل, حدود در 10 دقیقه.

اطلاعات بیشتر در مورد تنظیم بیان پارتیشن در یک بخش [نحوه مشخص کردن عبارت پارتیشن](#alter-how-to-specify-part-expr).

The query is replicated – it deletes data on all replicas.

#### DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

بخش مشخص شده یا تمام قسمت های پارتیشن مشخص شده را از بین می برد `detached`.
اطلاعات بیشتر در مورد تنظیم بیان پارتیشن در یک بخش [نحوه مشخص کردن عبارت پارتیشن](#alter-how-to-specify-part-expr).

#### ATTACH PARTITION\|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

می افزاید داده ها به جدول از `detached` فهرست راهنما. ممکن است که به اضافه کردن داده ها برای کل پارتیشن و یا برای یک بخش جداگانه. مثالها:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

اطلاعات بیشتر در مورد تنظیم بیان پارتیشن در یک بخش [نحوه مشخص کردن عبارت پارتیشن](#alter-how-to-specify-part-expr).

این پرس و جو تکرار شده است. چک المثنی-ابتکار چه داده ها در وجود دارد `detached` فهرست راهنما. اگر داده وجود دارد, پرس و جو چک یکپارچگی خود را. اگر همه چیز درست است, پرس و جو می افزاید: داده ها به جدول. همه کپی دیگر دانلود داده ها از ماکت-مبتکر.

بنابراین شما می توانید داده ها را به `detached` دایرکتوری در یک ماکت, و استفاده از `ALTER ... ATTACH` پرس و جو برای اضافه کردن به جدول در تمام کپی.

#### ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

این پرس و جو پارتیشن داده را از `table1` به `table2` می افزاید داده ها به لیست موجود در `table2`. توجه داشته باشید که داده ها از حذف نمی شود `table1`.

برای پرس و جو به اجرا با موفقیت, شرایط زیر باید رعایت شود:

-   هر دو جدول باید همان ساختار دارند.
-   هر دو جدول باید کلید پارتیشن همان.

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

این پرس و جو پارتیشن داده را از `table1` به `table2` و جایگزین پارتیشن موجود در `table2`. توجه داشته باشید که داده ها از حذف نمی شود `table1`.

برای پرس و جو به اجرا با موفقیت, شرایط زیر باید رعایت شود:

-   هر دو جدول باید همان ساختار دارند.
-   هر دو جدول باید کلید پارتیشن همان.

#### MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

این پرس و جو انتقال پارتیشن داده ها از `table_source` به `table_dest` با حذف داده ها از `table_source`.

برای پرس و جو به اجرا با موفقیت, شرایط زیر باید رعایت شود:

-   هر دو جدول باید همان ساختار دارند.
-   هر دو جدول باید کلید پارتیشن همان.
-   هر دو جدول باید همان خانواده موتور باشد. (تکرار و یا غیر تکرار)
-   هر دو جدول باید سیاست ذخیره سازی همان.

#### CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

بازنشانی تمام مقادیر در ستون مشخص شده در یک پارتیشن. اگر `DEFAULT` بند هنگام ایجاد یک جدول تعیین شد, این پرس و جو مجموعه مقدار ستون به یک مقدار پیش فرض مشخص.

مثال:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

این پرس و جو یک نسخه پشتیبان تهیه محلی از یک پارتیشن مشخص شده ایجاد می کند. اگر `PARTITION` بند حذف شده است, پرس و جو ایجاد پشتیبان گیری از تمام پارتیشن در یک بار.

!!! note "یادداشت"
    تمامی مراحل پشتیبان گیری بدون توقف سرور انجام می شود.

توجه داشته باشید که برای جداول قدیمی مدل دهید شما می توانید پیشوند نام پارتیشن را مشخص کنید (به عنوان مثال, ‘2019’)- سپس پرس و جو پشتیبان گیری برای تمام پارتیشن مربوطه ایجاد می کند. اطلاعات بیشتر در مورد تنظیم بیان پارتیشن در یک بخش [نحوه مشخص کردن عبارت پارتیشن](#alter-how-to-specify-part-expr).

در زمان اجرای, برای یک تصویر لحظهای داده, پرس و جو ایجاد لینک به داده های جدول. پیوندهای سخت در دایرکتوری قرار می گیرند `/var/lib/clickhouse/shadow/N/...` کجا:

-   `/var/lib/clickhouse/` است دایرکتوری محل کلیک کار مشخص شده در پیکربندی.
-   `N` تعداد افزایشی از نسخه پشتیبان تهیه شده است.

!!! note "یادداشت"
    در صورت استفاده [مجموعه ای از دیسک برای ذخیره سازی داده ها در یک جدول](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) این `shadow/N` دایرکتوری به نظر می رسد در هر دیسک, ذخیره سازی قطعات داده که توسط همسان `PARTITION` اصطلاح.

همان ساختار دایرکتوری ها در داخل پشتیبان به عنوان داخل ایجاد شده است `/var/lib/clickhouse/`. پرس و جو انجام می شود ‘chmod’ برای همه پروندهها نوشتن رو ممنوع کردم

پس از ایجاد نسخه پشتیبان تهیه, شما می توانید داده ها را از کپی `/var/lib/clickhouse/shadow/` به سرور از راه دور و سپس از سرور محلی حذف کنید. توجه داشته باشید که `ALTER t FREEZE PARTITION` پرس و جو تکرار نشده است. این یک نسخه پشتیبان تهیه محلی تنها بر روی سرور محلی ایجاد می کند.

پرس و جو ایجاد نسخه پشتیبان تهیه تقریبا بلافاصله (اما برای اولین بار برای نمایش داده شد در حال حاضر به جدول مربوطه منتظر را به پایان برساند در حال اجرا).

`ALTER TABLE t FREEZE PARTITION` نسخه تنها داده, ابرداده جدول نیست. برای تهیه نسخه پشتیبان از فراداده های جدول فایل را کپی کنید `/var/lib/clickhouse/metadata/database/table.sql`

برای بازیابی اطلاعات از یک نسخه پشتیبان تهیه زیر را انجام دهید:

1.  ایجاد جدول اگر وجود ندارد. برای مشاهده پرس و جو, استفاده از .پرونده جدید `ATTACH` در این با `CREATE`).
2.  رونوشت داده از `data/database/table/` دایرکتوری در داخل پشتیبان گیری به `/var/lib/clickhouse/data/database/table/detached/` فهرست راهنما.
3.  بدو `ALTER TABLE t ATTACH PARTITION` نمایش داده شد برای اضافه کردن داده ها به یک جدول.

بازگرداندن از یک نسخه پشتیبان تهیه می کند نیاز به متوقف کردن سرور نیست.

برای کسب اطلاعات بیشتر در مورد پشتیبان گیری و بازیابی اطلاعات, دیدن [پشتیبان گیری داده ها](../../operations/backup.md) بخش.

#### CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

پرس و جو کار می کند شبیه به `CLEAR COLUMN`, اما بازنشانی شاخص به جای یک داده ستون.

#### FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

دانلود یک پارتیشن از سرور دیگر. این پرس و جو تنها برای جداول تکرار کار می کند.

پرس و جو می کند به شرح زیر است:

1.  پارتیشن را از سفال مشخص شده دانلود کنید. داخل ‘path-in-zookeeper’ شما باید یک مسیر به سفال در باغ وحش را مشخص کنید.
2.  سپس پرس و جو داده های دانلود شده را به `detached` دایرکتوری از `table_name` جدول استفاده از [ATTACH PARTITION\|PART](#alter_attach-partition) پرسوجو برای افزودن داده به جدول.

به عنوان مثال:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

توجه داشته باشید که:

-   این `ALTER ... FETCH PARTITION` پرس و جو تکرار نشده است. این پارتیشن را به `detached` دایرکتوری تنها بر روی سرور محلی.
-   این `ALTER TABLE ... ATTACH` پرس و جو تکرار شده است. این می افزاید: داده ها به تمام کپی. داده ها به یکی از کپی ها از `detached` فهرست راهنما, و به دیگران - از کپی همسایه.

قبل از دانلود, سیستم چک اگر پارتیشن وجود دارد و ساختار جدول مسابقات. ماکت مناسب ترین به طور خودکار از کپی سالم انتخاب شده است.

اگر چه پرس و جو نامیده می شود `ALTER TABLE`, این ساختار جدول را تغییر دهید و بلافاصله داده های موجود در جدول را تغییر دهید.

#### MOVE PARTITION\|PART {#alter_move-partition}

پارتیشن ها یا قطعات داده را به حجم یا دیسک دیگری منتقل می کند `MergeTree`- جدول موتور . ببینید [با استفاده از دستگاه های بلوک های متعدد برای ذخیره سازی داده ها](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

این `ALTER TABLE t MOVE` پرسوجو:

-   تکرار نشده, به دلیل کپی های مختلف می توانید سیاست های ذخیره سازی مختلف دارند.
-   بازگرداندن خطا در صورتی که دیسک یا حجم مشخص پیکربندی نشده است. پرس و جو نیز یک خطا را برمی گرداند اگر شرایط در حال حرکت داده, که مشخص شده در سیاست ذخیره سازی, نمی توان اعمال کرد.
-   می توانید یک خطا در مورد بازگشت, زمانی که داده ها به نقل مکان کرد در حال حاضر توسط یک فرایند پس زمینه نقل مکان کرد, همزمان `ALTER TABLE t MOVE` پرس و جو و یا به عنوان یک نتیجه از ادغام داده های پس زمینه. کاربر باید هر گونه اقدامات اضافی در این مورد انجام نمی.

مثال:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

#### نحوه تنظیم بیان پارتیشن {#alter-how-to-specify-part-expr}

شما می توانید بیان پارتیشن را مشخص کنید `ALTER ... PARTITION` نمایش داده شد به روش های مختلف:

-   به عنوان یک مقدار از `partition` ستون از `system.parts` جدول به عنوان مثال, `ALTER TABLE visits DETACH PARTITION 201901`.
-   به عنوان بیان از ستون جدول. ثابت ها و عبارات ثابت پشتیبانی می شوند. به عنوان مثال, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   با استفاده از شناسه پارتیشن. شناسه پارتیشن شناسه رشته پارتیشن (انسان قابل خواندن در صورت امکان) است که به عنوان نام پارتیشن در فایل سیستم و در باغ وحش استفاده می شود. شناسه پارتیشن باید در `PARTITION ID` بند, در یک نقل قول واحد. به عنوان مثال, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   در [ALTER ATTACH PART](#alter_attach-partition) و [DROP DETACHED PART](#alter_drop-detached) پرس و جو, برای مشخص کردن نام یک بخش, استفاده از رشته تحت اللفظی با ارزش از `name` ستون از [سیستم.قطعات مجزا](../../operations/system-tables.md#system_tables-detached_parts) جدول به عنوان مثال, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

استفاده از نقل قول در هنگام مشخص کردن پارتیشن بستگی به نوع بیان پارتیشن. برای مثال برای `String` نوع, شما باید برای مشخص کردن نام خود را در نقل قول (`'`). برای `Date` و `Int*` انواع بدون نقل قول مورد نیاز است.

برای جداول قدیمی به سبک, شما می توانید پارتیشن یا به عنوان یک عدد مشخص `201901` یا یک رشته `'201901'`. نحو برای جداول سبک جدید سختگیرانه تر با انواع است (شبیه به تجزیه کننده برای فرمت ورودی ارزش).

تمام قوانین فوق نیز برای درست است [OPTIMIZE](misc.md#misc_operations-optimize) پرس و جو. اگر شما نیاز به مشخص کردن تنها پارتیشن در هنگام بهینه سازی یک جدول غیر تقسیم, تنظیم بیان `PARTITION tuple()`. به عنوان مثال:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

نمونه هایی از `ALTER ... PARTITION` نمایش داده شد در تست نشان داده شده است [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) و [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### دستکاری با جدول جدول {#manipulations-with-table-ttl}

شما می توانید تغییر دهید [جدول](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) با درخواست فرم زیر:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### همزمانی تغییر نمایش داده شد {#synchronicity-of-alter-queries}

برای جداول غیر قابل تکرار, همه `ALTER` نمایش داده شد همزمان انجام می شود. برای جداول تکرار, پرس و جو فقط می افزاید دستورالعمل برای اقدامات مناسب به `ZooKeeper` و اقدامات خود را در اسرع وقت انجام می شود. با این حال, پرس و جو می توانید صبر کنید برای این اقدامات در تمام کپی تکمیل شود.

برای `ALTER ... ATTACH|DETACH|DROP` نمایش داده شد, شما می توانید با استفاده از `replication_alter_partitions_sync` راه اندازی به راه اندازی انتظار.
مقادیر ممکن: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

### جهشها {#alter-mutations}

جهش یک نوع پرس و جو را تغییر دهید که اجازه می دهد تا تغییر یا حذف ردیف در یک جدول. در مقایسه با استاندارد `UPDATE` و `DELETE` نمایش داده شد که برای تغییرات داده نقطه در نظر گرفته شده, جهش برای عملیات سنگین است که تغییر بسیاری از ردیف در یک جدول در نظر گرفته شده. پشتیبانی برای `MergeTree` خانواده از موتورهای جدول از جمله موتورهای با پشتیبانی تکرار.

جداول موجود برای جهش به عنوان (بدون تبدیل لازم), اما پس از جهش برای اولین بار است که به یک جدول اعمال, فرمت ابرداده خود را با نسخه های سرور قبلی ناسازگار می شود و سقوط به نسخه های قبلی غیر ممکن می شود.

دستورات در حال حاضر در دسترس:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

این `filter_expr` باید از نوع باشد `UInt8`. پرس و جو حذف ردیف در جدول که این عبارت طول می کشد یک مقدار غیر صفر.

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

این `filter_expr` باید از نوع باشد `UInt8`. این پرس و جو به روز رسانی مقادیر ستون مشخص شده به ارزش عبارات مربوطه در ردیف که `filter_expr` طول می کشد یک مقدار غیر صفر. ارزش ها به نوع ستون با استفاده از قالبی `CAST` اپراتور به روز رسانی ستون هایی که در محاسبه اولیه استفاده می شود و یا کلید پارتیشن پشتیبانی نمی شود.

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

پرس و جو بازسازی شاخص ثانویه `name` در پارتیشن `partition_name`.

یک پرس و جو می تواند شامل چندین دستورات جدا شده توسط کاما.

برای \* جداول ادغام جهش اجرا با بازنویسی تمام قطعات داده. هیچ اتمیتی وجود ندارد - قطعات برای قطعات جهش یافته جایگزین می شوند به محض اینکه اماده باشند و `SELECT` پرس و جو است که شروع به اجرای در طول جهش داده ها از قطعات است که در حال حاضر همراه با داده ها از قطعات است که هنوز جهش یافته شده اند جهش را ببینید.

جهش ها به طور کامل توسط نظم خلقت خود دستور داده می شوند و به هر بخش به این ترتیب اعمال می شوند. جهش نیز تا حدی با درج دستور داد - داده هایی که به جدول وارد شد قبل از جهش ارسال خواهد شد جهش یافته و داده هایی که پس از که قرار داده شد جهش یافته نمی شود. توجه داشته باشید که جهش درج به هیچ وجه مسدود نیست.

یک جهش پرس و جو می گرداند بلافاصله پس از جهش مطلب اضافه شده است (در صورت تکرار جداول به باغ وحش برای nonreplicated جداول - به فایل سیستم). جهش خود را اجرا ناهمگام با استفاده از تنظیمات مشخصات سیستم. برای پیگیری پیشرفت جهش شما می توانید با استفاده از [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) جدول یک جهش است که با موفقیت ارسال شد ادامه خواهد داد برای اجرای حتی اگر سرور کلیک دوباره راه اندازی. هیچ راهی برای عقب انداختن جهش هنگامی که ارسال شده است وجود دارد, اما اگر جهش برای برخی از دلیل گیر می تواند با لغو [`KILL MUTATION`](misc.md#kill-mutation) پرس و جو.

مطالب برای جهش به پایان رسید حذف نمی حق دور (تعداد نوشته های حفظ شده توسط تعیین `finished_mutations_to_keep` پارامتر موتور ذخیره سازی). نوشته جهش قدیمی تر حذف می شوند.

## ALTER USER {#alter-user-statement}

تغییرات حساب کاربر کلیک.

### نحو {#alter-user-syntax}

``` sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### توصیف {#alter-user-dscr}

برای استفاده `ALTER USER` شما باید [ALTER USER](grant.md#grant-access-management) امتیاز.

### مثالها {#alter-user-examples}

تنظیم نقش های اعطا شده به عنوان پیش فرض:

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

اگر نقش قبلا به یک کاربر اعطا نمی, تاتر می اندازد یک استثنا.

تنظیم تمام نقش های اعطا شده به طور پیش فرض:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

اگر یک نقش به یک کاربر در اینده اعطا خواهد شد به طور پیش فرض به طور خودکار تبدیل خواهد شد.

تنظیم تمام نقش های اعطا شده به استثنای پیش فرض `role1` و `role2`:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

## ALTER ROLE {#alter-role-statement}

نقش ها را تغییر می دهد.

### نحو {#alter-role-syntax}

``` sql
ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

## ALTER ROW POLICY {#alter-row-policy-statement}

سیاست تغییرات ردیف.

### نحو {#alter-row-policy-syntax}

``` sql
ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## ALTER QUOTA {#alter-quota-statement}

سهمیه تغییرات.

### نحو {#alter-quota-syntax}

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## ALTER SETTINGS PROFILE {#alter-settings-profile-statement}

سهمیه تغییرات.

### نحو {#alter-settings-profile-syntax}

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/alter/) <!--hide-->
