---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: INSERT INTO
---

## INSERT {#insert}

اضافه کردن داده ها.

قالب پرس و جو عمومی:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

پرسوجو میتواند فهرستی از ستونها را برای درج مشخص کند `[(c1, c2, c3)]`. در این مورد, بقیه ستون ها با پر:

-   مقادیر محاسبه شده از `DEFAULT` عبارات مشخص شده در تعریف جدول.
-   صفر و رشته خالی, اگر `DEFAULT` عبارات تعریف نشده.

اگر [_مرحلهای دقیق = 1](../../operations/settings/settings.md) ستون هایی که ندارند `DEFAULT` تعریف شده باید در پرس و جو ذکر شده است.

داده ها را می توان به درج در هر گذشت [قالب](../../interfaces/formats.md#formats) پشتیبانی شده توسط فاحشه خانه. قالب باید به صراحت در پرس و جو مشخص شود:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

For example, the following query format is identical to the basic version of INSERT … VALUES:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

تاتر حذف تمام فضاها و خوراک یک خط (در صورتی که وجود دارد) قبل از داده ها. هنگام تشکیل یک پرس و جو, توصیه می کنیم قرار دادن داده ها بر روی یک خط جدید پس از اپراتورهای پرس و جو (این مهم است که اگر داده ها با فضاهای شروع می شود).

مثال:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

شما می توانید داده ها را به طور جداگانه از پرس و جو با استفاده از مشتری خط فرمان یا رابط اچ تی پی وارد کنید. برای کسب اطلاعات بیشتر به بخش مراجعه کنید “[واسط](../../interfaces/index.md#interfaces)”.

### قیدها {#constraints}

اگر جدول [قیدها](create.md#constraints), their expressions will be checked for each row of inserted data. If any of those constraints is not satisfied — server will raise an exception containing constraint name and expression, the query will be stopped.

### قرار دادن نتایج `SELECT` {#insert_query_insert-select}

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

ستون ها با توجه به موقعیت خود را در بند را انتخاب کنید نقشه برداری. با این حال, نام خود را در عبارت را انتخاب کنید و جدول برای درج ممکن است متفاوت باشد. در صورت لزوم نوع ریخته گری انجام می شود.

هیچ یک از فرمت های داده به جز مقادیر اجازه تنظیم مقادیر به عبارات مانند `now()`, `1 + 2` و به همین ترتیب. فرمت ارزش اجازه می دهد تا استفاده محدود از عبارات, اما این توصیه نمی شود, چرا که در این مورد کد کم است برای اجرای خود استفاده می شود.

نمایش داده شد دیگر برای تغییر قطعات داده ها پشتیبانی نمی شوند: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
با این حال, شما می توانید داده های قدیمی با استفاده از حذف `ALTER TABLE ... DROP PARTITION`.

`FORMAT` بند باید در پایان پرس و جو مشخص شود اگر `SELECT` بند شامل تابع جدول [ورودی()](../table-functions/input.md).

### ملاحظات عملکرد {#performance-considerations}

`INSERT` داده های ورودی را با کلید اصلی مرتب می کند و توسط یک کلید پارتیشن به پارتیشن تقسیم می شود. اگر داده ها را به چندین پارتیشن در یک بار وارد کنید می تواند به طور قابل توجهی عملکرد را کاهش دهد `INSERT` پرس و جو. برای جلوگیری از این:

-   اضافه کردن داده ها در دسته نسبتا بزرگ, مانند 100,000 ردیف در یک زمان.
-   داده های گروه توسط یک کلید پارتیشن قبل از بارگذاری به کلیک.

عملکرد کاهش نخواهد یافت اگر:

-   داده ها در زمان واقعی اضافه شده است.
-   شما داده ها است که معمولا بر اساس زمان طبقه بندی شده اند را بارگذاری کنید.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/insert_into/) <!--hide-->
