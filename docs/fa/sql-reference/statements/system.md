---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: SYSTEM
---

# نمایش داده شد سیستم {#query-language-system}

-   [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
-   [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
-   [DROP DNS CACHE](#query_language-system-drop-dns-cache)
-   [DROP MARK CACHE](#query_language-system-drop-mark-cache)
-   [FLUSH LOGS](#query_language-system-flush_logs)
-   [RELOAD CONFIG](#query_language-system-reload-config)
-   [SHUTDOWN](#query_language-system-shutdown)
-   [KILL](#query_language-system-kill)
-   [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
-   [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
-   [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)
-   [STOP MERGES](#query_language-system-stop-merges)
-   [START MERGES](#query_language-system-start-merges)

## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

بارگذاری مجدد تمام لغت نامه که با موفقیت قبل از لود شده است.
به طور پیش فرض, لغت نامه ها به صورت تنبلی لود (دیدن [_بارگیری کامل](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)), بنابراین به جای اینکه به طور خودکار در هنگام راه اندازی لود, در اولین دسترسی از طریق تابع دیکته مقداردهی اولیه و یا از جداول با موتور = فرهنگ لغت را انتخاب کنید. این `SYSTEM RELOAD DICTIONARIES` پرس و جو بارگذاری مجدد از جمله لغت نامه (لود شده).
همیشه باز می گردد `Ok.` صرف نظر از نتیجه به روز رسانی فرهنگ لغت.

## بازخوانی لغتنامهها {#query_language-system-reload-dictionary}

به طور کامل یک فرهنگ لغت را دوباره بارگذاری کنید `dictionary_name` بدون در نظر گرفتن دولت از فرهنگ لغت (لود / NOT_LOADED / شکست خورده).
همیشه باز می گردد `Ok.` صرف نظر از نتیجه به روز رسانی فرهنگ لغت.
وضعیت فرهنگ لغت را می توان با پرس و جو بررسی کرد `system.dictionaries` جدول

``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

کش دی ان اس داخلی بازنشانی را کلیک کنید. گاهی اوقات (برای ClickHouse نسخه) لازم است برای استفاده از این دستور هنگامی که در حال تغییر زیرساخت ها (تغییر آدرس IP دیگر ClickHouse سرور یا سرور استفاده شده توسط لغت نامه).

برای راحت تر (اتوماتیک) مدیریت کش دیدن disable_internal_dns_cache, dns_cache_update_period پارامترهای.

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

بازنشانی کش علامت. مورد استفاده در توسعه تست های کلیک و عملکرد.

## FLUSH LOGS {#query_language-system-flush_logs}

Flushes buffers of log messages to system tables (e.g. system.query_log). Allows you to not wait 7.5 seconds when debugging.

## RELOAD CONFIG {#query_language-system-reload-config}

بارگذاری مجدد پیکربندی محل کلیک. استفاده می شود که پیکربندی در باغ وحش ذخیره می شود.

## SHUTDOWN {#query_language-system-shutdown}

به طور معمول خاموش کردن کلیک (مانند `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

سقط فرایند کلیک (مانند `kill -9 {$ pid_clickhouse-server}`)

## مدیریت جداول توزیع شده {#query-language-system-distributed}

کلیک خانه می تواند مدیریت کند [توزیع شده](../../engines/table-engines/special/distributed.md) میز هنگامی که یک کاربر درج داده ها را به این جداول, خانه رعیتی برای اولین بار ایجاد یک صف از داده ها است که باید به گره های خوشه ای ارسال, سپس ناهمگام می فرستد. شما می توانید پردازش صف با مدیریت [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed) و [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) نمایش داده شد. شما همچنین می توانید همزمان داده های توزیع شده را با `insert_distributed_sync` تنظیمات.

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

غیرفعال توزیع داده های پس زمینه در هنگام قرار دادن داده ها به جداول توزیع شده است.

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

نیروهای خانه را کلیک کنید برای ارسال داده ها به گره های خوشه همزمان. اگر هر گره در دسترس نیست, تاتر می اندازد یک استثنا و متوقف می شود اجرای پرس و جو. شما می توانید پرس و جو را دوباره امتحان کنید تا زمانی که موفق, که اتفاق خواهد افتاد زمانی که تمام گره ها در حال بازگشت.

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

توزیع داده های پس زمینه را هنگام قرار دادن داده ها به جداول توزیع می کند.

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### STOP MERGES {#query_language-system-stop-merges}

فراهم می کند امکان متوقف ادغام پس زمینه برای جداول در خانواده ادغام:

``` sql
SYSTEM STOP MERGES [[db.]merge_tree_family_table_name]
```

!!! note "یادداشت"
    `DETACH / ATTACH` جدول پس زمینه ادغام برای جدول شروع خواهد شد حتی در صورتی که ادغام برای تمام جداول ادغام قبل از متوقف شده است.

### START MERGES {#query_language-system-start-merges}

فراهم می کند امکان شروع پس زمینه ادغام برای جداول در خانواده ادغام:

``` sql
SYSTEM START MERGES [[db.]merge_tree_family_table_name]
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/system/) <!--hide-->
