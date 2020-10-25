---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "\u062C\u062F\u0627\u0648\u0644 \u0633\u06CC\u0633\u062A\u0645"
---

# جداول سیستم {#system-tables}

جداول سیستم برای اجرای بخشی از قابلیت های سیستم استفاده می شود و برای دسترسی به اطلاعات در مورد چگونگی کار سیستم.
شما می توانید یک جدول سیستم را حذف کنید (اما شما می توانید جدا انجام).
جداول سیستم فایل های با داده ها بر روی دیسک و یا فایل های با ابرداده ندارد. سرور ایجاد تمام جداول سیستم زمانی که شروع می شود.
جداول سیستم فقط خواندنی.
این در واقع ‘system’ بانک اطلاعات.

## سیستم.\_نامهنویسی ناهمزمان {#system_tables-asynchronous_metrics}

شامل معیارهای که به صورت دوره ای در پس زمینه محاسبه می شود. مثلا, مقدار رم در حال استفاده.

ستونها:

-   `metric` ([رشته](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([جسم شناور64](../sql-reference/data-types/float.md)) — Metric value.

**مثال**

``` sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

``` text
┌─metric──────────────────────────────────┬──────value─┐
│ jemalloc.background_thread.run_interval │          0 │
│ jemalloc.background_thread.num_runs     │          0 │
│ jemalloc.background_thread.num_threads  │          0 │
│ jemalloc.retained                       │  422551552 │
│ jemalloc.mapped                         │ 1682989056 │
│ jemalloc.resident                       │ 1656446976 │
│ jemalloc.metadata_thp                   │          0 │
│ jemalloc.metadata                       │   10226856 │
│ UncompressedCacheCells                  │          0 │
│ MarkCacheFiles                          │          0 │
└─────────────────────────────────────────┴────────────┘
```

**همچنین نگاه کنید به**

-   [نظارت](monitoring.md) — Base concepts of ClickHouse monitoring.
-   [سیستم.متریک](#system_tables-metrics) — Contains instantly calculated metrics.
-   [سیستم.رویدادها](#system_tables-events) — Contains a number of events that have occurred.
-   [سیستم.\_اشکالزدایی](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.

## سیستم.خوشه {#system-clusters}

حاوی اطلاعاتی در مورد خوشه های موجود در فایل پیکربندی و سرورهای موجود در ان.

ستونها:

-   `cluster` (String) — The cluster name.
-   `shard_num` (UInt32) — The shard number in the cluster, starting from 1.
-   `shard_weight` (UInt32) — The relative weight of the shard when writing data.
-   `replica_num` (UInt32) — The replica number in the shard, starting from 1.
-   `host_name` (String) — The host name, as specified in the config.
-   `host_address` (String) — The host IP address obtained from DNS.
-   `port` (UInt16) — The port to use for connecting to the server.
-   `user` (String) — The name of the user for connecting to the server.
-   `errors_count` (اوینت32) - تعداد دفعاتی که این میزبان موفق به رسیدن به ماکت.
-   `estimated_recovery_time` (اوینت32) - ثانیه به سمت چپ تا زمانی که تعداد خطا ماکت صفر است و در نظر گرفته می شود به حالت عادی.

لطفا توجه داشته باشید که `errors_count` یک بار در هر پرس و جو به خوشه به روز, ولی `estimated_recovery_time` بر روی تقاضا محاسبه شده است. بنابراین می تواند یک مورد غیر صفر باشد `errors_count` و صفر `estimated_recovery_time`, که پرس و جو بعدی صفر خواهد شد `errors_count` و سعی کنید به استفاده از ماکت به عنوان اگر هیچ خطا.

**همچنین نگاه کنید به**

-   [موتور جدول توزیع شده است](../engines/table-engines/special/distributed.md)
-   [تنظیمات \_فرهنگ توزیع میشود](settings/settings.md#settings-distributed_replica_error_cap)
-   [پخش \_راپیشا\_را\_را\_را\_حالف\_لایف تنظیم](settings/settings.md#settings-distributed_replica_error_half_life)

## سیستم.ستونها {#system-columns}

حاوی اطلاعات در مورد ستون در تمام جداول.

شما می توانید از این جدول برای دریافت اطلاعات شبیه به [DESCRIBE TABLE](../sql-reference/statements/misc.md#misc-describe-table) پرس و جو, اما برای جداول متعدد در یک بار.

این `system.columns` جدول شامل ستون های زیر (نوع ستون در براکت نشان داده شده است):

-   `database` (String) — Database name.
-   `table` (String) — Table name.
-   `name` (String) — Column name.
-   `type` (String) — Column type.
-   `default_kind` (String) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`) برای مقدار پیش فرض, و یا یک رشته خالی اگر تعریف نشده است.
-   `default_expression` (String) — Expression for the default value, or an empty string if it is not defined.
-   `data_compressed_bytes` (UInt64) — The size of compressed data, in bytes.
-   `data_uncompressed_bytes` (UInt64) — The size of decompressed data, in bytes.
-   `marks_bytes` (UInt64) — The size of marks, in bytes.
-   `comment` (String) — Comment on the column, or an empty string if it is not defined.
-   `is_in_partition_key` (UInt8) — Flag that indicates whether the column is in the partition expression.
-   `is_in_sorting_key` (UInt8) — Flag that indicates whether the column is in the sorting key expression.
-   `is_in_primary_key` (UInt8) — Flag that indicates whether the column is in the primary key expression.
-   `is_in_sampling_key` (UInt8) — Flag that indicates whether the column is in the sampling key expression.

## سیستم.یاریدهندکان {#system-contributors}

حاوی اطلاعات در مورد همکاران. همه مربیان به صورت تصادفی. سفارش تصادفی در زمان اجرای پرس و جو است.

ستونها:

-   `name` (String) — Contributor (author) name from git log.

**مثال**

``` sql
SELECT * FROM system.contributors LIMIT 10
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

برای پیدا کردن خود را در جدول, استفاده از یک پرس و جو:

``` sql
SELECT * FROM system.contributors WHERE name='Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```

## سیستم.پایگاههای داده {#system-databases}

این جدول شامل یک ستون رشته ای به نام ‘name’ – the name of a database.
هر پایگاه داده که سرور می داند در مورد یک ورودی مربوطه را در جدول.
این جدول سیستم برای اجرای استفاده می شود `SHOW DATABASES` پرس و جو.

## سیستم.قطعات مجزا {#system_tables-detached_parts}

حاوی اطلاعات در مورد قطعات جدا شده از [ادغام](../engines/table-engines/mergetree-family/mergetree.md) میز این `reason` ستون مشخص می کند که چرا بخش جدا شد. برای قطعات کاربر جدا, دلیل خالی است. چنین قطعات را می توان با [ALTER TABLE ATTACH PARTITION\|PART](../sql-reference/statements/alter.md#alter_attach-partition) فرمان. برای توضیحات ستون های دیگر را ببینید [سیستم.قطعات](#system_tables-parts). اگر نام قسمت نامعتبر است, ارزش برخی از ستون ممکن است `NULL`. این قطعات را می توان با حذف [ALTER TABLE DROP DETACHED PART](../sql-reference/statements/alter.md#alter_drop-detached).

## سیستم.واژهنامهها {#system_tables-dictionaries}

حاوی اطلاعات در مورد [واژهنامهها خارجی](../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

ستونها:

-   `database` ([رشته](../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
-   `name` ([رشته](../sql-reference/data-types/string.md)) — [نام واژهنامه](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([شمار8](../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    -   `NOT_LOADED` — Dictionary was not loaded because it was not used.
    -   `LOADED` — Dictionary loaded successfully.
    -   `FAILED` — Unable to load the dictionary as a result of an error.
    -   `LOADING` — Dictionary is loading now.
    -   `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](../sql-reference/statements/system.md#query_language-system-reload-dictionary) پرس و جو, ایست, پیکربندی فرهنگ لغت تغییر کرده است).
    -   `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
-   `origin` ([رشته](../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
-   `type` ([رشته](../sql-reference/data-types/string.md)) — Type of a dictionary allocation. [ذخیره واژهنامهها در حافظه](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [نوع کلید](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key): کلید عددی ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) or Сomposite key ([رشته](../sql-reference/data-types/string.md)) — form “(type 1, type 2, …, type n)”.
-   `attribute.names` ([& حذف](../sql-reference/data-types/array.md)([رشته](../sql-reference/data-types/string.md))) — Array of [نام خصیصه](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) فراهم شده توسط فرهنگ لغت.
-   `attribute.types` ([& حذف](../sql-reference/data-types/array.md)([رشته](../sql-reference/data-types/string.md))) — Corresponding array of [انواع خصیصه](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) که توسط فرهنگ لغت فراهم شده است.
-   `bytes_allocated` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Amount of RAM allocated for the dictionary.
-   `query_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
-   `hit_rate` ([جسم شناور64](../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
-   `element_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of items stored in the dictionary.
-   `load_factor` ([جسم شناور64](../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
-   `source` ([رشته](../sql-reference/data-types/string.md)) — Text describing the [منبع داده](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) برای فرهنگ لغت.
-   `lifetime_min` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Minimum [طول عمر](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) از فرهنگ لغت در حافظه, پس از کلیک که تلاش می کند به بازنگری فرهنگ لغت (اگر `invalidate_query` قرار است, سپس تنها در صورتی که تغییر کرده است). تنظیم در ثانیه.
-   `lifetime_max` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Maximum [طول عمر](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) از فرهنگ لغت در حافظه, پس از کلیک که تلاش می کند به بازنگری فرهنگ لغت (اگر `invalidate_query` قرار است, سپس تنها در صورتی که تغییر کرده است). تنظیم در ثانیه.
-   `loading_start_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
-   `last_successful_update_time` ([DateTime](../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with external sources and investigate causes.
-   `loading_duration` ([Float32](../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
-   `last_exception` ([رشته](../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn't be created.

**مثال**

پیکربندی فرهنگ لغت.

``` sql
CREATE DICTIONARY dictdb.dict
(
    `key` Int64 DEFAULT -1,
    `value_default` String DEFAULT 'world',
    `value_expression` String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())
```

اطمینان حاصل کنید که فرهنگ لغت لود شده است.

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```

## سیستم.رویدادها {#system_tables-events}

حاوی اطلاعات در مورد تعدادی از حوادث که در سیستم رخ داده است. مثلا, در جدول, شما می توانید پیدا کنید که چگونه بسیاری از `SELECT` نمایش داده شد از سرور کلیک شروع پردازش شد.

ستونها:

-   `event` ([رشته](../sql-reference/data-types/string.md)) — Event name.
-   `value` ([UInt64](../sql-reference/data-types/int-uint.md)) — Number of events occurred.
-   `description` ([رشته](../sql-reference/data-types/string.md)) — Event description.

**مثال**

``` sql
SELECT * FROM system.events LIMIT 5
```

``` text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.                  │
│ SelectQuery                           │     8 │ Same as Query, but only for SELECT queries.                                                                                                                                                                                                                │
│ FileOpen                              │    73 │ Number of files opened.                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ Number of reads (read/pread) from a file descriptor. Does not include sockets.                                                                                                                                                                             │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.                                                                                                                                              │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**همچنین نگاه کنید به**

-   [سیستم.\_نامهنویسی ناهمزمان](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [سیستم.متریک](#system_tables-metrics) — Contains instantly calculated metrics.
-   [سیستم.\_اشکالزدایی](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [نظارت](monitoring.md) — Base concepts of ClickHouse monitoring.

## سیستم.توابع {#system-functions}

حاوی اطلاعات در مورد توابع عادی و جمع.

ستونها:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

## سیستم.بازداشت گرافیت {#system-graphite-retentions}

حاوی اطلاعات در مورد پارامترها [لغزش \_ نمودار](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) که در جداول با استفاده [اطلاعات دقیق](../engines/table-engines/mergetree-family/graphitemergetree.md) موتورها.

ستونها:

-   `config_name` ) رشته) - `graphite_rollup` نام پارامتر.
-   `regexp` (رشته) - یک الگوی برای نام متریک.
-   `function` (رشته) - نام تابع جمع.
-   `age` (UInt64) - حداقل سن دیتا در ثانیه.
-   `precision` (اوینت64) - چگونه دقیقا به تعریف سن داده ها در ثانیه.
-   `priority` (UInt16) - الگوی اولویت است.
-   `is_default` (UInt8) - آیا الگوی پیش فرض است.
-   `Tables.database` (مجموعه (رشته)) - مجموعه ای از نام جداول پایگاه داده که از `config_name` پارامتر.
-   `Tables.table` (صف (رشته)) - مجموعه ای از نام جدول که با استفاده از `config_name` پارامتر.

## سیستم.ادغام {#system-merges}

حاوی اطلاعات در مورد ادغام و جهش بخشی در حال حاضر در روند برای جداول در خانواده ادغام.

ستونها:

-   `database` (String) — The name of the database the table is in.
-   `table` (String) — Table name.
-   `elapsed` (Float64) — The time elapsed (in seconds) since the merge started.
-   `progress` (Float64) — The percentage of completed work from 0 to 1.
-   `num_parts` (UInt64) — The number of pieces to be merged.
-   `result_part_name` (String) — The name of the part that will be formed as the result of merging.
-   `is_mutation` (اوینت8) - 1 اگر این فرایند جهش بخشی است.
-   `total_size_bytes_compressed` (UInt64) — The total size of the compressed data in the merged chunks.
-   `total_size_marks` (UInt64) — The total number of marks in the merged parts.
-   `bytes_read_uncompressed` (UInt64) — Number of bytes read, uncompressed.
-   `rows_read` (UInt64) — Number of rows read.
-   `bytes_written_uncompressed` (UInt64) — Number of bytes written, uncompressed.
-   `rows_written` (UInt64) — Number of rows written.

## سیستم.متریک {#system_tables-metrics}

شامل معیارهای است که می تواند فورا محاسبه, و یا یک مقدار فعلی. مثلا, تعداد نمایش داده شد به طور همزمان پردازش و یا تاخیر ماکت فعلی. این جدول همیشه به روز.

ستونها:

-   `metric` ([رشته](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Int64](../sql-reference/data-types/int-uint.md)) — Metric value.
-   `description` ([رشته](../sql-reference/data-types/string.md)) — Metric description.

لیستی از معیارهای پشتیبانی شده شما می توانید در [همایش های بین المللیپردازنده](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) فایل منبع از خانه کلیک.

**مثال**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric─────────────────────┬─value─┬─description──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                      │     1 │ Number of executing queries                                                                                                                                                                      │
│ Merge                      │     0 │ Number of executing background merges                                                                                                                                                            │
│ PartMutation               │     0 │ Number of mutations (ALTER DELETE/UPDATE)                                                                                                                                                        │
│ ReplicatedFetch            │     0 │ Number of data parts being fetched from replicas                                                                                                                                                │
│ ReplicatedSend             │     0 │ Number of data parts being sent to replicas                                                                                                                                                      │
│ ReplicatedChecks           │     0 │ Number of data parts checking for consistency                                                                                                                                                    │
│ BackgroundPoolTask         │     0 │ Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches, or replication queue bookkeeping)                                                                                │
│ BackgroundSchedulePoolTask │     0 │ Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.   │
│ DiskSpaceReservedForMerge  │     0 │ Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.                                                                     │
│ DistributedSend            │     0 │ Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.                                                          │
└────────────────────────────┴───────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**همچنین نگاه کنید به**

-   [سیستم.\_نامهنویسی ناهمزمان](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [سیستم.رویدادها](#system_tables-events) — Contains a number of events that occurred.
-   [سیستم.\_اشکالزدایی](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [نظارت](monitoring.md) — Base concepts of ClickHouse monitoring.

## سیستم.\_اشکالزدایی {#system_tables-metric_log}

دارای تاریخچه معیارهای ارزش از جداول `system.metrics` و `system.events`, دوره ای به دیسک سرخ.
برای روشن کردن مجموعه تاریخچه معیارهای در `system.metric_log` ایجاد `/etc/clickhouse-server/config.d/metric_log.xml` با محتوای زیر:

``` xml
<yandex>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
    </metric_log>
</yandex>
```

**مثال**

``` sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
event_date:                                                 2020-02-18
event_time:                                                 2020-02-18 07:15:33
milliseconds:                                               554
ProfileEvent_Query:                                         0
ProfileEvent_SelectQuery:                                   0
ProfileEvent_InsertQuery:                                   0
ProfileEvent_FileOpen:                                      0
ProfileEvent_Seek:                                          0
ProfileEvent_ReadBufferFromFileDescriptorRead:              1
ProfileEvent_ReadBufferFromFileDescriptorReadFailed:        0
ProfileEvent_ReadBufferFromFileDescriptorReadBytes:         0
ProfileEvent_WriteBufferFromFileDescriptorWrite:            1
ProfileEvent_WriteBufferFromFileDescriptorWriteFailed:      0
ProfileEvent_WriteBufferFromFileDescriptorWriteBytes:       56
...
CurrentMetric_Query:                                        0
CurrentMetric_Merge:                                        0
CurrentMetric_PartMutation:                                 0
CurrentMetric_ReplicatedFetch:                              0
CurrentMetric_ReplicatedSend:                               0
CurrentMetric_ReplicatedChecks:                             0
...
```

**همچنین نگاه کنید به**

-   [سیستم.\_نامهنویسی ناهمزمان](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [سیستم.رویدادها](#system_tables-events) — Contains a number of events that occurred.
-   [سیستم.متریک](#system_tables-metrics) — Contains instantly calculated metrics.
-   [نظارت](monitoring.md) — Base concepts of ClickHouse monitoring.

## سیستم.اعداد {#system-numbers}

این جدول شامل یک UInt64 ستون به نام ‘number’ که شامل تقریبا تمام اعداد طبیعی با شروع از صفر.
شما می توانید این جدول برای تست استفاده, و یا اگر شما نیاز به انجام یک جستجو نیروی بی رحم.
بار خوانده شده از این جدول موازی نیست.

## سیستم.\_شماره حساب {#system-numbers-mt}

همان ‘system.numbers’ اما بار خوانده شده موازی هستند. اعداد را می توان در هر سفارش بازگشت.
مورد استفاده برای تست.

## سیستم.یک {#system-one}

این جدول شامل یک ردیف با یک ‘dummy’ در زیر8 ستون حاوی مقدار 0.
این جدول استفاده می شود اگر پرس و جو را انتخاب کنید از بند مشخص نیست.
این شبیه میز دوگانه است که در سایر موارد یافت می شود.

## سیستم.قطعات {#system_tables-parts}

حاوی اطلاعات در مورد بخش هایی از [ادغام](../engines/table-engines/mergetree-family/mergetree.md) میز

هر سطر توصیف یک بخش داده.

ستونها:

-   `partition` (String) – The partition name. To learn what a partition is, see the description of the [ALTER](../sql-reference/statements/alter.md#query_language_queries_alter) پرس و جو.

    فرشها:

    -   `YYYYMM` برای پارتیشن بندی خودکار در ماه.
    -   `any_string` هنگامی که پارتیشن بندی دستی.

-   `name` (`String`) – Name of the data part.

-   `active` (`UInt8`) – Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's deleted. Inactive data parts remain after merging.

-   `marks` (`UInt64`) – The number of marks. To get the approximate number of rows in a data part, multiply `marks` با دانه دانه دانه شاخص (معمولا 8192) (این اشاره برای دانه دانه تطبیقی کار نمی کند).

-   `rows` (`UInt64`) – The number of rows.

-   `bytes_on_disk` (`UInt64`) – Total size of all the data part files in bytes.

-   `data_compressed_bytes` (`UInt64`) – Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `data_uncompressed_bytes` (`UInt64`) – Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `marks_bytes` (`UInt64`) – The size of the file with marks.

-   `modification_time` (`DateTime`) – The time the directory with the data part was modified. This usually corresponds to the time of data part creation.\|

-   `remove_time` (`DateTime`) – The time when the data part became inactive.

-   `refcount` (`UInt32`) – The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges.

-   `min_date` (`Date`) – The minimum value of the date key in the data part.

-   `max_date` (`Date`) – The maximum value of the date key in the data part.

-   `min_time` (`DateTime`) – The minimum value of the date and time key in the data part.

-   `max_time`(`DateTime`) – The maximum value of the date and time key in the data part.

-   `partition_id` (`String`) – ID of the partition.

-   `min_block_number` (`UInt64`) – The minimum number of data parts that make up the current part after merging.

-   `max_block_number` (`UInt64`) – The maximum number of data parts that make up the current part after merging.

-   `level` (`UInt32`) – Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts.

-   `data_version` (`UInt64`) – Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than `data_version`).

-   `primary_key_bytes_in_memory` (`UInt64`) – The amount of memory (in bytes) used by primary key values.

-   `primary_key_bytes_in_memory_allocated` (`UInt64`) – The amount of memory (in bytes) reserved for primary key values.

-   `is_frozen` (`UInt8`) – Flag that shows that a partition data backup exists. 1, the backup exists. 0, the backup doesn't exist. For more details, see [FREEZE PARTITION](../sql-reference/statements/alter.md#alter_freeze-partition)

-   `database` (`String`) – Name of the database.

-   `table` (`String`) – Name of the table.

-   `engine` (`String`) – Name of the table engine without parameters.

-   `path` (`String`) – Absolute path to the folder with data part files.

-   `disk` (`String`) – Name of a disk that stores the data part.

-   `hash_of_all_files` (`String`) – [سیفون128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) از فایل های فشرده.

-   `hash_of_uncompressed_files` (`String`) – [سیفون128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) از فایل های غیر فشرده (فایل های با علامت, فایل شاخص و غیره.).

-   `uncompressed_hash_of_compressed_files` (`String`) – [سیفون128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) از داده ها در فایل های فشرده به عنوان اگر غیر فشرده شد.

-   `bytes` (`UInt64`) – Alias for `bytes_on_disk`.

-   `marks_size` (`UInt64`) – Alias for `marks_bytes`.

## سیستم.\_خروج {#system_tables-part-log}

این `system.part_log` جدول تنها در صورتی ایجاد می شود [\_خروج](server-configuration-parameters/settings.md#server_configuration_parameters-part-log) تنظیم سرور مشخص شده است.

این جدول حاوی اطلاعات در مورد اتفاقاتی که با رخ داده است [قطعات داده](../engines/table-engines/mergetree-family/custom-partitioning-key.md) در [ادغام](../engines/table-engines/mergetree-family/mergetree.md) جداول خانواده, مانند اضافه کردن و یا ادغام داده ها.

این `system.part_log` جدول شامل ستون های زیر است:

-   `event_type` (Enum) — Type of the event that occurred with the data part. Can have one of the following values:
    -   `NEW_PART` — Inserting of a new data part.
    -   `MERGE_PARTS` — Merging of data parts.
    -   `DOWNLOAD_PART` — Downloading a data part.
    -   `REMOVE_PART` — Removing or detaching a data part using [DETACH PARTITION](../sql-reference/statements/alter.md#alter_detach-partition).
    -   `MUTATE_PART` — Mutating of a data part.
    -   `MOVE_PART` — Moving the data part from the one disk to another one.
-   `event_date` (Date) — Event date.
-   `event_time` (DateTime) — Event time.
-   `duration_ms` (UInt64) — Duration.
-   `database` (String) — Name of the database the data part is in.
-   `table` (String) — Name of the table the data part is in.
-   `part_name` (String) — Name of the data part.
-   `partition_id` (String) — ID of the partition that the data part was inserted to. The column takes the ‘all’ ارزش اگر پارتیشن بندی توسط `tuple()`.
-   `rows` (UInt64) — The number of rows in the data part.
-   `size_in_bytes` (UInt64) — Size of the data part in bytes.
-   `merged_from` (Array(String)) — An array of names of the parts which the current part was made up from (after the merge).
-   `bytes_uncompressed` (UInt64) — Size of uncompressed bytes.
-   `read_rows` (UInt64) — The number of rows was read during the merge.
-   `read_bytes` (UInt64) — The number of bytes was read during the merge.
-   `error` (UInt16) — The code number of the occurred error.
-   `exception` (String) — Text message of the occurred error.

این `system.part_log` جدول پس از اولین قرار دادن داده ها به ایجاد `MergeTree` جدول

## سیستم.فرایندها {#system_tables-processes}

این جدول سیستم برای اجرای استفاده می شود `SHOW PROCESSLIST` پرس و جو.

ستونها:

-   `user` (String) – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the `default` کاربر. زمینه شامل نام کاربری برای یک پرس و جو خاص, نه برای پرس و جو که این پرس و جو شروع.
-   `address` (String) – The IP address the request was made from. The same for distributed processing. To track where a distributed query was originally made from, look at `system.processes` در سرور درخواست پرس و جو.
-   `elapsed` (Float64) – The time in seconds since request execution started.
-   `rows_read` (UInt64) – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `bytes_read` (UInt64) – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `total_rows_approx` (UInt64) – The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.
-   `memory_usage` (UInt64) – Amount of RAM the request uses. It might not include some types of dedicated memory. See the [\_کاساژ بیشینه](../operations/settings/query-complexity.md#settings_max_memory_usage) تنظیمات.
-   `query` (String) – The query text. For `INSERT` این شامل داده ها برای وارد کردن نیست.
-   `query_id` (String) – Query ID, if defined.

## سیستم.\_خروج {#system-tables-text-log}

شامل ورودی ورود به سیستم. سطح ورود به سیستم که می رود به این جدول را می توان با محدود `text_log.level` تنظیم سرور.

ستونها:

-   `event_date` (`Date`)- تاریخ ورود.
-   `event_time` (`DateTime`)- زمان ورود .
-   `microseconds` (`UInt32`)- میکروثانیه از ورود.
-   `thread_name` (String) — Name of the thread from which the logging was done.
-   `thread_id` (UInt64) — OS thread ID.
-   `level` (`Enum8`)- ورود به سطح .
    -   `'Fatal' = 1`
    -   `'Critical' = 2`
    -   `'Error' = 3`
    -   `'Warning' = 4`
    -   `'Notice' = 5`
    -   `'Information' = 6`
    -   `'Debug' = 7`
    -   `'Trace' = 8`
-   `query_id` (`String`)- شناسه پرس و جو .
-   `logger_name` (`LowCardinality(String)`) - Name of the logger (i.e. `DDLWorker`)
-   `message` (`String`)- پیام خود را.
-   `revision` (`UInt32`)- تجدید نظر کلیک کنیدهاوس .
-   `source_file` (`LowCardinality(String)`)- فایل منبع که از ورود به سیستم انجام شد .
-   `source_line` (`UInt64`)- خط منبع که از ورود به سیستم انجام شد.

## سیستم.\_خروج {#system_tables-query_log}

حاوی اطلاعات در مورد اجرای نمایش داده شد. برای هر پرس و جو, شما می توانید زمان شروع پردازش را ببینید, مدت زمان پردازش, پیام های خطا و اطلاعات دیگر.

!!! note "یادداشت"
    جدول حاوی اطلاعات ورودی برای `INSERT` نمایش داده شد.

تاتر این جدول را فقط در صورتی ایجاد می کند [\_خروج](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) پارامتر سرور مشخص شده است. این پارامتر مجموعه قوانین ورود به سیستم, مانند فاصله ورود به سیستم و یا نام جدول نمایش داده شد خواهد شد وارد سایت شوید.

برای فعال کردن ورود به سیستم پرس و جو, تنظیم [\_خروج](settings/settings.md#settings-log-queries) پارامتر به 1. برای اطلاعات بیشتر [تنظیمات](settings/settings.md) بخش.

این `system.query_log` جدول ثبت دو نوع نمایش داده شد:

1.  نمایش داده شد اولیه که به طور مستقیم توسط مشتری اجرا شد.
2.  کودک نمایش داده شد که توسط دیگر نمایش داده شد (برای اجرای پرس و جو توزیع). برای این نوع از نمایش داده شد, اطلاعات در مورد پدر و مادر نمایش داده شد در نشان داده شده است `initial_*` ستون ها

ستونها:

-   `type` (`Enum8`) — Type of event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` (Date) — Query starting date.
-   `event_time` (DateTime) — Query starting time.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` نمایش داده شد, تعداد ردیف نوشته شده. برای نمایش داده شد دیگر مقدار ستون 0 است.
-   `written_bytes` (UInt64) — For `INSERT` نمایش داده شد, تعداد بایت نوشته شده. برای نمایش داده شد دیگر مقدار ستون 0 است.
-   `result_rows` (UInt64) — Number of rows in the result.
-   `result_bytes` (UInt64) — Number of bytes in the result.
-   `memory_usage` (UInt64) — Memory consumption by the query.
-   `query` (String) — Query string.
-   `exception` (String) — Exception message.
-   `stack_trace` (String) — Stack trace (a list of methods called before the error occurred). An empty string, if the query is completed successfully.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [کلیک مشتری](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از مشتری تی پی اجرا می شود.
-   `client_name` (String) — The [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از نام مشتری تی پی.
-   `client_revision` (UInt32) — Revision of the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از مشتری تی پی.
-   `client_version_major` (UInt32) — Major version of the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از مشتری تی پی.
-   `client_version_minor` (UInt32) — Minor version of the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از مشتری تی پی.
-   `client_version_patch` (UInt32) — Patch component of the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از نسخه مشتری تی سی پی.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` روش مورد استفاده قرار گرفت.
    -   2 — `POST` روش مورد استفاده قرار گرفت.
-   `http_user_agent` (String) — The `UserAgent` هدر در درخواست قام منتقل می شود.
-   `quota_key` (String) — The “quota key” مشخص شده در [سهمیه](quotas.md) تنظیم (دیدن `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `thread_numbers` (Array(UInt32)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics. The description of them could be found in the table [سیستم.رویدادها](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics that are listed in the `ProfileEvents.Names` ستون.
-   `Settings.Names` (Array(String)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` پارامتر به 1.
-   `Settings.Values` (Array(String)) — Values of settings that are listed in the `Settings.Names` ستون.

هر پرس و جو ایجاد یک یا دو ردیف در `query_log` جدول بسته به وضعیت پرس و جو:

1.  اگر اجرای پرس و جو موفق است, دو رویداد با انواع 1 و 2 ایجاد می شوند (دیدن `type` ستون).
2.  اگر یک خطا در طول پردازش پرس و جو رخ داده است, دو رویداد با انواع 1 و 4 ایجاد می شوند.
3.  اگر یک خطا قبل از راه اندازی پرس و جو رخ داده است, یک رویداد واحد با نوع 3 ایجاد شده است.

به طور پیش فرض, سیاهههای مربوط به جدول در فواصل 7.5 ثانیه اضافه. شما می توانید این فاصله در مجموعه [\_خروج](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) تنظیم سرور (نگاه کنید به `flush_interval_milliseconds` پارامتر). به خیط و پیت کردن سیاهههای مربوط به زور از بافر حافظه را به جدول, استفاده از `SYSTEM FLUSH LOGS` پرس و جو.

هنگامی که جدول به صورت دستی حذف, به طور خودکار در پرواز ایجاد. توجه داشته باشید که تمام سیاهههای مربوط قبلی حذف خواهد شد.

!!! note "یادداشت"
    دوره ذخیره سازی برای سیاهههای مربوط نامحدود است. سیاهههای مربوط به طور خودکار از جدول حذف نمی شود. شما نیاز به سازماندهی حذف سیاهههای مربوط منسوخ شده خود را.

شما می توانید یک کلید پارتیشن بندی دلخواه برای مشخص `system.query_log` جدول در [\_خروج](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) تنظیم سرور (نگاه کنید به `partition_by` پارامتر).

## سیستم.\_ر\_خروج {#system_tables-query-thread-log}

جدول شامل اطلاعات در مورد هر موضوع اجرای پرس و جو.

تاتر این جدول را فقط در صورتی ایجاد می کند [\_ر\_خروج](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) پارامتر سرور مشخص شده است. این پارامتر مجموعه قوانین ورود به سیستم, مانند فاصله ورود به سیستم و یا نام جدول نمایش داده شد خواهد شد وارد سایت شوید.

برای فعال کردن ورود به سیستم پرس و جو, تنظیم [باز کردن](settings/settings.md#settings-log-query-threads) پارامتر به 1. برای اطلاعات بیشتر [تنظیمات](settings/settings.md) بخش.

ستونها:

-   `event_date` (Date) — the date when the thread has finished execution of the query.
-   `event_time` (DateTime) — the date and time when the thread has finished execution of the query.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` نمایش داده شد, تعداد ردیف نوشته شده. برای نمایش داده شد دیگر مقدار ستون 0 است.
-   `written_bytes` (UInt64) — For `INSERT` نمایش داده شد, تعداد بایت نوشته شده. برای نمایش داده شد دیگر مقدار ستون 0 است.
-   `memory_usage` (Int64) — The difference between the amount of allocated and freed memory in context of this thread.
-   `peak_memory_usage` (Int64) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `thread_name` (String) — Name of the thread.
-   `thread_number` (UInt32) — Internal thread ID.
-   `os_thread_id` (Int32) — OS thread ID.
-   `master_thread_id` (UInt64) — OS initial ID of initial thread.
-   `query` (String) — Query string.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [کلیک مشتری](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از مشتری تی پی اجرا می شود.
-   `client_name` (String) — The [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از نام مشتری تی پی.
-   `client_revision` (UInt32) — Revision of the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از مشتری تی پی.
-   `client_version_major` (UInt32) — Major version of the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از مشتری تی پی.
-   `client_version_minor` (UInt32) — Minor version of the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از مشتری تی پی.
-   `client_version_patch` (UInt32) — Patch component of the [کلیک مشتری](../interfaces/cli.md) یا یکی دیگر از نسخه مشتری تی سی پی.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` روش مورد استفاده قرار گرفت.
    -   2 — `POST` روش مورد استفاده قرار گرفت.
-   `http_user_agent` (String) — The `UserAgent` هدر در درخواست قام منتقل می شود.
-   `quota_key` (String) — The “quota key” مشخص شده در [سهمیه](quotas.md) تنظیم (دیدن `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics for this thread. The description of them could be found in the table [سیستم.رویدادها](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics for this thread that are listed in the `ProfileEvents.Names` ستون.

به طور پیش فرض, سیاهههای مربوط به جدول در فواصل 7.5 ثانیه اضافه. شما می توانید این فاصله در مجموعه [\_ر\_خروج](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) تنظیم سرور (نگاه کنید به `flush_interval_milliseconds` پارامتر). به خیط و پیت کردن سیاهههای مربوط به زور از بافر حافظه را به جدول, استفاده از `SYSTEM FLUSH LOGS` پرس و جو.

هنگامی که جدول به صورت دستی حذف, به طور خودکار در پرواز ایجاد. توجه داشته باشید که تمام سیاهههای مربوط قبلی حذف خواهد شد.

!!! note "یادداشت"
    دوره ذخیره سازی برای سیاهههای مربوط نامحدود است. سیاهههای مربوط به طور خودکار از جدول حذف نمی شود. شما نیاز به سازماندهی حذف سیاهههای مربوط منسوخ شده خود را.

شما می توانید یک کلید پارتیشن بندی دلخواه برای مشخص `system.query_thread_log` جدول در [\_ر\_خروج](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) تنظیم سرور (نگاه کنید به `partition_by` پارامتر).

## سیستم.\_قطع {#system_tables-trace_log}

حاوی ردیاب های پشته ای است که توسط پروفایل پرس و جو نمونه گیری می شود.

تاتر این جدول زمانی ایجاد می کند [\_قطع](server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) بخش پیکربندی سرور تنظیم شده است. همچنین [جستجو](settings/settings.md#query_profiler_real_time_period_ns) و [ایران در تهران](settings/settings.md#query_profiler_cpu_time_period_ns) تنظیمات باید تنظیم شود.

برای تجزیه و تحلیل سیاهههای مربوط, استفاده از `addressToLine`, `addressToSymbol` و `demangle` توابع درون گرایی.

ستونها:

-   `event_date` ([تاریخ](../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `timestamp_ns` ([UInt64](../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    هنگام اتصال به سرور توسط `clickhouse-client`, شما رشته شبیه به دیدن `Connected to ClickHouse server version 19.18.1 revision 54429.`. این فیلد شامل `revision` اما نه `version` از یک سرور.

-   `timer_type` ([شمار8](../sql-reference/data-types/enum.md)) — Timer type:

    -   `Real` نشان دهنده زمان دیوار ساعت.
    -   `CPU` نشان دهنده زمان پردازنده.

-   `thread_number` ([UInt32](../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([رشته](../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [\_خروج](#system_tables-query_log) جدول سیستم.

-   `trace` ([Array(UInt64)](../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**مثال**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-15
event_time:    2019-11-15 15:09:38
revision:      54428
timer_type:    Real
thread_number: 48
query_id:      acc4d61f-5bd1-4a3e-bc91-2180be37c915
trace:         [94222141367858,94222152240175,94222152325351,94222152329944,94222152330796,94222151449980,94222144088167,94222151682763,94222144088167,94222151682763,94222144088167,94222144058283,94222144059248,94222091840750,94222091842302,94222091831228,94222189631488,140509950166747,140509942945935]
```

## سیستم.تکرار {#system_tables-replicas}

شامل اطلاعات و وضعیت برای جداول تکرار ساکن بر روی سرور محلی.
این جدول را می توان برای نظارت استفاده می شود. جدول شامل یک ردیف برای هر تکرار \* جدول.

مثال:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

``` text
Row 1:
──────
database:                   merge
table:                      visits
engine:                     ReplicatedCollapsingMergeTree
is_leader:                  1
can_become_leader:          1
is_readonly:                0
is_session_expired:         0
future_parts:               1
parts_to_check:             0
zookeeper_path:             /clickhouse/tables/01-06/visits
replica_name:               example01-06-1.yandex.ru
replica_path:               /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:            9
queue_size:                 1
inserts_in_queue:           0
merges_in_queue:            1
part_mutations_in_queue:    0
queue_oldest_time:          2020-02-20 08:34:30
inserts_oldest_time:        1970-01-01 00:00:00
merges_oldest_time:         2020-02-20 08:34:30
part_mutations_oldest_time: 1970-01-01 00:00:00
oldest_part_to_get:
oldest_part_to_merge_to:    20200220_20284_20840_7
oldest_part_to_mutate_to:
log_max_index:              596273
log_pointer:                596274
last_queue_update:          2020-02-20 08:34:32
absolute_delay:             0
total_replicas:             2
active_replicas:            2
```

ستونها:

-   `database` (`String`)- نام پایگاه داده
-   `table` (`String`)- نام جدول
-   `engine` (`String`)- نام موتور جدول
-   `is_leader` (`UInt8`)- چه ماکت رهبر است.
    فقط یک ماکت در یک زمان می تواند رهبر باشد. رهبر برای انتخاب پس زمینه ادغام به انجام است.
    توجه داشته باشید که می نویسد را می توان به هر ماکت است که در دسترس است و یک جلسه در زک انجام, صرف نظر از اینکه این یک رهبر است.
-   `can_become_leader` (`UInt8`)- چه ماکت می تواند به عنوان یک رهبر انتخاب می شوند.
-   `is_readonly` (`UInt8`)- چه ماکت در حالت فقط خواندنی است.
    در این حالت روشن است اگر پیکربندی ندارد بخش با باغ وحش اگر یک خطای ناشناخته رخ داده است که reinitializing جلسات در باغ وحش و در طول جلسه reinitialization در باغ وحش.
-   `is_session_expired` (`UInt8`)- جلسه با باغ وحش منقضی شده است. در واقع همان `is_readonly`.
-   `future_parts` (`UInt32`)- تعداد قطعات داده است که به عنوان نتیجه درج و یا ادغام که هنوز انجام نشده است ظاهر می شود.
-   `parts_to_check` (`UInt32`)- تعداد قطعات داده در صف برای تایید. اگر شک وجود دارد که ممکن است صدمه دیده است بخشی در صف تایید قرار داده است.
-   `zookeeper_path` (`String`)- مسیر به داده های جدول در باغ وحش.
-   `replica_name` (`String`)- نام ماکت در باغ وحش. کپی های مختلف از همان جدول نام های مختلف.
-   `replica_path` (`String`)- مسیر به داده های ماکت در باغ وحش. همان الحاق ‘zookeeper\_path/replicas/replica\_path’.
-   `columns_version` (`Int32`)- تعداد نسخه از ساختار جدول . نشان می دهد که چند بار تغییر انجام شد. اگر کپی نسخه های مختلف, به این معنی برخی از کپی ساخته شده است همه از تغییر نکرده است.
-   `queue_size` (`UInt32`)- اندازه صف برای عملیات در حال انتظار برای انجام شود . عملیات شامل قرار دادن بلوک های داده ادغام و برخی اقدامات دیگر. معمولا همزمان با `future_parts`.
-   `inserts_in_queue` (`UInt32`)- تعداد درج بلوک از داده ها که نیاز به ساخته شده است . درج معمولا نسبتا به سرعت تکرار. اگر این تعداد بزرگ است, به این معنی چیزی اشتباه است.
-   `merges_in_queue` (`UInt32`)- تعداد ادغام انتظار ساخته شود. گاهی اوقات ادغام طولانی هستند, بنابراین این مقدار ممکن است بیشتر از صفر برای یک مدت طولانی.
-   `part_mutations_in_queue` (`UInt32`)- تعداد جهش در انتظار ساخته شده است.
-   `queue_oldest_time` (`DateTime`)- اگر `queue_size` بیشتر از 0, نشان می دهد که قدیمی ترین عملیات به صف اضافه شد.
-   `inserts_oldest_time` (`DateTime` دیدن وضعیت شبکه `queue_oldest_time`
-   `merges_oldest_time` (`DateTime` دیدن وضعیت شبکه `queue_oldest_time`
-   `part_mutations_oldest_time` (`DateTime` دیدن وضعیت شبکه `queue_oldest_time`

4 ستون بعدی یک مقدار غیر صفر تنها جایی که یک جلسه فعال با زک وجود دارد.

-   `log_max_index` (`UInt64`)- حداکثر تعداد ورودی در ورود به سیستم از فعالیت های عمومی.
-   `log_pointer` (`UInt64`) - حداکثر تعداد ورودی در ورود به سیستم از فعالیت های عمومی که ماکت کپی شده به صف اعدام خود را, به علاوه یک. اگر `log_pointer` بسیار کوچکتر از `log_max_index`, چیزی اشتباه است.
-   `last_queue_update` (`DateTime`)- هنگامی که صف در زمان گذشته به روز شد.
-   `absolute_delay` (`UInt64`)- تاخیر چقدر بزرگ در ثانیه ماکت فعلی است.
-   `total_replicas` (`UInt8`)- تعداد کل کپی شناخته شده از این جدول.
-   `active_replicas` (`UInt8`)- تعداد کپی از این جدول که یک جلسه در باغ وحش (یعنی تعداد تکرار عملکرد).

اگر شما درخواست تمام ستون, جدول ممکن است کمی کند کار, از چند بار خوانده شده از باغ وحش برای هر سطر ساخته شده.
اگر شما درخواست آخرین 4 ستون (log\_max\_index, log\_pointer, total\_replicas, active\_replicas) جدول با این نسخهها کار به سرعت.

مثلا, شما می توانید بررسی کنید که همه چیز به درستی کار مثل این:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

اگر این پرس و جو چیزی نمی گرداند, به این معنی که همه چیز خوب است.

## سیستم.تنظیمات {#system-tables-system-settings}

شامل اطلاعات در مورد تنظیمات جلسه برای کاربر فعلی.

ستونها:

-   `name` ([رشته](../sql-reference/data-types/string.md)) — Setting name.
-   `value` ([رشته](../sql-reference/data-types/string.md)) — Setting value.
-   `changed` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
-   `description` ([رشته](../sql-reference/data-types/string.md)) — Short setting description.
-   `min` ([Nullable](../sql-reference/data-types/nullable.md)([رشته](../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [قیدها](settings/constraints-on-settings.md#constraints-on-settings). اگر تنظیمات دارای حداقل مقدار, شامل [NULL](../sql-reference/syntax.md#null-literal).
-   `max` ([Nullable](../sql-reference/data-types/nullable.md)([رشته](../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [قیدها](settings/constraints-on-settings.md#constraints-on-settings). اگر تنظیمات دارای حداکثر مقدار, شامل [NULL](../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    -   `0` — Current user can change the setting.
    -   `1` — Current user can't change the setting.

**مثال**

مثال زیر نشان می دهد که چگونه برای دریافت اطلاعات در مورد تنظیمات که شامل نام `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_i%'
```

``` text
┌─name────────────────────────────────────────┬─value─────┬─changed─┬─description───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─min──┬─max──┬─readonly─┐
│ min_insert_block_size_rows                  │ 1048576   │       0 │ Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.                                                                         │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ min_insert_block_size_bytes                 │ 268435456 │       0 │ Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.                                                                        │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ read_backoff_min_interval_between_events_ms │ 1000      │       0 │ Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time. │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
└─────────────────────────────────────────────┴───────────┴─────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────┴──────┴──────────┘
```

استفاده از `WHERE changed` می تواند مفید باشد, مثلا, زمانی که شما می خواهید برای بررسی:

-   اینکه تنظیمات در پروندههای پیکربندی به درستی بارگذاری شوند یا در حال استفاده باشند.
-   تنظیماتی که در جلسه فعلی تغییر کرده است.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**همچنین نگاه کنید به**

-   [تنظیمات](settings/index.md#session-settings-intro)
-   [مجوز برای نمایش داده شد](settings/permissions-for-queries.md#settings_readonly)
-   [محدودیت در تنظیمات](settings/constraints-on-settings.md)

## سیستم.\_زبانهها {#system.table_engines}

``` text
┌─name───────────────────┬─value───────┐
│ max_threads            │ 8           │
│ use_uncompressed_cache │ 0           │
│ load_balancing         │ random      │
│ max_memory_usage       │ 10000000000 │
└────────────────────────┴─────────────┘
```

## سیستم.خرابی در حذف گواهینامهها {#system-merge_tree_settings}

حاوی اطلاعات در مورد تنظیمات برای `MergeTree` میز

ستونها:

-   `name` (String) — Setting name.
-   `value` (String) — Setting value.
-   `description` (String) — Setting description.
-   `type` (String) — Setting type (implementation specific string value).
-   `changed` (UInt8) — Whether the setting was explicitly defined in the config or explicitly changed.

## سیستم.\_زبانهها {#system-table-engines}

شامل شرح موتورهای جدول پشتیبانی شده توسط سرور و اطلاعات پشتیبانی از ویژگی های خود را.

این جدول شامل ستون های زیر (نوع ستون در براکت نشان داده شده است):

-   `name` (String) — The name of table engine.
-   `supports_settings` (UInt8) — Flag that indicates if table engine supports `SETTINGS` بند بند.
-   `supports_skipping_indices` (UInt8) — Flag that indicates if table engine supports [پرش شاخص](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — Flag that indicates if table engine supports [TTL](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — Flag that indicates if table engine supports clauses `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` و `SAMPLE_BY`.
-   `supports_replication` (UInt8) — Flag that indicates if table engine supports [تکرار داده ها](../engines/table-engines/mergetree-family/replication.md).
-   `supports_duduplication` (UInt8) — Flag that indicates if table engine supports data deduplication.

مثال:

``` sql
SELECT *
FROM system.table_engines
WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

``` text
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┐
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┘
```

**همچنین نگاه کنید به**

-   ادغام خانواده [بندهای پرسوجو](../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
-   کافکا [تنظیمات](../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table)
-   پیوستن [تنظیمات](../engines/table-engines/special/join.md#join-limitations-and-settings)

## سیستم.جداول {#system-tables}

حاوی ابرداده از هر جدول که سرور می داند در مورد. جداول جداگانه در نشان داده نمی شود `system.tables`.

این جدول شامل ستون های زیر (نوع ستون در براکت نشان داده شده است):

-   `database` (String) — The name of the database the table is in.

-   `name` (String) — Table name.

-   `engine` (String) — Table engine name (without parameters).

-   `is_temporary` (زیر8) - پرچم که نشان می دهد که جدول موقت است.

-   `data_path` (رشته) - مسیر به داده های جدول در سیستم فایل.

-   `metadata_path` (رشته) - مسیر به ابرداده جدول در سیستم فایل.

-   `metadata_modification_time` (تاریخ ساعت) - زمان شدن اصلاح ابرداده جدول.

-   `dependencies_database` - وابستگی پایگاه داده .

-   `dependencies_table` (رشته)) - وابستگی های جدول ([ماده بینی](../engines/table-engines/special/materializedview.md) جداول بر اساس جدول فعلی).

-   `create_table_query` (رشته) - پرس و جو که برای ایجاد جدول مورد استفاده قرار گرفت.

-   `engine_full` (رشته) - پارامترهای موتور جدول.

-   `partition_key` (رشته) - بیان کلید پارتیشن مشخص شده در جدول.

-   `sorting_key` (رشته) - عبارت کلیدی مرتب سازی مشخص شده در جدول.

-   `primary_key` (رشته) - عبارت کلیدی اولیه مشخص شده در جدول.

-   `sampling_key` (رشته) - نمونه عبارت کلیدی مشخص شده در جدول.

-   `storage_policy` (رشته) - سیاست ذخیره سازی:

    -   [ادغام](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [توزیع شده](../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64)) - تعداد کل ردیف آن است که اگر ممکن است به سرعت تعیین دقیق تعداد ردیف در جدول در غیر این صورت `Null` (از جمله زیرینگ `Buffer` جدول).

-   `total_bytes` (Nullable(UInt64)) - مجموع تعداد بایت, اگر آن را ممکن است به سرعت تعیین دقیق تعداد بایت به صورت جدول ذخیره در غیر این صورت `Null` (**نه** شامل هر ذخیره سازی زمینه ای).

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   اگر جدول ذخیره داده ها در حافظه, بازده تعداد تقریبی بایت مورد استفاده در حافظه.

این `system.tables` جدول در استفاده می شود `SHOW TABLES` اجرای پرس و جو.

## سیستم.باغ وحش {#system-zookeeper}

جدول وجود ندارد اگر باغ وحش پیکربندی نشده است. اجازه می دهد تا خواندن داده ها از خوشه باغ وحش تعریف شده در پیکربندی.
پرس و جو باید یک ‘path’ شرایط برابری در بند جایی که. این مسیر در باغ وحش برای کودکان که شما می خواهید برای دریافت اطلاعات برای است.

پرسوجو `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` خروجی داده ها برای همه کودکان در `/clickhouse` گره.
به داده های خروجی برای تمام گره های ریشه, نوشتن مسیر = ‘/’.
اگر مسیر مشخص شده در ‘path’ وجود ندارد, یک استثنا پرتاب خواهد شد.

ستونها:

-   `name` (String) — The name of the node.
-   `path` (String) — The path to the node.
-   `value` (String) — Node value.
-   `dataLength` (Int32) — Size of the value.
-   `numChildren` (Int32) — Number of descendants.
-   `czxid` (Int64) — ID of the transaction that created the node.
-   `mzxid` (Int64) — ID of the transaction that last changed the node.
-   `pzxid` (Int64) — ID of the transaction that last deleted or added descendants.
-   `ctime` (DateTime) — Time of node creation.
-   `mtime` (DateTime) — Time of the last modification of the node.
-   `version` (Int32) — Node version: the number of times the node was changed.
-   `cversion` (Int32) — Number of added or removed descendants.
-   `aversion` (Int32) — Number of changes to the ACL.
-   `ephemeralOwner` (Int64) — For ephemeral nodes, the ID of the session that owns this node.

مثال:

``` sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

``` text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```

## سیستم.جهشها {#system_tables-mutations}

جدول حاوی اطلاعات در مورد [جهشها](../sql-reference/statements/alter.md#alter-mutations) از جداول ادغام و پیشرفت خود را. هر دستور جهش توسط یک ردیف نشان داده شده است. جدول دارای ستون های زیر است:

**دادگان**, **جدول** - نام پایگاه داده و جدول که جهش استفاده شد .

**قطع عضو** - شناسه جهش. برای جداول تکرار این شناسه به نام زنود در مطابقت `<table_path_in_zookeeper>/mutations/` راهنمای در باغ وحش. برای جداول سه برابر شناسه مربوط به فایل نام در دایرکتوری داده ها از جدول.

**فرمان** - رشته فرمان جهش (بخشی از پرس و جو پس از `ALTER TABLE [db.]table`).

**\_بروزرسانی** - هنگامی که این دستور جهش برای اجرای ارسال شد .

**\_شمارهی بلوک.ا\_ضافه کردن**, **\_شمارهی بلوک.شماره** - ستون تو در تو . برای جهش از جداول تکرار, این شامل یک رکورد برای هر پارتیشن: شناسه پارتیشن و شماره بلوک که توسط جهش خریداری شد (در هر پارتیشن, تنها بخش هایی که حاوی بلوک با اعداد کمتر از تعداد بلوک های خریداری شده توسط جهش در پارتیشن که جهش خواهد شد). در جداول غیر تکرار, تعداد بلوک در تمام پارتیشن به صورت یک توالی واحد. این به این معنی است که برای جهش از جداول غیر تکرار, ستون یک رکورد با یک عدد بلوک واحد خریداری شده توسط جهش شامل.

**\_کوچکنمایی** - تعدادی از قطعات داده است که نیاز به جهش را به پایان برساند جهش یافته است .

**\_مخفی کردن** - توجه داشته باشید که حتی اگر `parts_to_do = 0` ممکن است که جهش جدول تکرار هنوز به دلیل درج طولانی در حال اجرا است که ایجاد بخش داده های جدید است که نیاز به جهش انجام می شود است.

اگر مشکلی با جهش برخی از قطعات وجود دارد, ستون های زیر حاوی اطلاعات اضافی:

**\_شروع مجدد** - نام جدید ترین بخش است که نمی تواند جهش یافته است.

**زمان \_رشته** - زمان جدید ترین شکست جهش بخشی .

**\_شروع مجدد** - پیام استثنا که باعث شکست جهش بخشی اخیر.

## سیستم.دیسکها {#system_tables-disks}

حاوی اطلاعات در مورد دیسک های تعریف شده در [پیکربندی کارساز](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

ستونها:

-   `name` ([رشته](../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
-   `path` ([رشته](../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
-   `free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
-   `total_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
-   `keep_free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` پارامتر پیکربندی دیسک.

## سیستم.داستان\_یابی {#system_tables-storage_policies}

حاوی اطلاعات در مورد سیاست های ذخیره سازی و حجم تعریف شده در [پیکربندی کارساز](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

ستونها:

-   `policy_name` ([رشته](../sql-reference/data-types/string.md)) — Name of the storage policy.
-   `volume_name` ([رشته](../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
-   `volume_priority` ([UInt64](../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration.
-   `disks` ([رشته)](../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
-   `max_data_part_size` ([UInt64](../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
-   `move_factor` ([جسم شناور64](../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.

اگر سیاست ذخیره سازی شامل بیش از یک حجم, سپس اطلاعات برای هر حجم در ردیف فرد از جدول ذخیره می شود.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/system_tables/) <!--hide-->
