---
description: 'يتيح هذا المحرّك معالجة ملفات سجلّات التطبيقات على هيئة تدفّق من
  السجلات.'
sidebar_label: 'FileLog'
sidebar_position: 160
slug: /engines/table-engines/special/filelog
title: 'محرّك الجدول FileLog'
doc_type: 'reference'
---

يتيح هذا المحرّك معالجة ملفات سجلّات التطبيقات على هيئة تدفّق من السجلات.

يتيح لك `FileLog` ما يلي:

* الاشتراك في ملفات السجلّات.
* معالجة السجلات الجديدة عند إضافتها إلى ملفات السجلّات المشترَك فيها.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = FileLog('path_to_logs', 'format_name') SETTINGS
    [poll_timeout_ms = 0,]
    [poll_max_batch_size = 0,]
    [max_block_size = 0,]
    [max_threads = 0,]
    [poll_directory_watch_events_backoff_init = 500,]
    [poll_directory_watch_events_backoff_max = 32000,]
    [poll_directory_watch_events_backoff_factor = 2,]
    [handle_error_mode = 'default']
```

وسيطات المحرك:

* `path_to_logs` – المسار إلى ملفات السجل المطلوب متابعتها. يمكن أن يكون مسارًا إلى دليل يحتوي على ملفات سجل أو إلى ملف سجل واحد. لاحظ أن ClickHouse لا يسمح إلا بالمسارات الموجودة داخل الدليل `user_files`.
* `format_name` - تنسيق السجل. لاحظ أن FileLog يعالج كل سطر في الملف على أنه سجل منفصل، لذلك لا تكون كل تنسيقات البيانات مناسبة له.

المعلمات الاختيارية:

* `poll_timeout_ms` - مهلة الاستقصاء الواحدة من ملف السجل. القيمة الافتراضية: [stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
* `poll_max_batch_size` — الحد الأقصى لعدد السجلات التي يمكن استقصاؤها في عملية استقصاء واحدة. القيمة الافتراضية: [max&#95;block&#95;size](/ar/operations/settings/settings#max_block_size).
* `max_block_size` — الحد الأقصى لحجم الدفعة (بعدد السجلات) لعملية الاستقصاء. القيمة الافتراضية: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `max_threads` - الحد الأقصى لعدد الخيوط المستخدمة لتحليل الملفات، والقيمة الافتراضية هي 0، ما يعني أن العدد سيكون max(1, physical&#95;cpu&#95;cores / 4).
* `poll_directory_watch_events_backoff_init` - قيمة `sleep` الأولية لخيط مراقبة الدليل. القيمة الافتراضية: `500`.
* `poll_directory_watch_events_backoff_max` - قيمة `sleep` القصوى لخيط مراقبة الدليل. القيمة الافتراضية: `32000`.
* `poll_directory_watch_events_backoff_factor` - سرعة `backoff`، وهي أسية افتراضيًا. القيمة الافتراضية: `2`.
* `handle_error_mode` — كيفية التعامل مع الأخطاء في محرك FileLog. القيم الممكنة: default (سيُطلَق استثناء إذا تعذر علينا تحليل رسالة)، stream (سيتم حفظ رسالة الاستثناء والرسالة الخام في الأعمدة الافتراضية `_error` و `_raw_message`).

<div id="description">
  ## الوصف
</div>

تُتتبَّع السجلات الواردة تلقائيًا، لذلك لا يُحتسب كل سجل في ملف السجل إلا مرة واحدة فقط.

لا يُعد `SELECT` مفيدًا كثيرًا لقراءة السجلات (إلا لأغراض تصحيح الأخطاء)، لأنه لا يمكن قراءة كل سجل إلا مرة واحدة. والأكثر عملية هو إنشاء تدفقات لحظية باستخدام [العروض المادية](../../../sql-reference/statements/create/view.md). للقيام بذلك:

1. استخدم المحرّك لإنشاء جدول FileLog واعتبره تدفق بيانات.
2. أنشئ جدولًا بالبنية المطلوبة.
3. أنشئ عرضًا ماديًا يحوّل البيانات من المحرّك ويضعها في جدول أُنشئ مسبقًا.

عند ربط `MATERIALIZED VIEW` بالمحرّك، يبدأ في جمع البيانات في الخلفية. يتيح لك ذلك الاستمرار في تلقي السجلات من ملفات السجل وتحويلها إلى التنسيق المطلوب باستخدام `SELECT`.
يمكن لجدول FileLog واحد أن يحتوي على أي عدد تريده من العروض المادية؛ فهي لا تقرأ البيانات من الجدول مباشرةً، بل تتلقى السجلات الجديدة (على شكل كتل)، وبهذه الطريقة يمكنك الكتابة إلى عدة جداول بمستويات تفصيل مختلفة (مع التجميع - aggregation وبدونه).

مثال:

```sql
  CREATE TABLE logs (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = FileLog('user_files/my_app/app.log', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM logs GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

لإيقاف تلقي بيانات التدفقات أو لتغيير منطق التحويل، افصل العرض المادي:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

إذا أردت تغيير الجدول الهدف باستخدام `ALTER`، فنوصي بتعطيل العرض المادي لتجنّب حدوث أي عدم اتساق بين الجدول الهدف والبيانات الواردة من العرض.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_filename` - اسم ملف السجل. نوع البيانات: `LowCardinality(String)`.
* `_offset` - الإزاحة في ملف السجل. نوع البيانات: `UInt64`.

أعمدة افتراضية إضافية عند `handle_error_mode='stream'`:

* `_raw_record` - السجل الخام الذي تعذّر تحليله. نوع البيانات: `Nullable(String)`.
* `_error` - رسالة الاستثناء التي ظهرت عند فشل التحليل. نوع البيانات: `Nullable(String)`.

ملاحظة: لا تُملأ الأعمدة الافتراضية `_raw_record` و `_error` إلا عند حدوث استثناء أثناء التحليل، وتكون دائمًا `NULL` عندما يُحلَّلَت الرسالة بنجاح.