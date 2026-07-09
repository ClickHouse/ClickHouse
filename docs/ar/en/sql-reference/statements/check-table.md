---
description: 'توثيق CHECK TABLE'
sidebar_label: 'CHECK TABLE'
sidebar_position: 41
slug: /sql-reference/statements/check-table
title: 'تعليمة CHECK TABLE'
doc_type: 'reference'
---

يُستخدم الاستعلام `CHECK TABLE` في ClickHouse لإجراء فحص تحقق على جدول محدد أو على أقسامه. ويضمن سلامة البيانات من خلال التحقق من checksums وبُنى البيانات الداخلية الأخرى.

وعلى وجه الخصوص، يقارن أحجام الملفات الفعلية بالقيم المتوقعة المخزنة على الخادم. وإذا لم تتطابق أحجام الملفات مع القيم المخزنة، فهذا يعني أن البيانات تالفة. وقد يحدث ذلك، على سبيل المثال، بسبب تعطل النظام أثناء تنفيذ الاستعلام.

:::warning
قد يقرأ الاستعلام `CHECK TABLE` جميع البيانات الموجودة في الجدول ويشغل بعض الموارد، مما يجعله كثيف الاستهلاك للموارد.
ضع في اعتبارك التأثير المحتمل على الأداء واستخدام الموارد قبل تنفيذ هذا الاستعلام.
لن يؤدي هذا الاستعلام إلى تحسين أداء النظام، ويجب ألا تنفذه إذا لم تكن متأكدًا مما تفعله.
:::

<div id="syntax">
  ## الصيغة
</div>

الصيغة الأساسية للاستعلام كما يلي:

```sql
CHECK TABLE table_name [PARTITION partition_expression | PART part_name] [FORMAT format] [SETTINGS check_query_single_value_result = (0|1) [, other_settings]]
```

* `table_name`: يحدّد اسم الجدول الذي تريد التحقق منه.
* `partition_expression`: (اختياري) إذا كنت تريد التحقق من قسم محدد في الجدول، يمكنك استخدام هذا التعبير لتحديد الـ قسم.
* `part_name`: (اختياري) إذا كنت تريد التحقق من جزء بيانات محدد في الجدول، يمكنك إضافة قيمة حرفية نصية لتحديد اسم الـ part.
* `FORMAT format`: (اختياري) يتيح لك تحديد تنسيق إخراج النتيجة.
* `SETTINGS`: (اختياري) يتيح لك تحديد إعدادات إضافية.
  * (اختياري): [check&#95;query&#95;single&#95;value&#95;result](../../operations/settings/settings#check_query_single_value_result): يتحكم هذا الإعداد في ما إذا كان الإخراج مفصلًا (`0`) أو مُلخّصًا (`1`).
  * يمكن أيضًا تطبيق إعدادات أخرى. إذا لم تكن بحاجة إلى ترتيب حتمي للنتائج، فيمكنك ضبط max&#95;threads على قيمة أكبر من واحد لتسريع الاستعلام.

تعتمد استجابة الاستعلام على قيمة الإعداد `check_query_single_value_result`.
في حالة `check_query_single_value_result = 1`، لا تتم إعادة سوى العمود `result` مع صف واحد. وتكون القيمة في هذا الصف `1` إذا نجح التحقق من السلامة و`0` إذا كانت البيانات تالفة.

عند استخدام `check_query_single_value_result = 0`، يعيد الاستعلام الأعمدة التالية:

* `part_path`: يشير إلى مسار جزء بيانات أو اسم الملف.
  * `is_passed`: يعيد 1 إذا نجح التحقق من هذا الجزء، و0 بخلاف ذلك.
  * `message`: أي رسائل إضافية متعلقة بالتحقق، مثل رسائل الخطأ أو رسائل النجاح.

يدعم استعلام `CHECK TABLE` محركات الجداول التالية:

* [Log](../../engines/table-engines/log-family/log.md)
* [TinyLog](../../engines/table-engines/log-family/tinylog.md)
* [StripeLog](../../engines/table-engines/log-family/stripelog.md)
* [عائلة MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)

يؤدي تنفيذ هذا الاستعلام على جداول تستخدم محركات جداول أخرى إلى ظهور الاستثناء `NOT_IMPLEMENTED`.

لا توفر المحركات من عائلة `*Log` استردادًا تلقائيًا للبيانات عند حدوث فشل. استخدم استعلام `CHECK TABLE` لاكتشاف فقدان البيانات في الوقت المناسب.

<div id="examples">
  ## أمثلة
</div>

يعرض استعلام `CHECK TABLE`، بشكل افتراضي، الحالة العامة لفحص الجدول:

```sql title="Query"
CHECK TABLE test_table;
```

```text title="Response"
┌─result─┐
│      1 │
└────────┘
```

إذا كنت تريد الاطّلاع على حالة الفحص لكل جزء بيانات على حدة، فيمكنك استخدام الإعداد `check_query_single_value_result`.

كذلك، لفحص قسم محدد من الجدول، يمكنك استخدام الكلمة المفتاحية `PARTITION`.

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
│ 201003_3_3_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

وبالمثل، يمكنك فحص جزء معيّن من الجدول باستخدام الكلمة المفتاحية `PART`.

```sql title="Query"
CHECK TABLE t0 PART '201003_7_7_0'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

لاحظ أنه عندما لا يكون الجزء موجودًا، يُرجع الاستعلام خطأً:

```sql title="Query"
CHECK TABLE t0 PART '201003_111_222_0'
```

```text title="Response"
DB::Exception: No such data part '201003_111_222_0' to check in table 'default.t0'. (NO_SUCH_DATA_PART)
```

<div id="receiving-a-corrupted-result">
  ### ظهور نتيجة &#39;Corrupted&#39;
</div>

:::warning
إخلاء مسؤولية: الإجراء الموضّح هنا، بما في ذلك التلاعب بالملفات يدويًا أو حذفها مباشرةً من دليل البيانات، مخصّص فقط للبيئات التجريبية أو بيئات التطوير. **لا** تحاول تنفيذ ذلك على خادم الإنتاج، لأن ذلك قد يؤدي إلى فقدان البيانات أو إلى عواقب أخرى غير مقصودة.
:::

أزل ملف checksum الحالي:

```bash
rm /var/lib/clickhouse-server/data/default/t0/201003_3_3_0/checksums.txt
```

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message──────────────────────────────────┐
│ 201003_7_7_0 │         1 │                                          │
│ 201003_3_3_0 │         1 │ Checksums recounted and written to disk. │
└──────────────┴───────────┴──────────────────────────────────────────┘
```

إذا كان الملف checksums.txt مفقودًا، فيمكن استعادته. سيُعاد حسابه وتُعاد كتابته أثناء تنفيذ الأمر `CHECK TABLE` للقسم المحدد، وستظل الحالة مُبلّغًا عنها على أنها &#39;is&#95;passed = 1&#39;.

يمكنك التحقق من جميع جداول `(Replicated)MergeTree` الموجودة دفعةً واحدة باستخدام الاستعلام `CHECK ALL TABLES`.

```sql
CHECK ALL TABLES
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text
┌─database─┬─table────┬─part_path───┬─is_passed─┬─message─┐
│ default  │ t2       │ all_1_95_3  │         1 │         │
│ db1      │ table_01 │ all_39_39_0 │         1 │         │
│ default  │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ table_01 │ all_1_6_1   │         1 │         │
│ default  │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ table_01 │ all_7_38_2  │         1 │         │
│ db1      │ t1       │ all_7_38_2  │         1 │         │
│ default  │ t1       │ all_7_38_2  │         1 │         │
└──────────┴──────────┴─────────────┴───────────┴─────────┘
```

<div id="if-the-data-is-corrupted">
  ## إذا كانت البيانات تالفة
</div>

إذا كان الجدول تالفًا، يمكنك نسخ البيانات غير التالفة إلى جدول آخر. للقيام بذلك:

1. أنشئ جدولًا جديدًا له البنية نفسها الخاصة بالجدول التالف. للقيام بذلك، نفّذ الاستعلام `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2. اضبط قيمة `max_threads` على 1 لمعالجة الاستعلام التالي ضمن خيط تنفيذ واحد. للقيام بذلك، شغّل الاستعلام `SET max_threads = 1`.
3. نفّذ الاستعلام `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. ينسخ هذا الأمر البيانات غير التالفة من الجدول التالف إلى جدول آخر. لن تُنسخ إلا البيانات التي تسبق الجزء التالف.
4. أعد تشغيل `clickhouse-client` لإعادة ضبط قيمة `max_threads`.