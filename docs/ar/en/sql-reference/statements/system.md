---
description: 'توثيق لعبارات SYSTEM'
sidebar_label: 'SYSTEM'
sidebar_position: 36
slug: /sql-reference/statements/system
title: 'عبارات SYSTEM'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="system-statements">
  # عبارة SYSTEM
</div>

<div id="reload-embedded-dictionaries">
  ## SYSTEM RELOAD EMBEDDED DICTIONARIES
</div>

أعد تحميل جميع [القواميس الداخلية](./create/dictionary/overview.md).
تكون القواميس الداخلية معطّلة افتراضيًا.
يعيد دائمًا `Ok.` بغضّ النظر عن نتيجة تحديث القواميس الداخلية.

<div id="reload-dictionaries">
  ## SYSTEM RELOAD DICTIONARIES
</div>

يعيد الاستعلام `SYSTEM RELOAD DICTIONARIES` تحميل القواميس التي تكون حالتها `LOADED` (راجع العمود `status` في [`system.dictionaries`](/ar/operations/system-tables/dictionaries))، أي القواميس التي سبق تحميلها بنجاح.
بشكل افتراضي، تُحمَّل القواميس عند الحاجة (راجع [dictionaries&#95;lazy&#95;load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load))، لذا فبدلًا من تحميلها تلقائيًا عند بدء التشغيل، تُهيَّأ عند أول وصول إليها باستخدام الدالة [`dictGet`](/ar/sql-reference/functions/ext-dict-functions#dictGet) أو عبر استخدام `SELECT` من الجداول التي تحتوي على `ENGINE = Dictionary`.

**الصياغة**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

<div id="reload-dictionary">
  ## SYSTEM RELOAD DICTIONARY
</div>

يعيد تحميل القاموس `dictionary_name` بالكامل، بصرف النظر عن حالته (LOADED / NOT&#95;LOADED / FAILED).
ويُرجع دائمًا `Ok.` مهما كانت نتيجة تحديث القاموس.

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

يمكن التحقق من حالة القاموس عبر الاستعلام عن جدول `system.dictionaries`.

```sql
SELECT name, status FROM system.dictionaries;
```

<div id="reload-models">
  ## SYSTEM RELOAD MODELS
</div>

:::note
لا تؤدي هذه التعليمة ولا `SYSTEM RELOAD MODEL` إلا إلى إلغاء تحميل نماذج CatBoost من clickhouse-library-bridge. وتحمّل الدالة `catboostEvaluate()`
نموذجًا عند أول وصول إليه إذا لم يكن محمّلًا بعد.
:::

يُلغِي تحميل جميع نماذج CatBoost.

**الصياغة**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

<div id="reload-model">
  ## SYSTEM RELOAD MODEL
</div>

يلغي تحميل نموذج CatBoost من `model_path`.

**الصياغة**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

<div id="reload-functions">
  ## SYSTEM RELOAD FUNCTIONS
</div>

يعيد تحميل جميع [الدوال القابلة للتنفيذ المعرّفة من قبل المستخدم](/ar/sql-reference/functions/udf#executable-user-defined-functions) المسجّلة، أو إحداها، من ملف تهيئة.

**الصياغة**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

<div id="reload-asynchronous-metrics">
  ## SYSTEM RELOAD ASYNCHRONOUS METRICS
</div>

يعيد حساب جميع [المقاييس غير المتزامنة](../../operations/system-tables/asynchronous_metrics.md). وبما أن المقاييس غير المتزامنة تُحدَّث دوريًا استنادًا إلى الإعداد [asynchronous&#95;metrics&#95;update&#95;period&#95;s](../../operations/server-configuration-parameters/settings.md)، فعادةً لا تكون هناك حاجة إلى تحديثها يدويًا باستخدام عبارة SQL هذه.

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

<div id="drop-dns-cache">
  ## SYSTEM CLEAR|DROP ذاكرة DNS المؤقتة
</div>

يمسح ذاكرة DNS المؤقتة الداخلية في ClickHouse. أحيانًا (في إصدارات ClickHouse الأقدم) يلزم استخدام هذا الأمر عند تغيير البنية التحتية (مثل تغيير عنوان IP لخادم ClickHouse آخر أو للخادم الذي تستخدمه Dictionaries).

لإدارة ذاكرة التخزين المؤقت بطريقة أكثر سهولة (تلقائيًا)، راجع المَعلمات `disable_internal_dns_cache` و`dns_cache_max_entries` و`dns_cache_update_period`.

<div id="drop-mark-cache">
  ## SYSTEM CLEAR|DROP MARK CACHE
</div>

يمسح ذاكرة التخزين المؤقت لعلامات البيانات.

<div id="drop-primary-index-cache">
  ## SYSTEM CLEAR|DROP PRIMARY INDEX CACHE
</div>

يمسح ذاكرة التخزين المؤقت للفهرس الأساسي، التي تحتفظ بالمفاتيح الأساسية لجداول [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) في الذاكرة.
ويُحدَّد حجمها بواسطة الإعداد على مستوى الخادم [`primary_index_cache_size`](../../operations/server-configuration-parameters/settings.md#primary_index_cache_size).

<div id="drop-iceberg-metadata-cache">
  ## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE
</div>

يمسح ذاكرة التخزين المؤقت للبيانات الوصفية في Iceberg.

<div id="drop-avro-schema-cache">
  ## SYSTEM CLEAR|DROP AVRO SCHEMA CACHE
</div>

يمسح ذواكر التخزين المؤقت في Confluent Schema Registry لكل `URL` والمستخدمة من قِبل التنسيق `AvroConfluent`. ويؤدي ذلك إلى حذف كلٍّ من ذاكرة التخزين المؤقت لجلب المخططات (id → schema) وذاكرة التخزين المؤقت لتسجيل المخططات (subject + schema → id)، بحيث تعود عمليات القراءة والكتابة اللاحقة إلى خادم السجل. ويكون هذا مفيدًا عندما يكون مخطط قد حُذف أو أُعيدت كتابته من جهة السجل، أو للتحقق من خاصية idempotency الخاصة بالسجل أثناء الاختبارات.

<div id="drop-parquet-metadata-cache">
  ## SYSTEM DROP PARQUET METADATA CACHE
</div>

يمسح ذاكرة التخزين المؤقت لبيانات Parquet الوصفية.

<div id="drop-point-in-polygon-cache">
  ## SYSTEM CLEAR|DROP POINT IN POLYGON CACHE
</div>

يمسح ذاكرة التخزين المؤقت للمضلعات الثابتة المُعالجة مسبقًا التي تستخدمها الدالة [`pointInPolygon`](../functions/geo/coordinates.md#pointinpolygon). ويظل حدّ الحجم المُعدّ (إعداد الخادم `point_in_polygon_cache_size`) دون تغيير، لذا تواصل ذاكرة التخزين المؤقت قبول العناصر بعد ذلك. ولتعطيل ذاكرة التخزين المؤقت بدلًا من ذلك، اضبط `point_in_polygon_cache_size` على `0`.

<div id="drop-text-index-caches">
  ## SYSTEM CLEAR|DROP TEXT INDEX CACHES
</div>

يمسح ذواكر التخزين المؤقت للرموز المميزة والترويسة وقوائم الإحالة الخاصة بالفهرس النصي.

إذا أردت حذف إحدى ذواكر التخزين المؤقت هذه بشكل منفصل، يمكنك تشغيل:

* `SYSTEM CLEAR TEXT INDEX TOKENS CACHE`,
* `SYSTEM CLEAR TEXT INDEX HEADER CACHE`، أو
* `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

<div id="drop-index-mark-cache">
  ## SYSTEM CLEAR|DROP INDEX MARK CACHE
</div>

يمسح ذاكرة التخزين المؤقت لعلامات الفهارس الثانوية (لتخطي البيانات).

<div id="drop-index-uncompressed-cache">
  ## SYSTEM CLEAR|DROP INDEX UNCOMPRESSED CACHE
</div>

يمسح ذاكرة التخزين المؤقت للكتل غير المضغوطة الخاصة بالفهارس الثانوية (لتخطّي البيانات).

<div id="drop-mmap-cache">
  ## SYSTEM CLEAR|DROP MMAP CACHE
</div>

يمسح ذاكرة التخزين المؤقت للملفات المُعيَّنة في الذاكرة.

<div id="drop-page-cache">
  ## SYSTEM CLEAR|DROP PAGE CACHE
</div>

يمسح ذاكرة التخزين المؤقت للصفحات في فضاء المستخدم، وهي ذاكرة ClickHouse المؤقتة الخاصة بها داخل الذاكرة للبيانات المقروءة من طبقة التخزين الأساسية.

<div id="drop-vector-similarity-index-cache">
  ## SYSTEM CLEAR|DROP ذاكرة التخزين المؤقت لفهرس تشابه المتجهات
</div>

يمسح ذاكرة التخزين المؤقت لفهرس تشابه المتجهات.

<div id="drop-connections-cache">
  ## SYSTEM CLEAR|DROP CONNECTIONS CACHE
</div>

يمسح ذاكرة التخزين المؤقت لمجمّعات اتصالات HTTP المستخدمة في الاتصالات الصادرة.

<div id="drop-s3-client-cache">
  ## SYSTEM CLEAR|DROP S3 CLIENT CACHE
</div>

يمسح ذاكرة التخزين المؤقت الخاصة بعملاء S3.

<div id="prewarm-mark-cache">
  ## SYSTEM PREWARM MARK CACHE
</div>

يحمّل علامات الجدول إلى [ذاكرة التخزين المؤقت للعلامات](#drop-mark-cache). كما يحمّل أيضًا علامات الفهارس الثانوية إلى [ذاكرة التخزين المؤقت لعلامات الفهرس](#drop-index-mark-cache).

```sql
SYSTEM PREWARM MARK CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="prewarm-primary-index-cache">
  ## SYSTEM PREWARM PRIMARY INDEX CACHE
</div>

يحمّل الفهارس الأساسية لجدول `MergeTree` إلى [ذاكرة التخزين المؤقت للفهرس الأساسي](#drop-primary-index-cache).

```sql
SYSTEM PREWARM PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="drop-disk-metadata-cache">
  ## SYSTEM CLEAR|DROP DISK METADATA CACHE
</div>

يمسح ذاكرة التخزين المؤقت للبيانات الوصفية الخاصة بالقرص المحدد.

```sql
SYSTEM DROP DISK METADATA CACHE <disk_name>
```

<div id="sync-filesystem-cache">
  ## SYSTEM SYNC FILESYSTEM CACHE
</div>

يُزامِن حالة ذاكرة التخزين المؤقت لنظام الملفات في ClickHouse المخزّنة في الذاكرة مع ملفات cache الموجودة فعليًا على القرص، ويُرجع `cache_name` و`path` و`size` المُنزَّل لكل file segment مخزَّن مؤقتًا. ويمكن استخدام اسم cache اختياري لقصر العملية على cache واحد.

```sql
SYSTEM SYNC FILESYSTEM CACHE ['<cache_name>']
```

<div id="drop-distributed-cache">
  ## SYSTEM CLEAR|DROP DISTRIBUTED CACHE
</div>

:::note
يتوفر `SYSTEM CLEAR|DROP DISTRIBUTED CACHE` فقط في ClickHouse Cloud.
:::

يحذف Distributed Cache. استخدم `CONNECTIONS` لحذف الاتصالات المخزنة مؤقتًا إلى خوادم Distributed Cache فقط، أو مرّر معرّف خادم لاستهداف خادم واحد.

```sql
SYSTEM DROP DISTRIBUTED CACHE [CONNECTIONS | 'server_id']
```

<div id="drop-replica">
  ## SYSTEM DROP REPLICA
</div>

يمكن حذف النُسخ المتماثلة المتعطلة لجداول `ReplicatedMergeTree` باستخدام الصيغة التالية:

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

ستزيل الاستعلامات مسار نسخة متماثلة الخاص بـ `ReplicatedMergeTree` في ZooKeeper. ويكون ذلك مفيدًا عندما تكون نسخة متماثلة معطّلة ولا يمكن إزالة بياناتها الوصفية من ZooKeeper باستخدام `DROP TABLE` لأنه لم يعد هناك مثل هذا الجدول. ولن يؤدي ذلك إلا إلى حذف الـ نسخة متماثلة غير النشطة/القديمة، ولا يمكنه حذف الـ نسخة متماثلة المحلية، لذا استخدم `DROP TABLE` لهذا الغرض. ولا يقوم `DROP REPLICA` بحذف أي جداول، كما لا يزيل أي بيانات أو بيانات وصفية من القرص.

الأول يزيل البيانات الوصفية للـ نسخة متماثلة المسماة `'replica_name'` للجدول `database.table`.
والثاني يفعل الشيء نفسه لجميع الجداول المتماثلة في قاعدة البيانات.
والثالث يفعل الشيء نفسه لجميع الجداول المتماثلة على الخادم المحلي.
والرابع مفيد لإزالة البيانات الوصفية لـ نسخة متماثلة معطّلة عندما تكون جميع الـ نسخ متماثلة الأخرى للجدول قد حُذفت. ويتطلب ذلك تحديد مسار الجدول صراحةً. ويجب أن يكون هو نفسه المسار الذي مُرِّر إلى الوسيطة الأولى لمحرك `ReplicatedMergeTree` عند إنشاء الجدول.

<div id="drop-database-replica">
  ## SYSTEM DROP DATABASE REPLICA
</div>

يمكن حذف النسخ المتماثلة المتوقفة لقواعد بيانات `Replicated` باستخدام الصيغة التالية:

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

مشابه لـ `SYSTEM DROP REPLICA`، لكنه يزيل مسار نسخة قاعدة البيانات المتماثلة `Replicated` من ZooKeeper عندما لا تكون هناك قاعدة بيانات يمكن تنفيذ `DROP DATABASE` عليها. يُرجى ملاحظة أنه لا يزيل النسخ المتماثلة لـ `ReplicatedMergeTree` (لذا قد تحتاج أيضًا إلى `SYSTEM DROP REPLICA`). اسما الشظية والنسخة المتماثلة هما الاسمان اللذان تم تحديدهما في معاملات محرك `Replicated` عند إنشاء قاعدة البيانات. كذلك، يمكن الحصول على هذين الاسمين من العمودين `database_shard_name` و`database_replica_name` في `system.clusters`. إذا كانت عبارة `FROM SHARD` غير موجودة، فيجب أن يكون `replica_name` اسم النسخة المتماثلة الكامل بالتنسيق `shard_name|replica_name`.

<div id="drop-uncompressed-cache">
  ## SYSTEM CLEAR|DROP UNCOMPRESSED CACHE
</div>

يمسح ذاكرة التخزين المؤقت للبيانات غير المضغوطة.
تُفعَّل/تُعطَّل ذاكرة التخزين المؤقت للبيانات غير المضغوطة باستخدام الإعداد على مستوى الاستعلام/المستخدم/الملف التعريفي [`use_uncompressed_cache`](../../operations/settings/settings.md#use_uncompressed_cache).
يمكن ضبط حجمها باستخدام الإعداد على مستوى الخادم [`uncompressed_cache_size`](../../operations/server-configuration-parameters/settings.md#uncompressed_cache_size).

<div id="drop-compiled-expression-cache">
  ## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE
</div>

يمسح ذاكرة التخزين المؤقت للتعبيرات المُترجمة.
يمكن تفعيل ذاكرة التخزين المؤقت للتعبيرات المُترجمة أو تعطيلها باستخدام الإعداد [`compile_expressions`](../../operations/settings/settings.md#compile_expressions) على مستوى الاستعلام/المستخدم/الملف الشخصي.

<div id="drop-query-condition-cache">
  ## SYSTEM CLEAR|DROP QUERY CONDITION CACHE
</div>

يمسح ذاكرة التخزين المؤقت لشروط الاستعلام.

<div id="drop-query-cache">
  ## SYSTEM CLEAR|DROP ذاكرة التخزين المؤقت للاستعلامات
</div>

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
```

يمسح [ذاكرة التخزين المؤقت للاستعلام](../../operations/query-cache.md).
إذا جرى تحديد وسم، فلا تُحذف إلا عناصر ذاكرة التخزين المؤقت للاستعلام التي تحمل الوسم المحدد.

<div id="system-drop-schema-format">
  ## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE
</div>

يمسح ذاكرة التخزين المؤقت للمخططات المُحمَّلة من [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path).

الأهداف المدعومة:

* Protobuf: يزيل تعريفات رسائل Protobuf المستوردة من الذاكرة.
* Files: يحذف ملفات المخطط المخزنة مؤقتًا والمحفوظة محليًا في [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path)، والتي تُنشأ عند ضبط `format_schema_source` على `query`.
  ملاحظة: إذا لم يتم تحديد هدف، فسيتم مسح كلتا ذاكرتَي التخزين المؤقت.

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

<div id="flush-logs">
  ## SYSTEM FLUSH LOGS
</div>

تُفرِّغ رسائل السجل المخزنة مؤقتًا إلى جداول النظام، مثل `system.query&#95;log`. ويكون هذا مفيدًا بشكل أساسي لأغراض استكشاف الأخطاء وإصلاحها، لأن معظم جداول النظام لها فترة تفريغ افتراضية تبلغ 7.5 ثانية.
وسيؤدي هذا أيضًا إلى إنشاء جداول النظام حتى إذا كان صف الرسائل فارغًا.

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

إذا كنت لا تريد تفريغ كل شيء، يمكنك تفريغ سجل واحد أو عدة سجلات بشكل منفصل عبر تمرير إما اسمها أو الجدول الهدف لها:

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

<div id="reload-config">
  ## SYSTEM RELOAD CONFIG
</div>

يعيد تحميل إعدادات ClickHouse. يُستخدم عندما تكون الإعدادات مخزّنة في ZooKeeper. لاحظ أن `SYSTEM RELOAD CONFIG` لا يعيد تحميل إعدادات `USER` المخزّنة في ZooKeeper، بل يعيد تحميل إعدادات `USER` المخزّنة في `users.xml` فقط. لإعادة تحميل جميع إعدادات `USER`، استخدم `SYSTEM RELOAD USERS`

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

<div id="reload-users">
  ## SYSTEM RELOAD USERS
</div>

يعيد تحميل جميع وسائل تخزين الوصول، بما في ذلك: users.xml، ووسيلة تخزين الوصول المحلية على القرص، ووسيلة تخزين الوصول المُكرَّرة (في ZooKeeper).

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

<div id="shutdown">
  ## SYSTEM SHUTDOWN
</div>

<CloudNotSupportedBadge />

يُوقف ClickHouse عادةً (مثل `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

<div id="kill">
  ## SYSTEM KILL
</div>

ينهي عملية ClickHouse (مثل `kill -9 {$ pid_clickhouse-server}`)

<div id="instrument">
  ## SYSTEM INSTRUMENT
</div>

يدير نقاط التتبّع باستخدام ميزة XRay من LLVM، وهي متاحة عند بناء ClickHouse باستخدام `ENABLE_XRAY=1`.
يتيح ذلك تصحيح المشكلات وتحليل الأداء في بيئة الإنتاج من دون تعديل الشيفرة المصدرية وبأقل قدر ممكن من العبء الإضافي.
وعند عدم إضافة أي نقطة تتبّع، تكون كلفة الأداء الإضافية ضئيلة للغاية، لأنه لا يضيف سوى قفزة إضافية إلى عنوان قريب
في بداية هذه الدوال ونهايتها، وذلك للدوال التي يزيد طولها على 200 تعليمة.

<div id="instrument-add">
  ### SYSTEM INSTRUMENT ADD
</div>

يضيف نقطة تتبّع جديدة. يمكن فحص الدوال التي جرى instrument لها في جدول النظام [`system.instrumentation`](../../operations/system-tables/instrumentation.md). ويمكن إضافة أكثر من معالج واحد إلى الدالة نفسها، وسيُنفَّذ كلٌّ منها بالترتيب نفسه الذي أُضيفت به نقاط تتبّع.
يمكن جمع الدوال المطلوب instrument لها من جدول النظام [`system.symbols`](../../operations/system-tables/symbols.md).

توجد ثلاثة أنواع مختلفة من معالجات يمكن إضافتها إلى الدوال:

**البنية**

```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [ARGUMENTS]
```

حيث تكون `FUNCTION` أي دالة أو `substring` من دالة، مثل `QueryMetricLog::startQuery`، ويكون المعالج أحد ما يلي

<div id="instrument-add-log">
  #### LOG
</div>

يطبع النص المُمرَّر كوسيطة وتتبّع المكدس عند `ENTRY` أو `EXIT` للدالة.

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

<div id="instrument-add-sleep">
  #### SLEEP
</div>

ينتظر لعدد ثابت من الثواني، إما عند `ENTRY` أو `EXIT`:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

أو لعدد عشوائي من الثواني بتوزيع منتظم، مع تحديد الحدين الأدنى والأقصى بحيث تفصل بينهما مسافة:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

<div id="instrument-add-profile">
  #### PROFILE
</div>

يقيس الوقت المستغرَق بين `ENTRY` و`EXIT` للدالة.
تُخزَّن نتيجة التنميط في [`system.trace_log`](../../operations/system-tables/trace_log.md)، ويمكن تحويلها
إلى [Chrome Event Trace Format](../../operations/system-tables/trace_log.md#chrome-event-trace-format).

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

<div id="instrument-remove">
  ### SYSTEM INSTRUMENT REMOVE
</div>

يزيل إمّا نقطة تتبّع واحدة باستخدام:

```sql
SYSTEM INSTRUMENT REMOVE ID
```

كلّها باستخدام الكلمة المفتاحية `ALL`:

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

مجموعة من المعرّفات من استعلام فرعي:

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

أو جميع نقاط التتبّع التي تطابق قيمة function&#95;name معيّنة:

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

يمكن جمع معلومات نقطة تتبّع من جدول النظام [`system.instrumentation`](../../operations/system-tables/instrumentation.md).

<div id="managing-distributed-tables">
  ## إدارة الجداول الموزعة
</div>

يمكن لـ ClickHouse إدارة الجداول [الموزعة](../../engines/table-engines/special/distributed.md). عندما يُدرِج المستخدم بيانات في هذه الجداول، ينشئ ClickHouse أولًا قائمة انتظار للبيانات التي يجب إرسالها إلى عُقد العنقود، ثم يرسلها بشكل غير متزامن. يمكنك إدارة معالجة قائمة الانتظار باستخدام الاستعلامات [`STOP DISTRIBUTED SENDS`](#stop-distributed-sends) و[FLUSH DISTRIBUTED](#flush-distributed) و[`START DISTRIBUTED SENDS`](#start-distributed-sends). ويمكنك أيضًا إدخال البيانات الموزعة بشكل متزامن باستخدام الإعداد [`distributed_foreground_insert`](../../operations/settings/settings.md#distributed_foreground_insert).

<div id="stop-distributed-sends">
  ### SYSTEM STOP DISTRIBUTED SENDS
</div>

يوقف توزيع البيانات في الخلفية عند إدراج البيانات في الجداول الموزعة.

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

:::note
إذا كان الخيار [`prefer_localhost_replica`](../../operations/settings/settings.md#prefer_localhost_replica) مفعّلًا (وهو الإعداد الافتراضي)، فسيتم إدراج البيانات في الشارد المحلي على أي حال.
:::

<div id="flush-distributed">
  ### SYSTEM FLUSH DISTRIBUTED
</div>

يُجبر ClickHouse على إرسال البيانات إلى عُقد العنقود بشكل متزامن. وإذا كانت أي من العُقد غير متاحة، يطرح ClickHouse استثناء ويتوقف تنفيذ الاستعلام. يمكنك إعادة محاولة تنفيذ الاستعلام حتى ينجح، وسيحدث ذلك عندما تعود جميع العُقد للعمل.

يمكنك أيضًا تجاوز بعض الإعدادات عبر عبارة `SETTINGS`، وقد يكون ذلك مفيدًا لتفادي بعض القيود المؤقتة، مثل `max_concurrent_queries_for_all_users` أو `max_memory_usage`.

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
تُخزَّن كل كتلة معلّقة على القرص باستخدام الإعدادات الواردة في استعلام INSERT الأصلي، لذا قد تحتاج أحيانًا إلى تعديل هذه الإعدادات.
:::

<div id="start-distributed-sends">
  ### SYSTEM START DISTRIBUTED SENDS
</div>

يُفعِّل توزيع البيانات في الخلفية عند إدراج البيانات في الجداول الموزعة.

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

<div id="stop-listen">
  ### SYSTEM STOP LISTEN
</div>

يُغلق المقبس ويُنهي الاتصالات الحالية مع الخادم على المنفذ المحدد وباستخدام البروتوكول المحدد، بشكلٍ سلس.

ومع ذلك، إذا لم تكن إعدادات البروتوكول المقابلة محددة في تهيئة clickhouse-server، فلن يكون لهذا الأمر أي تأثير.

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

* إذا تم تحديد المُعدِّل `CUSTOM 'protocol'`، فسيُوقَف البروتوكول المخصّص بالاسم المحدد والمُعرَّف في قسم البروتوكولات ضمن إعدادات الخادم.
* إذا تم تحديد المُعدِّل `QUERIES ALL [EXCEPT .. [,..]]`، فسيتم إيقاف جميع البروتوكولات، ما لم يُحدَّد خلاف ذلك باستخدام البند `EXCEPT`.
* إذا تم تحديد المُعدِّل `QUERIES DEFAULT [EXCEPT .. [,..]]`، فسيتم إيقاف جميع البروتوكولات الافتراضية، ما لم يُحدَّد خلاف ذلك باستخدام البند `EXCEPT`.
* إذا تم تحديد المُعدِّل `QUERIES CUSTOM [EXCEPT .. [,..]]`، فسيتم إيقاف جميع البروتوكولات المخصّصة، ما لم يُحدَّد خلاف ذلك باستخدام البند `EXCEPT`.

<div id="start-listen">
  ### SYSTEM START LISTEN
</div>

يسمح بإنشاء اتصالات جديدة عبر البروتوكولات المحددة.

ومع ذلك، إذا لم يكن الخادم على المنفذ والبروتوكول المحددين قد أُوقِف باستخدام الأمر SYSTEM STOP LISTEN، فلن يكون لهذا الأمر أي تأثير.

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

<div id="managing-mergetree-tables">
  ## إدارة جداول MergeTree
</div>

يمكن لـ ClickHouse إدارة العمليات الخلفية في جداول [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

<div id="stop-merges">
  ### SYSTEM STOP MERGES
</div>

<CloudNotSupportedBadge />

يتيح إيقاف عمليات الدمج في الخلفية للجداول ضمن عائلة MergeTree:

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
سيؤدي `DETACH / ATTACH` للجدول إلى بدء عمليات الدمج في الخلفية لهذا الجدول، حتى إذا كانت عمليات الدمج قد أُوقفت سابقًا لجميع جداول MergeTree.
:::

<div id="start-merges">
  ### SYSTEM START MERGES
</div>

<CloudNotSupportedBadge />

يتيح بدء عمليات الدمج في الخلفية للجداول التابعة لعائلة MergeTree:

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

<div id="stop-ttl-merges">
  ### SYSTEM STOP TTL MERGES
</div>

<CloudNotSupportedBadge />

يتيح إيقاف الحذف التلقائي للبيانات القديمة في الخلفية وفقًا لـ [تعبير TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) للجداول ضمن عائلة MergeTree:
يعيد `Ok.` حتى إذا كان الجدول غير موجود أو لم يكن يستخدم محرك MergeTree. ويعيد خطأ إذا كانت قاعدة البيانات غير موجودة:

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-ttl-merges">
  ### SYSTEM START TTL MERGES
</div>

<CloudNotSupportedBadge />

يوفّر إمكانية بدء حذف البيانات القديمة في الخلفية وفقًا لـ [تعبير TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) للجداول من عائلة MergeTree:
يعيد `Ok.` حتى إذا لم يكن الجدول موجودًا. ويعيد خطأ إذا لم تكن قاعدة البيانات موجودة:

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="stop-moves">
  ### SYSTEM STOP MOVES
</div>

يوفّر إمكانية إيقاف عمليات نقل البيانات في الخلفية وفقًا لـ [تعبير TTL للجدول مع العبارة TO VOLUME أو TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) للجداول في عائلة MergeTree:
يعيد `Ok.` حتى إذا كان الجدول غير موجود. ويُرجع خطأً عندما لا تكون قاعدة البيانات موجودة:

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-moves">
  ### SYSTEM START MOVES
</div>

يوفّر إمكانية بدء عمليات نقل البيانات في الخلفية وفقًا لـ [تعبير TTL للجدول مع العبارتين TO VOLUME و TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) للجداول ضمن عائلة MergeTree:
يُرجع `Ok.` حتى إذا لم يكن الجدول موجودًا. ويُرجع خطأً إذا لم تكن قاعدة البيانات موجودة:

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="query_language-system-unfreeze">
  ### SYSTEM UNFREEZE
</div>

يحذف نسخة احتياطية مجمّدة بالاسم المحدد من جميع الأقراص. اطّلع على مزيد من المعلومات حول إلغاء تجميد الأجزاء بشكل منفصل في [ALTER TABLE table&#95;name UNFREEZE WITH NAME ](/ar/sql-reference/statements/alter/partition#unfreeze-partition)

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

<div id="wait-loading-parts">
  ### SYSTEM WAIT LOADING PARTS
</div>

انتظر حتى يكتمل تحميل جميع أجزاء البيانات الخاصة بالجدول التي تُحمَّل بشكل غير متزامن (أجزاء البيانات القديمة).

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

<div id="managing-replicatedmergetree-tables">
  ## إدارة جداول ReplicatedMergeTree
</div>

يمكن لـ ClickHouse إدارة العمليات الخلفية المرتبطة بالنسخ المتماثل في جداول [ReplicatedMergeTree](/ar/engines/table-engines/mergetree-family/replication).

<div id="stop-fetches">
  ### SYSTEM STOP FETCHES
</div>

<CloudNotSupportedBadge />

يتيح إيقاف عمليات الجلب الخلفية للأجزاء المُدرجة للجداول ضمن عائلة `ReplicatedMergeTree`:
ويُرجع دائمًا `Ok.` بغض النظر عن محرك الجدول، وحتى إذا لم يكن الجدول أو قاعدة البيانات موجودين.

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-fetches">
  ### SYSTEM START FETCHES
</div>

<CloudNotSupportedBadge />

يوفّر إمكانية بدء عمليات الجلب في الخلفية للأجزاء المُدخلة للجداول ضمن عائلة `ReplicatedMergeTree`:
ويُرجع دائمًا `Ok.` بغضّ النظر عن محرك الجدول، حتى إذا لم يكن الجدول أو قاعدة البيانات موجودًا.

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replicated-sends">
  ### SYSTEM STOP REPLICATED SENDS
</div>

يوفّر إمكانية إيقاف عمليات الإرسال في الخلفية إلى النُسخ المتماثلة الأخرى في العنقود للأجزاء الجديدة المُدرجة في الجداول من عائلة `ReplicatedMergeTree`:

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replicated-sends">
  ### SYSTEM START REPLICATED SENDS
</div>

يتيح بدء عمليات الإرسال في الخلفية إلى النسخ المتماثلة الأخرى في العنقود للأجزاء المُدرجة حديثًا في الجداول من عائلة `ReplicatedMergeTree`:

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replication-queues">
  ### SYSTEM STOP REPLICATION QUEUES
</div>

يوفّر إمكانية إيقاف مهام الجلب التي تعمل في الخلفية ضمن قوائم انتظار النسخ المتماثل المخزّنة في Zookeeper، وذلك للجداول من عائلة `ReplicatedMergeTree`. أنواع المهام الخلفية الممكنة: عمليات الدمج، وعمليات الجلب، وعمليات التعديل، وعبارات DDL مع عبارة ON CLUSTER:

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replication-queues">
  ### SYSTEM START REPLICATION QUEUES
</div>

يتيح بدء مهام الجلب في الخلفية من قوائم انتظار النسخ المتماثل المخزّنة في Zookeeper للجداول ضمن عائلة `ReplicatedMergeTree`. أنواع مهام الخلفية الممكنة هي: عمليات الدمج، وعمليات الجلب، وعمليات mutation، وعبارات DDL التي تتضمن بند ON CLUSTER:

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-pulling-replication-log">
  ### SYSTEM STOP PULLING REPLICATION LOG
</div>

يوقف تحميل السجلات الجديدة من replication log إلى قائمة انتظار replication في جدول `ReplicatedMergeTree`.

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-pulling-replication-log">
  ### SYSTEM START PULLING REPLICATION LOG
</div>

يلغي الأمر `SYSTEM STOP PULLING REPLICATION LOG`.

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="sync-replica">
  ### SYSTEM SYNC REPLICA
</div>

انتظر حتى تتم مزامنة جدول `ReplicatedMergeTree` مع النسخ المتماثلة الأخرى في مجموعة، على ألا تتجاوز المدة `receive_timeout` ثانية.

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

بعد تشغيل هذه التعليمة، يقوم `[db.]replicated_merge_tree_family_table_name` بجلب الأوامر من السجل المتماثل المشترك إلى قائمة انتظار النسخ المتماثل الخاصة به، ثم ينتظر الاستعلام إلى أن تعالج النسخة المتماثلة جميع الأوامر التي جرى جلبها. تُدعم المعدِّلات التالية:

* باستخدام `IF EXISTS` (متاح منذ 25.6)، لن يُصدر الاستعلام خطأ إذا لم يكن الجدول موجودًا. ويكون ذلك مفيدًا عند إضافة نسخة متماثلة جديدة إلى عنقود، حين تكون بالفعل جزءًا من تهيئة العنقود لكنها لا تزال في طور إنشاء الجدول ومزامنته.
* إذا تم تحديد المعدِّل `STRICT`، فسينتظر الاستعلام حتى تصبح قائمة انتظار النسخ المتماثل فارغة. وقد لا تنجح صيغة `STRICT` أبدًا إذا كانت إدخالات جديدة تظهر باستمرار في قائمة انتظار النسخ المتماثل.
* إذا تم تحديد المعدِّل `LIGHTWEIGHT`، فسينتظر الاستعلام فقط حتى تتم معالجة إدخالات `GET_PART` و`ATTACH_PART` و`DROP_RANGE` و`REPLACE_RANGE` و`DROP_PART`.
  بالإضافة إلى ذلك، يدعم المعدِّل LIGHTWEIGHT بند FROM &#39;srcReplicas&#39; اختياريًا، حيث إن &#39;srcReplicas&#39; هي قائمة مفصولة بفواصل تضم أسماء النسخ المتماثلة المصدر. ويتيح هذا الامتداد مزامنة أكثر استهدافًا من خلال التركيز فقط على مهام النسخ المتماثل القادمة من النسخ المتماثلة المصدر المحددة.
* إذا تم تحديد المعدِّل `PULL`، فسيسحب الاستعلام إدخالات جديدة إلى قائمة انتظار النسخ المتماثل من ZooKeeper، لكنه لن ينتظر معالجة أي شيء.

<div id="sync-database-replica">
  ### SYNC DATABASE REPLICA
</div>

ينتظر إلى أن تُطبّق [قاعدة البيانات المكرّرة](/ar/engines/database-engines/replicated) المحددة جميع تغييرات المخطط من قائمة انتظار DDL الخاصة بتلك القاعدة.

**الصيغة**

```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

<div id="restart-replica">
  ### SYSTEM RESTART REPLICA
</div>

يوفّر إمكانية إعادة تهيئة حالة جلسة ZooKeeper لجدول `ReplicatedMergeTree`، ويقارن الحالة الحالية بما هو موجود في ZooKeeper بوصفه المرجع المعتمد، ويضيف مهامًا إلى قائمة انتظار ZooKeeper عند الحاجة.
تتم تهيئة قائمة انتظار النسخ المتماثل استنادًا إلى بيانات ZooKeeper بالطريقة نفسها المتبعة في عبارة `ATTACH TABLE`. ولمدة قصيرة، لن يكون الجدول متاحًا لأي عمليات.

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

<div id="restore-replica">
  ### SYSTEM RESTORE REPLICA
</div>

يستعيد نسخة متماثلة إذا كانت البيانات [قد] تكون موجودة، لكن بيانات ZooKeeper الوصفية مفقودة.

يعمل فقط على جداول `ReplicatedMergeTree` التي تكون في وضع القراءة فقط.

يمكن تنفيذ هذا الاستعلام بعد:

* فقدان جذر ZooKeeper `/`.
* فقدان مسار النسخ المتماثلة `/replicas`.
* فقدان مسار نسخة متماثلة فردية `/replicas/replica_name/`.

تُلحق النسخة المتماثلة الأجزاء الموجودة محليًا، وترسل معلومات عنها إلى ZooKeeper.
ولا يُعاد جلب الأجزاء الموجودة على النسخة المتماثلة قبل فقدان البيانات الوصفية من النسخ الأخرى إذا لم تكن outdated (لذا فإن استعادة النسخة المتماثلة لا تعني إعادة تنزيل جميع البيانات عبر الشبكة).

:::note
تُنقل الأجزاء بجميع حالاتها إلى المجلد `detached/`. وتُلحَق الأجزاء التي كانت نشطة قبل فقدان البيانات (committed).
:::

<div id="restore-database-replica">
  ### SYSTEM RESTORE DATABASE REPLICA
</div>

يستعيد نسخة متماثلة إذا كانت البيانات موجودة [ربما] ولكن بيانات ZooKeeper الوصفية مفقودة.

**البنية**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**مثال**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

**الصياغة**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

صيغة بديلة:

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**مثال**

إنشاء جدول على عدة خوادم. بعد فقدان البيانات الوصفية الخاصة بالنسخة المتماثلة في ZooKeeper، سيُرفَق الجدول بوضع القراءة فقط بسبب فقدان البيانات الوصفية. يجب تنفيذ الاستعلام الأخير على كل نسخة متماثلة.

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

طريقة أخرى:

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

<div id="restart-replicas">
  ### SYSTEM RESTART REPLICAS
</div>

يوفّر إمكانية إعادة تهيئة حالة جلسات Zookeeper لجميع جداول `ReplicatedMergeTree`، ويقارن الحالة الحالية مع Zookeeper بوصفه مصدر الحقيقة، ويضيف مهام إلى قائمة انتظار في Zookeeper عند الحاجة

<div id="drop-filesystem-cache">
  ### SYSTEM CLEAR|DROP FILESYSTEM CACHE
</div>

يسمح بحذف ذاكرة التخزين المؤقت لنظام الملفات.

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

<div id="sync-file-cache">
  ### SYSTEM SYNC FILE CACHE
</div>

:::note
هذا الأمر مُكلف جدًا وقد يكون عُرضة لإساءة الاستخدام.
:::

سينفّذ استدعاء النظام sync.

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

<div id="load-primary-key">
  ### SYSTEM LOAD PRIMARY KEY
</div>

حمّل المفاتيح الأساسية للجدول المعني أو لجميع الجداول.

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

<div id="unload-primary-key">
  ### SYSTEM UNLOAD PRIMARY KEY
</div>

ألغِ تحميل المفاتيح الأساسية للجدول المحدد أو لجميع الجداول.

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

<div id="managing-refreshable-materialized-views">
  ## إدارة العروض المادية القابلة للتحديث
</div>

أوامر للتحكم في المهام التي تعمل في الخلفية وتنفذها [العروض المادية القابلة للتحديث](../../sql-reference/statements/create/view.md#refreshable-materialized-view)

راقب [`system.view_refreshes`](../../operations/system-tables/view_refreshes.md) عند استخدامها.

<div id="stop-view-stop-views">
  ### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS
</div>

يعطّل التحديث الدوري للعرض المحدد أو لجميع العروض القابلة للتحديث. وإذا كان هناك تحديث جارٍ، فسيُلغى أيضًا.

إذا كان العرض موجودًا في قاعدة بيانات من نوع Replicated أو Shared، فإن `STOP VIEW` يؤثر فقط في النسخة المتماثلة الحالية، بينما يؤثر `STOP REPLICATED VIEW` في جميع النسخ المتماثلة.

:::note
لا تستمر حالة الإيقاف بعد إعادة تشغيل الخادم. بعد إعادة التشغيل، ستستأنف العروض جداول التحديث المُعدّة لها.
في قواعد البيانات من نوع Replicated أو Shared، فإن `SYSTEM STOP VIEW` يؤثر فقط في النسخة المتماثلة الحالية. استخدم `SYSTEM STOP REPLICATED VIEW` لإيقاف عمليات التحديث على جميع النسخ المتماثلة.
:::

```sql
SYSTEM STOP VIEW [db.]name
```

```sql
SYSTEM STOP VIEWS
```

<div id="start-view-start-views">
  ### SYSTEM START [REPLICATED] VIEW, START VIEWS
</div>

يُفعِّل التحديث الدوري للعرض المحدد أو لجميع العروض القابلة للتحديث. ولا يؤدي ذلك إلى تشغيل أي تحديث فوري.

إذا كان العرض موجودًا في قاعدة بيانات Replicated أو Shared، فإن `START VIEW` يلغي أثر `STOP VIEW`، كما أن `START REPLICATED VIEW` يلغي أثر `STOP REPLICATED VIEW`. ويلغي `START VIEW` أيضًا أثر `PAUSE VIEW`.

```sql
SYSTEM START VIEW [db.]name
```

```sql
SYSTEM START VIEWS
```

<div id="pause-view-pause-views">
  ### SYSTEM PAUSE VIEW, PAUSE VIEWS
</div>

أوقِف مؤقتًا التحديث الدوري للعرض المحدد أو لجميع العروض القابلة للتحديث.
وعلى خلاف `SYSTEM STOP VIEW`، لا يقطع `SYSTEM PAUSE VIEW` عملية تحديث بدأت بالفعل: يُسمح لعملية التحديث الجارية بأن تكتمل، ولا تُمنع إلا عمليات التحديث اللاحقة.

يمكن التراجع عن ذلك باستخدام `SYSTEM START VIEW` أو `SYSTEM START VIEWS`.

:::note
لا تستمر حالة الإيقاف المؤقت بعد إعادة تشغيل الخادم. بعد إعادة التشغيل، ستستأنف العروض جداول التحديث المهيأة لها.
في قواعد البيانات Replicated أو Shared، يؤثر `SYSTEM PAUSE VIEW` فقط في النسخة المتماثلة الحالية.
:::

```sql
SYSTEM PAUSE VIEW [db.]name
```

```sql
SYSTEM PAUSE VIEWS
```

<div id="refresh-view">
  ### SYSTEM REFRESH VIEW
</div>

نفِّذ تحديثًا فوريًا لعرضٍ معيّن خارج الموعد المجدول.

```sql
SYSTEM REFRESH VIEW [db.]name
```

<div id="wait-view">
  ### SYSTEM WAIT VIEW
</div>

ينتظر حتى يكتمل التحديث الجاري. إذا لم يكن هناك تحديث جارٍ، فسيعود فورًا. وإذا فشلت أحدث محاولة تحديث، فسيُبلّغ عن خطأ.

يمكن استخدامه مباشرةً بعد إنشاء عرض مادي قابل للتحديث جديد (من دون الكلمة المفتاحية EMPTY) لانتظار اكتمال التحديث الأولي.

إذا كان العرض في قاعدة بيانات Replicated أو Shared، وكان التحديث جاريًا على نسخة متماثلة أخرى، فإنه ينتظر حتى يكتمل ذلك التحديث.

```sql
SYSTEM WAIT VIEW [db.]name
```

<div id="cancel-view">
  ### SYSTEM CANCEL VIEW
</div>

إذا كانت هناك عملية تحديث جارية للعرض المحدد على النسخة المتماثلة الحالية، فقُم بمقاطعتها وإلغائها. وإلا، فلا تفعل شيئًا.

```sql
SYSTEM CANCEL VIEW [db.]name
```

<div id="flush-object-storage-queue">
  ## SYSTEM FLUSH OBJECT STORAGE QUEUE
</div>

ينتظر إلى أن تتم معالجة الملف المحدد أو أن يفشل فشلًا نهائيًا في جدول [S3Queue](../../engines/table-engines/integrations/s3queue.md) أو [AzureQueue](../../engines/table-engines/integrations/azure-queue.md) المحدد. ويعود فورًا إذا كانت معالجة الملف قد اكتملت بالفعل. ويُرجع خطأً إذا كان الملف قد فشل نهائيًا (بعد استنفاد جميع محاولات إعادة المحاولة).

```sql
SYSTEM FLUSH OBJECT STORAGE QUEUE [db.]table_name PATH 'path'
```