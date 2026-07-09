---
description: 'يوفّر هذا المحرك تكاملًا للقراءة فقط مع جداول Apache Iceberg الحالية
  في Amazon S3 وAzure وHDFS والجداول المخزّنة محليًا.'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'محرك جدول Iceberg'
doc_type: 'مرجع'
---

:::warning
نوصي باستخدام [دالة الجدول Iceberg](/ar/sql-reference/table-functions/iceberg.md) للعمل مع بيانات Iceberg في ClickHouse. إذ توفّر دالة الجدول Iceberg حاليًا وظائف كافية، مع واجهة جزئية للقراءة فقط لجداول Iceberg.

يتوفر Iceberg Table Engine، لكنه قد يفرض بعض القيود. لم يُصمَّم ClickHouse في الأصل لدعم الجداول ذات المخططات التي تتغيّر خارجيًا، مما قد يؤثر في وظائف Iceberg Table Engine. ونتيجة لذلك، قد لا تتوفر بعض الميزات التي تعمل مع الجداول العادية، أو قد لا تعمل على نحو صحيح، خاصةً عند استخدام المحلّل القديم.

للحصول على أفضل توافق، نقترح استخدام دالة الجدول Iceberg بينما نواصل تحسين دعم Iceberg Table Engine.
:::

يوفّر هذا المحرك تكاملًا للقراءة فقط مع جداول Apache [Iceberg](https://iceberg.apache.org/) الحالية في Amazon S3 وAzure وHDFS والجداول المخزّنة محليًا.

<div id="create-table">
  ## إنشاء جدول
</div>

لاحظ أن جدول Iceberg يجب أن يكون موجودًا مسبقًا في وحدة التخزين، إذ لا يدعم هذا الأمر تمرير معاملات DDL لإنشاء جدول جديد.

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression], [,extra_credentials])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## وسيطات المحرك
</div>

يتطابق وصف الوسيطات مع وصف الوسيطات في المحركات `S3` و`AzureBlobStorage` و`HDFS` و`File` المقابلة.
يشير `format` إلى تنسيق ملفات البيانات في جدول Iceberg.

بالنسبة إلى `IcebergS3`، يمكن استخدام المعلَمة الاختيارية `extra_credentials` لتمرير `role_arn` من أجل الوصول المستند إلى الأدوار في ClickHouse Cloud. راجع [تأمين S3](/ar/cloud/data-sources/secure-s3) للاطلاع على خطوات التهيئة.

يمكن تحديد معلمات المحرك باستخدام [المجموعات المسماة](../../../operations/named-collections.md)

<div id="example">
  ### مثال
</div>

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

باستخدام المجموعات المُسمّاة:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

<div id="aliases">
  ## الأسماء البديلة
</div>

يكتشف محرك الجدول `Iceberg` تلقائيًا الواجهة الخلفية للتخزين من إعداد `disk`، ثم يوجّه التنفيذ إلى `IcebergS3` أو `IcebergAzure` أو `IcebergLocal` وفقًا لذلك. وعند عدم تحديد `disk`، يُستخدم تنفيذ `IcebergS3` افتراضيًا.

<div id="data-types">
  ## أنواع البيانات
</div>

يوضح الجدول التالي كيفية تعيين أنواع بيانات Iceberg إلى أنواع بيانات ClickHouse أثناء استنتاج المخطط (لأغراض القراءة).

<div id="primitive-types">
  ### الأنواع الأولية
</div>

| نوع Iceberg        | نوع ClickHouse         | ملاحظات                                                   |
| ------------------ | ---------------------- | --------------------------------------------------------- |
| `boolean`          | `Bool`                 |                                                           |
| `int`              | `Int32`                |                                                           |
| `long`, `bigint`   | `Int64`                |                                                           |
| `float`            | `Float32`              |                                                           |
| `double`           | `Float64`              |                                                           |
| `date`             | `Date32`               |                                                           |
| `time`             | `Int64`                | ميكروثوانٍ منذ منتصف الليل                                |
| `timestamp`        | `DateTime64(6)`        | ميكروثوانٍ، من دون منطقة زمنية                            |
| `timestamptz`      | `DateTime64(6, 'UTC')` | ميكروثوانٍ، بتوقيت UTC                                    |
| `timestamp_ns`     | `DateTime64(9)`        | نانوثوانٍ، من دون منطقة زمنية (ابتداءً من Iceberg v3 فقط) |
| `timestamptz_ns`   | `DateTime64(9, 'UTC')` | نانوثوانٍ، بتوقيت UTC (ابتداءً من Iceberg v3 فقط)         |
| `string`, `binary` | `String`               |                                                           |
| `uuid`             | `UUID`                 |                                                           |
| `fixed(N)`         | `FixedString(N)`       |                                                           |
| `decimal(P, S)`    | `Decimal(P, S)`        |                                                           |

<div id="complex-types">
  ### الأنواع المركبة
</div>

| نوع Iceberg | نوع ClickHouse |
| ----------- | -------------- |
| `list`      | `Array`        |
| `map`       | `Map`          |
| `struct`    | `Tuple`        |

<div id="schema-evolution">
  ## تطور المخطط
</div>

يدعم ClickHouse قراءة جداول Iceberg التي تطور مخططها بمرور الوقت. ويشمل ذلك الجداول التي أُضيفت إليها أعمدة أو أُزيلت منها أو أُعيد ترتيبها، وكذلك الأعمدة التي تغيّرت من required إلى Nullable. بالإضافة إلى ذلك، تُدعَم تحويلات الأنواع التالية:

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) where P&#39; &gt; P.

حاليًا، لا يمكن تغيير البُنى المتداخلة أو أنواع العناصر داخل المصفوفات والخرائط.

لقراءة جدول تغيّر مخططه بعد إنشائه باستخدام استدلال المخطط الديناميكي، عيّن allow&#95;dynamic&#95;metadata&#95;for&#95;data&#95;lakes = true عند إنشاء الجدول.

<div id="partition-pruning">
  ## استبعاد الأقسام
</div>

يدعم ClickHouse استبعاد الأقسام في استعلامات SELECT على جداول Iceberg، مما يساعد على تحسين أداء الاستعلامات عبر تجاوز ملفات البيانات غير ذات الصلة. لتمكين استبعاد الأقسام، اضبط `use_iceberg_partition_pruning = 1`. لمزيد من المعلومات حول استبعاد أقسام Iceberg، راجع https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## السفر عبر الزمن
</div>

يدعم ClickHouse ميزة السفر عبر الزمن في جداول Iceberg، مما يتيح لك الاستعلام عن البيانات التاريخية باستخدام طابع زمني محدد أو معرّف لقطة.

<div id="deleted-rows">
  ## معالجة الجداول ذات الصفوف المحذوفة
</div>

يدعم ClickHouse قراءة جداول Iceberg التي تستخدم طرق الحذف التالية:

* [الحذف بحسب الموضع](https://iceberg.apache.org/spec/#position-delete-files)
* [الحذف بحسب المساواة](https://iceberg.apache.org/spec/#equality-delete-files) (مدعوم بدءًا من الإصدار 25.8+)

طريقة الحذف التالية **غير مدعومة**:

* [متجهات الحذف](https://iceberg.apache.org/spec/#deletion-vectors) (أُدخلت في v3)

<div id="basic-usage">
  ### الاستخدام الأساسي
</div>

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
```

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
```

ملاحظة: لا يمكنك تحديد المعاملين `iceberg_timestamp_ms` و`iceberg_snapshot_id` معًا في الاستعلام نفسه.

<div id="important-considerations">
  ### اعتبارات مهمة
</div>

* يتم عادةً إنشاء **اللقطات** عندما:
  * تُكتب بيانات جديدة إلى الجدول
  * تُجرى عملية **compaction** للبيانات

* **لا تؤدي تغييرات المخطط عادةً إلى إنشاء لقطات** - وهذا يؤدي إلى سلوكيات مهمة عند استخدام السفر عبر الزمن مع الجداول التي شهدت تطورًا في المخطط.

<div id="example-scenarios">
  ### سيناريوهات توضيحية
</div>

كُتبت جميع السيناريوهات باستخدام Spark لأن CH لا يدعم الكتابة إلى جداول Iceberg حتى الآن.

<div id="scenario-1">
  #### السيناريو 1: تغييرات المخطط من دون لقطات جديدة
</div>

ضع في اعتبارك تسلسل العمليات التالي:

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

-- Query the table at each timestamp
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

نتائج الاستعلام عند طوابع زمنية مختلفة:

* عند ts1 وts2: لا يظهر سوى العمودين الأصليين
* عند ts3: تظهر الأعمدة الثلاثة جميعها، وتكون قيمة NULL لسعر الصف الأول

<div id="scenario-2">
  #### السيناريو 2: الاختلافات بين المخطط التاريخي والمخطط الحالي
</div>

قد يُظهر استعلام السفر عبر الزمن في اللحظة الحالية مخططًا مختلفًا عن المخطط الحالي للجدول:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

يحدث هذا لأن `ALTER TABLE` لا يُنشئ snapshot جديدة، لكن Spark يأخذ للجدول الحالي قيمة `schema_id` من أحدث ملف بيانات وصفية، وليس من snapshot.

<div id="scenario-3">
  #### السيناريو 3: الاختلافات بين المخطط التاريخي والمخطط الحالي
</div>

أما النقطة الثانية فهي أنه عند استخدام السفر عبر الزمن، لا يمكنك الحصول على حالة الجدول قبل كتابة أي بيانات إليه:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

في ClickHouse، يكون السلوك متسقًا مع Spark. يمكنك اعتبار استعلامات Select في Spark بمثابة استعلامات Select في ClickHouse، وسيعمل الأمر بالطريقة نفسها.

<div id="metadata-file-resolution">
  ## تحديد ملف البيانات الوصفية
</div>

عند استخدام محرك الجدول `Iceberg` في ClickHouse، يحتاج النظام إلى تحديد ملف metadata.json الصحيح الذي يصف بنية جدول Iceberg. إليك كيف تتم هذه العملية:

<div id="candidate-search">
  ### البحث عن الملفات المرشحة
</div>

1. **تحديد المسار مباشرة**:

* إذا قمت بتعيين `iceberg_metadata_file_path`، فسيستخدم النظام هذا المسار المحدد بدمجه مع مسار دليل جدول Iceberg.
* عند توفير هذا الإعداد، يتم تجاهل جميع إعدادات الدقة الأخرى.

2. **مطابقة معرّف UUID للجدول**:

* إذا تم تحديد `iceberg_metadata_table_uuid`، فسيقوم النظام بما يلي:
  * النظر فقط في ملفات `.metadata.json` داخل دليل `metadata`
  * تصفية الملفات التي تحتوي على حقل `table-uuid` يطابق معرّف UUID الذي حددته (غير حساس لحالة الأحرف)

3. **البحث الافتراضي**:

* إذا لم يتم توفير أيٍّ من الإعدادين أعلاه، فستصبح جميع ملفات `.metadata.json` داخل دليل `metadata` ملفات مرشحة

<div id="most-recent-file">
  ### اختيار أحدث ملف
</div>

بعد تحديد الملفات المرشحة وفقًا للقواعد أعلاه، يحدّد النظام الملف الأحدث بينها:

* إذا كان `iceberg_recent_metadata_file_by_last_updated_ms_field` ممكّنًا:
  * يُختار الملف ذو أكبر قيمة لـ `last-updated-ms`

* بخلاف ذلك:
  * يُختار الملف ذو أعلى رقم إصدار
  * (يظهر الإصدار على شكل `V` في أسماء الملفات المنسّقة بصيغة `V.metadata.json` أو `V-uuid.metadata.json`)

**ملاحظة**: جميع الإعدادات المذكورة (ما لم يُذكر خلاف ذلك صراحةً) هي إعدادات على مستوى المحرّك، ويجب تحديدها أثناء إنشاء الجدول كما هو موضّح أدناه:

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**ملاحظة**: رغم أن كتالوجات Iceberg تتولى عادةً تحديد البيانات الوصفية، فإن محرك الجداول `Iceberg` في ClickHouse يفسّر الملفات المخزّنة في S3 مباشرةً على أنها جداول Iceberg، لذا من المهم فهم قواعد التحديد هذه.

<div id="data-cache">
  ## ذاكرة التخزين المؤقت للبيانات
</div>

يدعم محرك الجدول `Iceberg` ودالة الجدول التخزين المؤقت للبيانات، مثل وحدات التخزين `S3` و`AzureBlobStorage` و`HDFS`. راجع [هنا](../../../engines/table-engines/integrations/s3.md#data-cache).

<div id="metadata-cache">
  ## ذاكرة التخزين المؤقت للبيانات الوصفية
</div>

يدعم محرك الجدول Iceberg ودالة الجدول ذاكرةً مؤقتة للبيانات الوصفية لتخزين معلومات manifest files وmanifest list وmetadata json. وتُخزَّن هذه الذاكرة المؤقتة في الذاكرة. ويتحكم في هذه الميزة الإعداد `use_iceberg_metadata_files_cache`، وهو مفعّل افتراضيًا.

<div id="async-metadata-prefetch">
  ## الجلب المسبق غير المتزامن للبيانات الوصفية
</div>

يمكن تمكين الجلب المسبق غير المتزامن للبيانات الوصفية عند إنشاء جدول `Iceberg` من خلال تعيين `iceberg_metadata_async_prefetch_period_ms`. إذا ضُبطت هذه القيمة على 0 (الافتراضي)، أو إذا لم يكن التخزين المؤقت للبيانات الوصفية ممكّنًا، فسيتم تعطيل الجلب المسبق غير المتزامن.
ولتمكين هذه الميزة، يجب تحديد قيمة غير صفرية بالمللي ثانية. وتمثل هذه القيمة الفاصل الزمني بين دورات الجلب المسبق.

إذا كان ممكّنًا، فسيُشغّل الخادم عملية دورية في الخلفية لسرد الكتالوج البعيد واكتشاف إصدار جديد من البيانات الوصفية. ثم سيحلّلها ويجتاز اللقطة بشكل تكراري، مع جلب ملفات manifest list النشطة وملفات manifest.
أما الملفات المتوفرة بالفعل في ذاكرة التخزين المؤقت للبيانات الوصفية، فلن يُعاد تنزيلها. وفي نهاية كل دورة جلب مسبق، تصبح أحدث لقطة للبيانات الوصفية متاحة في ذاكرة التخزين المؤقت للبيانات الوصفية.

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS
    iceberg_metadata_async_prefetch_period_ms = 60000;
```

لتحقيق أقصى استفادة من الجلب المسبق غير المتزامن للبيانات الوصفية في عمليات القراءة، يجب تحديد المعلَمة `iceberg_metadata_staleness_ms` كمعلَمة استعلام أو جلسة. افتراضيًا (0 - غير محددة)، وفي سياق كل استعلام، سيجلب الخادم أحدث البيانات الوصفية من الكتالوج البعيد.
عند تحديد حدٍّ مسموح لتقادم البيانات الوصفية، يُسمح للخادم باستخدام النسخة المخزنة مؤقتًا من لقطة البيانات الوصفية من دون الرجوع إلى الكتالوج البعيد. إذا وُجد إصدار من البيانات الوصفية في ذاكرة التخزين المؤقت، وكان قد نُزِّل ضمن نافذة التقادم المحددة، فسيُستخدم لمعالجة الاستعلام.
وإلا فسيُجلَب أحدث إصدار من الكتالوج البعيد.

```sql
SELECT count() FROM icebench_table WHERE ...
SETTINGS iceberg_metadata_staleness_ms=120000
```

**ملاحظة**: يعمل الجلب المسبق غير المتزامن للبيانات الوصفية على `ICEBERG_SCEDULE_POOL`، وهو مجمّع مؤشرات ترابط من جهة الخادم مخصّص لعمليات الخلفية على جداول `Iceberg` النشطة. ويُتحكَّم في حجم مجمّع مؤشرات الترابط هذا بواسطة معلمة إعداد الخادم `iceberg_background_schedule_pool_size` (القيمة الافتراضية هي 10).

**ملاحظة**: التوقع الحالي هو أن يكون حجم ذاكرة التخزين المؤقت للبيانات الوصفية كافيًا للاحتفاظ بالكامل بأحدث لقطة للبيانات الوصفية لجميع الجداول النشطة، إذا كان الجلب المسبق غير المتزامن مفعّلًا.

<div id="see-also">
  ## راجع أيضًا
</div>

* [دالة الجدول Iceberg](/ar/sql-reference/table-functions/iceberg.md)