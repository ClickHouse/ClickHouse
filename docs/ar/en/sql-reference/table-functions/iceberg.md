---
description: 'يوفر واجهة شبيهة بالجداول للقراءة فقط لجداول Apache Iceberg في
  Amazon S3 أو Azure أو HDFS أو المخزنة محليًا.'
sidebar_label: 'iceberg'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'مرجع'
---

يوفر واجهة شبيهة بالجداول للقراءة فقط لجداول Apache [Iceberg](https://iceberg.apache.org/) في Amazon S3 أو Azure أو HDFS أو المخزنة محليًا.

<div id="syntax">
  ## الصيغة
</div>

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

<div id="arguments">
  ## الوسيطات
</div>

يتطابق وصف الوسيطات مع وصف الوسيطات في دوال الجداول `s3` و`azureBlobStorage` و`HDFS` و`file`، على التوالي.
يشير `format` إلى تنسيق ملفات البيانات في جدول Iceberg.

بالنسبة إلى `icebergS3`، يمكن استخدام معلمة اختيارية باسم `extra_credentials` لتمرير `role_arn` من أجل الوصول المستند إلى الدور في ClickHouse Cloud. راجع [Secure S3](/ar/cloud/data-sources/secure-s3) للاطلاع على خطوات الإعداد.

<div id="returned-value">
  ### القيمة المعادة
</div>

جدول ذو البنية المحددة لقراءة البيانات من جدول Iceberg المحدد.

<div id="example">
  ### مثال
</div>

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
يدعم ClickHouse حاليًا قراءة الإصدارين v1 وv2 من صيغة Iceberg عبر دوال الجدول `icebergS3` و`icebergAzure` و`icebergHDFS` و`icebergLocal`، ومحركات الجداول `IcebergS3` و`icebergAzure` و`IcebergHDFS` و`IcebergLocal`.
:::

<div id="defining-a-named-collection">
  ## تعريف مجموعة مسماة
</div>

إليك مثالًا على تهيئة مجموعة مسماة لتخزين URL وبيانات الاعتماد:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

<div id="iceberg-writes-catalogs">
  ## استخدام كتالوج بيانات
</div>

يمكن أيضًا استخدام جداول Iceberg مع كتالوجات بيانات متعددة، مثل [REST Catalog](https://iceberg.apache.org/rest-catalog-spec/)، و[AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html)، و[Unity Catalog](https://www.unitycatalog.io/).

:::important
عند استخدام كتالوج، سيحتاج معظم المستخدمين إلى استخدام محرك قاعدة البيانات `DataLakeCatalog`، إذ يربط ClickHouse بالكتالوج لديك لاكتشاف جداولك. ويمكنك استخدام محرك قاعدة البيانات هذا بدلًا من إنشاء جداول منفصلة يدويًا باستخدام محرك الجدول `IcebergS3`.
:::

لاستخدام ذلك، أنشئ جدولًا باستخدام المحرك `IcebergS3` وقدّم الإعدادات اللازمة.

على سبيل المثال، استخدام REST Catalog مع تخزين MinIO:

```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
```

أو باستخدام AWS Glue Data Catalog مع S3:

```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
```

<div id="schema-evolution">
  ## تطور المخطط
</div>

في الوقت الحالي، وبمساعدة CH، يمكنك قراءة جداول Iceberg التي تغيّر مخططها بمرور الوقت. نحن ندعم حاليًا قراءة الجداول التي أُضيفت إليها أعمدة وأُزيلت منها أعمدة، وتغيّر ترتيبها. ويمكنك أيضًا تحويل عمود يشترط وجود قيمة فيه إلى عمود يُسمح فيه بالقيمة NULL. بالإضافة إلى ذلك، ندعم تحويل الأنواع المسموح به للأنواع البسيطة، وهي:  

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) حيث P&#39; &gt; P.

حاليًا، لا يمكن تغيير البُنى المتداخلة أو أنواع العناصر داخل المصفوفات وMap.

<div id="partition-pruning">
  ## استبعاد الأقسام
</div>

يدعم ClickHouse استبعاد الأقسام أثناء استعلامات SELECT على جداول Iceberg، مما يساعد على تحسين أداء الاستعلام من خلال تخطي ملفات البيانات غير ذات الصلة. لتمكين استبعاد الأقسام، عيّن `use_iceberg_partition_pruning = 1`. لمزيد من المعلومات حول استبعاد أقسام Iceberg، راجع https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## السفر عبر الزمن
</div>

يدعم ClickHouse ميزة السفر عبر الزمن في جداول Iceberg، مما يتيح لك الاستعلام عن البيانات التاريخية باستخدام طابع زمني محدد أو معرّف لقطة.

<div id="deleted-rows">
  ## معالجة الجداول التي تحتوي على صفوف محذوفة
</div>

حاليًا، لا تُدعَم إلا جداول Iceberg التي تستخدم [عمليات حذف حسب الموضع](https://iceberg.apache.org/spec/#position-delete-files).

طرق الحذف التالية **غير مدعومة**:

* [عمليات الحذف بالمساواة](https://iceberg.apache.org/spec/#equality-delete-files)
* [متجهات الحذف](https://iceberg.apache.org/spec/#deletion-vectors) (طُرحت في v3)

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

ملاحظة: لا يمكنك تحديد كلٍّ من المعلمتين `iceberg_timestamp_ms` و`iceberg_snapshot_id` في الاستعلام نفسه.

<div id="important-considerations">
  ### اعتبارات مهمة
</div>

* **اللقطات** تُنشأ عادةً عندما:

* تُكتب بيانات جديدة إلى الجدول

* تُجرى عملية دمج للبيانات من نوع ما

* **لا تؤدي تغييرات المخطط عادةً إلى إنشاء لقطات** - وهذا يؤدي إلى سلوكيات مهمة عند استخدام السفر عبر الزمن مع الجداول التي خضعت لتطور في المخطط.

<div id="example-scenarios">
  ### أمثلة على السيناريوهات
</div>

جميع السيناريوهات مكتوبة باستخدام Spark لأن CH لا يدعم الكتابة إلى جداول Iceberg حتى الآن.

<div id="scenario-1">
  #### السيناريو 1: تغييرات المخطط من دون لقطات جديدة
</div>

ضع في اعتبارك تسلسل العمليات التالي:

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
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

* عند ts1 و ts2: لا يظهر سوى العمودين الأصليين
* عند ts3: تظهر الأعمدة الثلاثة جميعًا، مع قيمة NULL لسعر الصف الأول

<div id="scenario-2">
  #### السيناريو 2: اختلافات المخطط التاريخي مقارنةً بالحالي
</div>

قد يُظهر استعلام السفر عبر الزمن عند اللحظة الحالية مخططًا يختلف عن المخطط الحالي للجدول:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
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

يحدث هذا لأن `ALTER TABLE` لا ينشئ لقطة جديدة، لكن Spark في حالة الجدول الحالي يأخذ قيمة `schema_id` من أحدث ملف البيانات الوصفية، وليس من لقطة.

<div id="scenario-3">
  #### السيناريو 3: الاختلافات بين المخطط التاريخي والمخطط الحالي
</div>

أما الثانية، فهي أنك أثناء استخدام ميزة السفر عبر الزمن لا يمكنك الحصول على حالة الجدول قبل أن تُكتب فيه أي بيانات:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

في ClickHouse، يتوافق السلوك مع Spark. ويمكنك اعتبار استعلامات Select في ClickHouse بديلًا ذهنيًا لاستعلامات Select في Spark، وستعمل بالطريقة نفسها.

<div id="metadata-file-resolution">
  ## تحديد ملف البيانات الوصفية
</div>

عند استخدام دالة الجدول `iceberg` في ClickHouse، يحتاج النظام إلى العثور على ملف `metadata.json` الصحيح الذي يصف بنية جدول Iceberg. إليك كيفية عمل هذه العملية:

<div id="candidate-search">
  ### البحث عن المرشحين (حسب ترتيب الأولوية)
</div>

1. **تحديد المسار مباشرةً**:
   *إذا قمت بتعيين `iceberg_metadata_file_path`، فسيستخدم النظام هذا المسار حرفيًا بعد دمجه مع مسار دليل جدول Iceberg.

* عند توفير هذا الإعداد، يتم تجاهل جميع إعدادات التحديد الأخرى.

2. **مطابقة معرّف UUID للجدول**:
   *إذا تم تحديد `iceberg_metadata_table_uuid`، فسيقوم النظام بما يلي:
   *النظر فقط في ملفات `.metadata.json` داخل دليل `metadata`
   *تصفية الملفات التي تحتوي على الحقل `table-uuid` المطابق لمعرّف UUID الذي حددته (من دون حساسية لحالة الأحرف)

3. **البحث الافتراضي**:
   *إذا لم يتم توفير أي من الإعدادين أعلاه، تصبح جميع ملفات `.metadata.json` في دليل `metadata` مرشحين

<div id="most-recent-file">
  ### اختيار أحدث ملف
</div>

بعد تحديد الملفات المرشحة باستخدام القواعد أعلاه، يحدّد النظام أيّها الأحدث:

* إذا كان `iceberg_recent_metadata_file_by_last_updated_ms_field` مفعّلًا:

* يُختار الملف ذو أكبر قيمة `last-updated-ms`

* بخلاف ذلك:

* يُختار الملف ذو أعلى رقم إصدار

* (يظهر الإصدار بصيغة `V` في أسماء الملفات المنسّقة على النحو `V.metadata.json` أو `V-uuid.metadata.json`)

**ملاحظة**: جميع الإعدادات المذكورة هي إعدادات دالة الجدول (وليست إعدادات عامة أو على مستوى الاستعلام) ويجب تحديدها كما هو موضّح أدناه:

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**ملاحظة**: بينما تتولى كتالوجات Iceberg عادةً معالجة تحديد البيانات الوصفية، فإن دالة الجدول `iceberg` في ClickHouse تفسّر مباشرةً الملفات المخزّنة في S3 على أنها جداول Iceberg، ولذلك من المهم فهم قواعد التحديد هذه.

<div id="metadata-cache">
  ## ذاكرة التخزين المؤقت للبيانات الوصفية
</div>

يدعم كلٌّ من محرك الجدول `Iceberg` ودالة الجدول ذاكرةً مؤقتةً للبيانات الوصفية لتخزين معلومات ملفات manifest، وقائمة manifest، وملف JSON الخاص بالبيانات الوصفية. تُخزَّن هذه الذاكرة المؤقتة في الذاكرة. ويُتحكَّم في هذه الميزة بواسطة الإعداد `use_iceberg_metadata_files_cache`، وهو مُفعَّل افتراضيًا.

<div id="aliases">
  ## الأسماء المستعارة
</div>

دالة الجدول `iceberg` هي الآن اسم مستعار لـ `icebergS3`.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.
* `_etag` — قيمة etag للملف. النوع: `LowCardinality(String)`. إذا كانت قيمة etag غير معروفة، تكون القيمة `NULL`.

<div id="writes-into-iceberg-table">
  ## الكتابة في جدول Iceberg
</div>

اعتبارًا من الإصدار 25.7، يدعم ClickHouse إجراء تعديلات على جداول Iceberg الخاصة بالمستخدم.

حاليًا، هذه ميزة تجريبية، لذا تحتاج أولًا إلى تفعيلها:

```sql
SET allow_insert_into_iceberg = 1;
```

<div id="create-iceberg-table">
  ### إنشاء جدول Iceberg
</div>

لإنشاء جدول Iceberg فارغ خاص بك، استخدم الأوامر نفسها المستخدمة للقراءة، لكن حدِّد المخطط بشكل صريح.
تدعم الكتابة جميع تنسيقات البيانات المحددة في مواصفة Iceberg، مثل Parquet وAvro وORC.

<div id="example">
  ### مثال
</div>

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

ملاحظة: لإنشاء ملف تلميح الإصدار، فعِّل الإعداد `iceberg_use_version_hint`.
إذا أردت ضغط ملف metadata.json، فحدِّد اسم الـcodec في الإعداد `iceberg_metadata_compression_method`.

<div id="writes-inserts">
  ### INSERT
</div>

بعد إنشاء جدول جديد، يمكنك إدراج البيانات باستخدام صيغة ClickHouse المعتادة.

<div id="example">
  ### مثال
</div>

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-delete">
  ### DELETE
</div>

يدعم ClickHouse أيضًا حذف الصفوف الزائدة في تنسيق merge-on-read.
سينشئ هذا الاستعلام لقطة جديدة مع ملفات حذف المواضع.

<div id="example">
  ### مثال
</div>

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-schema-evolution">
  ### تطور المخطط
</div>

يتيح لك ClickHouse إضافة الأعمدة ذات الأنواع البسيطة أو حذفها أو تعديلها أو إعادة تسميتها (باستثناء tuple وarray وmap).

<div id="example">
  ### مثال
</div>

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

<div id="iceberg-writes-compaction">
  ### الدمج
</div>

يدعم ClickHouse دمج جداول Iceberg. حاليًا، يمكنه دمج ملفات حذف المواضع في ملفات البيانات مع تحديث البيانات الوصفية. وتبقى معرّفات اللقطات السابقة والطوابع الزمنية من دون تغيير، لذا تظل ميزة السفر عبر الزمن قابلة للاستخدام بالقيم نفسها.

كيفية استخدامه:

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-expire-snapshots">
  ### حذف اللقطات القديمة
</div>

تتراكم اللقطات في جداول Iceberg مع كل عملية `INSERT` أو `DELETE` أو `UPDATE`. ومع مرور الوقت، قد يؤدي ذلك إلى وجود عدد كبير من اللقطات وملفات البيانات المرتبطة بها. يزيل الأمر `expire_snapshots` اللقطات القديمة وينظّف ملفات البيانات التي لم تعد أي لقطة محتفَظ بها تُشير إليها.

**البنية:**

```sql
ALTER TABLE iceberg_table EXECUTE expire_snapshots(
    ['timestamp']
    [, expire_before = 'timestamp']
    [, retention_period = '3d']
    [, retain_last = 100]
    [, snapshot_ids = [1, 2, 3, 4]]
    [, dry_run = 1]
);
```

بشكل افتراضي، تُحدَّد اللقطات التي يجب الاحتفاظ بها وفقًا لـ[سياسة الاستبقاء](#iceberg-snapshot-retention-policy) (خصائص الجدول `min-snapshots-to-keep` و`max-snapshot-age-ms` وتجاوزات كل مرجع). عند تحديد `snapshot_ids`، يتم تجاوز سياسة الاستبقاء ولا تُؤخذ في الحسبان لانقضاء الصلاحية إلا اللقطات المُدرجة فقط.

**الوسائط:**

* `'timestamp'` (موضعي) أو `expire_before = 'timestamp'` — سلسلة تاريخ ووقت (مثل `'2024-06-01 00:00:00'`) تُفسَّر وفق **المنطقة الزمنية للخادم**. يعمل ذلك كصمام أمان: اللقطات التي تكون قيمة `timestamp-ms` الخاصة بها مساوية لهذه القيمة أو لاحقة لها تكون محمية من انقضاء الصلاحية، حتى لو كانت سياسة الاستبقاء ستنهي صلاحيتها لولا ذلك. يمكن دمجه مع `snapshot_ids`، وفي هذه الحالة لا تنقضي صلاحية اللقطات المُدرجة التي تقع عند هذا الطابع الزمني أو بعده.
* `retention_period = '<duration>'` — يتجاوز `history.expire.max-snapshot-age-ms` على مستوى الجدول لهذا الاستدعاء فقط. تصبح اللقطات الأقدم من هذه المدة (مقاسة من الآن) مرشحة لانقضاء الصلاحية. تكون القيمة سلسلة مدة تتكوّن من زوج واحد أو أكثر من `{number}{unit}` موصولة معًا. الوحدات المدعومة: `y` (365 يومًا)، `w` (7 أيام)، `d` (24 ساعة)، `h` (60 دقيقة)، `m` (60 ثانية)، `s` (ثانية واحدة)، `ms` (ملّي ثانية واحدة). يمكن دمج الوحدات، مثل `'3d'` و`'12h'` و`'1d12h30m'` و`'500ms'`.
* `retain_last = N` — يتجاوز `history.expire.min-snapshots-to-keep` على مستوى الجدول لهذا الاستدعاء فقط. يُحتفَظ دائمًا بما لا يقل عن `N` من اللقطات بغضّ النظر عن عمرها.
* `snapshot_ids = [id1, id2, ...]` — يُنهي صلاحية اللقطات ذات المعرّفات المُدرجة فقط (باستثناء اللقطات المشار إليها بواسطة اللقطة الحالية أو الفروع أو الوسوم). يتجاوز هذا الوضع سياسة الاستبقاء بالكامل، ولا يمكن دمجه مع `retention_period` أو `retain_last`.
* `dry_run = 1` — يحسب ما الذي ستنتهي صلاحيته ويُرجع المقاييس دون كتابة بيانات وصفية جديدة أو حذف الملفات.

:::note
يتجاوز `retention_period` و`retain_last` قيم الاستبقاء الافتراضية على **مستوى الجدول** فقط. أما تجاوزات الاستبقاء لكل مرجع (فرع/وسم) التي تم تكوينها في خصائص جدول Iceberg (مثل `refs.<branch>.min-snapshots-to-keep`) فلا يتم تجاوزها مطلقًا — بل تُطبَّق دائمًا كما هي محددة في البيانات الوصفية للجدول.
:::

**مثال:**

```sql
SET allow_insert_into_iceberg = 1;

-- Create some snapshots by inserting data
INSERT INTO iceberg_table VALUES (1);
INSERT INTO iceberg_table VALUES (2);
INSERT INTO iceberg_table VALUES (3);

-- Expire using retention policy only
ALTER TABLE iceberg_table EXECUTE expire_snapshots();

-- Expire with a safety fuse: protect snapshots newer than the timestamp (positional syntax)
ALTER TABLE iceberg_table EXECUTE expire_snapshots('2025-01-01 00:00:00');

-- Same using the named argument form
ALTER TABLE iceberg_table EXECUTE expire_snapshots(expire_before = '2025-01-01 00:00:00');

-- Override retention parameters for one execution
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '3d', retain_last = 10);

-- Expire explicit snapshots
ALTER TABLE iceberg_table EXECUTE expire_snapshots(snapshot_ids = [101, 102, 103]);

-- Dry-run preview (no metadata updates, no file deletes)
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '1d', dry_run = 1);
```

**المخرجات:**

يعيد الأمر جدولًا يتكوّن من عمودين (`metric_name String`, `metric_value Int64`)، ويحتوي على صف واحد لكل مقياس. وتتبع أسماء المقاييس [مواصفة Iceberg](https://iceberg.apache.org/docs/latest/spark-procedures/#output):

| metric&#95;name                       | الوصف                                               |
| ------------------------------------- | --------------------------------------------------- |
| `deleted_data_files_count`            | عدد ملفات البيانات المحذوفة                         |
| `deleted_position_delete_files_count` | عدد ملفات حذف الموضع المحذوفة                       |
| `deleted_equality_delete_files_count` | عدد ملفات حذف المساواة المحذوفة                     |
| `deleted_manifest_files_count`        | عدد ملفات المانيفست المحذوفة                        |
| `deleted_manifest_lists_count`        | عدد ملفات قائمة المانيفست المحذوفة                  |
| `deleted_statistics_files_count`      | عدد ملفات الإحصاءات المحذوفة (حاليًا تكون دائمًا 0) |
| `dry_run`                             | `1` لوضع التشغيل التجريبي، و`0` للتنفيذ العادي      |

ينفّذ الأمر الخطوات التالية:

1. يقيّم سياسة الاحتفاظ (انظر أدناه) لتحديد اللقطات التي يجب الإبقاء عليها
2. إذا تم تمرير وسيطة طابع زمني، فإنه يحمي أيضًا جميع اللقطات عند هذا الطابع الزمني أو الأحدث منه
3. ينتهي من صلاحية اللقطات التي لا تُحتفَظ بها وفق السياسة ولا يحميها قيد الطابع الزمني
4. يحسب الملفات المرتبطة حصريًا باللقطات منتهية الصلاحية
5. في الوضع العادي: ينشئ بيانات وصفية جديدة من دون اللقطات منتهية الصلاحية
6. في الوضع العادي: يحذف فعليًا ملفات قوائم المانيفست وملفات المانيفست وملفات البيانات التي لم يعد من الممكن الوصول إليها
7. في وضع `dry_run = 1`: يتخطى الخطوتين 5 و6 ويُرجع فقط المقاييس المحسوبة

<div id="iceberg-snapshot-retention-policy">
  #### سياسة الاحتفاظ باللقطات
</div>

يلتزم الأمر `expire_snapshots` بـ [سياسة الاحتفاظ بلقطات Iceberg](https://iceberg.apache.org/spec/#snapshot-retention-policy). يُضبط الاحتفاظ من خلال خصائص جدول Iceberg وقيم التجاوز الخاصة بكل مرجع:

| الخاصية                                | النطاق | القيمة الافتراضية                                                            | الوصف                                                                        |
| -------------------------------------- | ------ | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `history.expire.min-snapshots-to-keep` | الجدول | `iceberg_expire_default_min_snapshots_to_keep` (الافتراضي `1`)               | الحد الأدنى لعدد اللقطات التي يجب الاحتفاظ بها في سلسلة أسلاف كل فرع         |
| `history.expire.max-snapshot-age-ms`   | الجدول | `iceberg_expire_default_max_snapshot_age_ms` (الافتراضي `432000000`، 5 أيام) | الحد الأقصى لعمر اللقطات (بالملي ثانية) التي يُحتفَظ بها في الفرع            |
| `history.expire.max-ref-age-ms`        | الجدول | `iceberg_expire_default_max_ref_age_ms` (الافتراضي `∞`)                      | الحد الأقصى لعمر مرجع اللقطة (فرع أو وسم) بالملي ثانية قبل إزالة المرجع نفسه |

يمكن لكل مرجع لقطة (`refs` في بيانات Iceberg الوصفية) تجاوز هذه القيم باستخدام حقول خاصة بكل مرجع: `min-snapshots-to-keep` و`max-snapshot-age-ms` و`max-ref-age-ms`.

**تقييم الاحتفاظ:**

* **لكل فرع** (بما في ذلك `main`): تُتَّبع سلسلة الأسلاف بدءًا من رأس الفرع. ويُحتفَظ باللقطات ما دام أحد الشرطين التاليين متحققًا:
  * أن تكون اللقطة من بين أول `min-snapshots-to-keep` لقطات في السلسلة
  * أن يكون عمر اللقطة ضمن `max-snapshot-age-ms` (أي `now - timestamp-ms <= max-snapshot-age-ms`)
* **بالنسبة إلى الأوسمة**: يُحتفَظ باللقطة الموسومة ما لم يتجاوز الوسم قيمة `max-ref-age-ms` الخاصة به، وعندئذٍ يُزال مرجع الوسم
* **المراجع غير `main`** التي يتجاوز عمرها `max-ref-age-ms` تُزال بالكامل (ولا يُزال الفرع `main` مطلقًا)
* **المراجع المعلّقة** التي تشير إلى لقطات غير موجودة تُزال مع تحذير
* **تُحفَظ اللقطة الحالية دائمًا**، بغض النظر عن إعدادات الاحتفاظ

**الامتيازات المطلوبة:**

امتياز `ALTER TABLE EXECUTE` مطلوب، وهو امتياز فرعي من `ALTER TABLE` ضمن تسلسل هرمي للتحكم في الوصول في ClickHouse. ويمكنك منحه تحديدًا أو من خلال الامتياز الأب:

```sql
-- Grant only EXECUTE permission
GRANT ALTER TABLE EXECUTE ON my_iceberg_table TO my_user;

-- Or grant all ALTER TABLE permissions (includes ALTER TABLE EXECUTE)
GRANT ALTER TABLE ON my_iceberg_table TO my_user;
```

:::note

* لا يدعم سوى جداول Iceberg format version 2 (إذ لا تضمن لقطات v1 وجود `manifest-list`، وهو مطلوب لتحديد الملفات بأمان لغرض التنظيف)
* يجري دائمًا الاحتفاظ بـ اللقطة الحالية، حتى إذا كانت أقدم من الطابع الزمني المحدد
* يتطلب أن يكون الإعداد `allow_insert_into_iceberg` مُمكّنًا
* يتطلب أن يكون الإعداد `allow_experimental_expire_snapshots` مُمكّنًا
* يُفرَض التفويض الخاص بالـ كتالوج نفسه (مثل مصادقة REST كتالوج وAWS Glue IAM وغيرها) بشكل مستقل عندما يحدّث ClickHouse البيانات الوصفية
  :::

<div id="iceberg-remove-orphan-files">
  ### إزالة الملفات اليتيمة
</div>

الملفات اليتيمة هي ملفات موجودة في التخزين ولا تشير إليها أي لقطة في البيانات الوصفية لجدول Iceberg. وتتراكم بسبب عمليات الكتابة الفاشلة، والتنظيف الجزئي بعد الدمج، والعمليات المتوقفة، مما يؤدي إلى نمو غير محدود في التخزين. يحدّد الأمر `remove_orphan_files` هذه الملفات اليتيمة ويزيلها.

**الصيغة:**

```sql
-- Positional form: single unnamed older_than argument
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('timestamp')

-- Named form
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = 'timestamp',
    location = 'path',
    dry_run = 0|1
)

-- No arguments: use all defaults (older_than = 3 days ago)
ALTER TABLE iceberg_table EXECUTE remove_orphan_files()
```

**المعلمات:**

| المعلمة      | النوع                | الافتراضي                                                               | الوصف                                                                                                                                                                           |
| ------------ | -------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `older_than` | `String` (طابع زمني) | قبل 3 أيام (قابل للتهيئة عبر `iceberg_orphan_files_older_than_seconds`) | لا تُعدّ ملفاتٍ يتيمةً محتملة إلا الملفات التي يكون وقت آخر تعديل لها أقدم من هذا الطابع الزمني. هذا إجراء أمان لتجنّب حذف الملفات الناتجة عن عمليات كتابة لا تزال قيد التنفيذ. |
| `location`   | `String`             | موقع الجدول                                                             | يقتصر الفحص على دليل فرعي محدد ضمن موقع الجدول (على سبيل المثال، `'data/'` أو `'metadata/'`).                                                                                   |
| `dry_run`    | `UInt64`             | `0`                                                                     | عند ضبط القيمة على `1`، يتم تحديد الملفات اليتيمة وإرجاع ملخص النتيجة دون حذف أي شيء فعليًا.                                                                                    |

**أمثلة:**

```sql
-- Remove orphan files older than a specific timestamp
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('2026-03-01 00:00:00');

-- Dry run: preview which files would be deleted
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(dry_run = 1);

-- Scan only the data directory
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = '2026-03-01 00:00:00',
    location = 'data/'
);

-- Combine positional older_than with named arguments
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    '2026-03-01 00:00:00',
    dry_run = 1
);
```

**المخرجات:**

يعيد الأمر جدولًا يحتوي على العمودين `metric_name` و`metric_value`، ويعرض عدد الملفات المحذوفة (أو التي كان سيجري حذفها في وضع `dry&#95;run`) بحسب الفئة. وتُصنَّف فئات الملفات باستخدام استدلالات تقريبية تعتمد على اصطلاحات تسمية الملفات؛ أما الملفات التي لا تطابق أي نمط محدد فتُحتسب افتراضيًا ضمن `deleted_data_files_count`:

| metric&#95;name                                     | metric&#95;value |
| --------------------------------------------------- | ---------------- |
| deleted&#95;data&#95;files&#95;count                | 5                |
| deleted&#95;position&#95;delete&#95;files&#95;count | 2                |
| deleted&#95;equality&#95;delete&#95;files&#95;count | 0                |
| deleted&#95;manifest&#95;files&#95;count            | 3                |
| deleted&#95;manifest&#95;lists&#95;count            | 1                |
| deleted&#95;metadata&#95;files&#95;count            | 0                |
| deleted&#95;statistics&#95;files&#95;count          | 0                |
| skipped&#95;missing&#95;metadata&#95;count          | 0                |
| failed&#95;deletions&#95;count                      | 0                |

**الإعدادات:**

| الإعداد                                   | النوع    | القيمة الافتراضية | الوصف                                                 |
| ----------------------------------------- | -------- | ----------------- | ----------------------------------------------------- |
| `allow_iceberg_remove_orphan_files`       | `Bool`   | `false`           | إعداد تحكّم لتمكين هذه الميزة (تجريبية).              |
| `iceberg_orphan_files_older_than_seconds` | `UInt64` | `259200` (3 days) | قيمة `older_than` الافتراضية بالثواني عند حذف الوسيط. |

:::note

* **يتطلب Iceberg format version 2 (أو أحدث).** تُرفَض جداول الإصدار 1 لأنها تفتقر إلى مؤشرات `manifest-list` في `snapshots`، وهي مطلوبة لتحديد مجموعة الملفات القابلة للوصول بأمان. ويؤدي تشغيل الأمر على جدول v1 إلى إرجاع الخطأ `BAD_ARGUMENTS`.
* يتطلب ذلك تمكين كلٍّ من الإعدادين `allow_insert_into_iceberg` و`allow_iceberg_remove_orphan_files`
* يُوصى بتشغيل `expire_snapshots` قبل `remove_orphan_files` لكي تُنظَّف أولًا الملفات المشار إليها بشكل فريد بواسطة `snapshots` منتهية الصلاحية
* استخدم `dry_run = 1` لمعاينة `orphan files` قبل الحذف
* توفّر عتبة `older_than` حماية من حذف الملفات الناتجة عن عمليات كتابة ما تزال قيد التنفيذ — وتمنح القيمة الافتراضية البالغة 3 أيام هامش أمان مريحًا
  :::

<div id="see-also">
  ## راجع أيضًا
</div>

* [محرك Iceberg](/ar/engines/table-engines/integrations/iceberg.md)
* [دالة الجدول cluster في Iceberg](/ar/sql-reference/table-functions/icebergCluster.md)