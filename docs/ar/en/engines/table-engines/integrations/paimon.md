---
description: 'يوفّر هذا المحرك تكاملًا للقراءة فقط مع جداول Apache Paimon الموجودة
  على Amazon S3 وAzure وHDFS والجداول المخزّنة محليًا.'
sidebar_label: 'Paimon'
sidebar_position: 95
slug: /engines/table-engines/integrations/paimon
title: 'محرك جدول Paimon'
doc_type: 'مرجع'
---

يوفّر هذا المحرك تكاملًا للقراءة فقط مع جداول Apache [Paimon](https://paimon.apache.org/) الموجودة على Amazon S3 وAzure وHDFS والجداول المخزّنة محليًا.
كما يدعم قراءة اللقطات، والقراءة التزايدية، والاستبعاد الأساسي للأقسام الذي يوفّره المحرك.

<div id="create-table">
  ## إنشاء جدول
</div>

لاحظ أن جدول Paimon يجب أن يكون موجودًا بالفعل في التخزين؛ فهذا الأمر لا يدعم معلمات DDL لإنشاء جدول جديد.
ويخضع إنشاء جداول `Paimon*` لبوابة `allow_experimental_paimon_storage_engine` (وهي معطّلة افتراضيًا)، لذا فعِّلها قبل تشغيل `CREATE TABLE`.

```sql
SET allow_experimental_paimon_storage_engine = 1;

CREATE TABLE paimon_table_s3
    ENGINE = PaimonS3(url,  [, access_key_id, secret_access_key] [,format] [,compression])

CREATE TABLE paimon_table_azure
    ENGINE = PaimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

CREATE TABLE paimon_table_hdfs
    ENGINE = PaimonHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE paimon_table_local
    ENGINE = PaimonLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## وسيطات المحرك
</div>

وصف هذه الوسيطات مطابق لوصف الوسيطات في المحركات `S3` و`AzureBlobStorage` و`HDFS` و`File` على التوالي.
يشير `format` إلى تنسيق ملفات البيانات في جدول Paimon.

يمكن تحديد معلمات المحرك باستخدام [المجموعات المسماة](../../../operations/named-collections.md)

<div id="example">
  ### مثال
</div>

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

باستخدام مجموعات مسماة:

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3(paimon_conf, filename = 'test_table')
```

<div id="capabilities">
  ## الإمكانات
</div>

* قراءة اللقطات من أحدث لقطة للجدول.
* قراءات تزايدية تستند إلى معرّف اللقطة المُعتمَدة عند التمكين.
* استبعاد الأقسام عند تمكين `use_paimon_partition_pruning`.
* تحديث اختياري للبيانات الوصفية في الخلفية عند ضبطه.
* معرّف UUID ثابت للجدول عند استخدام قواعد بيانات Atomic/Replicated، مما يتيح استخدام ماكرو `{uuid}` في مسارات Keeper.

<div id="settings">
  ## الإعدادات
</div>

يستخدم هذا المحرّك الإعدادات نفسها المستخدمة في محرّكات تخزين الكائنات المقابلة، ويضيف إعدادات خاصة بـ Paimon:

* `allow_experimental_paimon_storage_engine` — يفعّل إنشاء محرّكات الجداول `Paimon` و`PaimonS3` و`PaimonAzure` و`PaimonHDFS` و`PaimonLocal`. القيمة الافتراضية: `0` (معطّل).
* `paimon_incremental_read` — يفعّل وضع القراءة التزايدية.
* `paimon_metadata_refresh_interval_sec` — الفاصل الزمني لتحديث البيانات الوصفية في الخلفية، بالثواني. عند ضبطه على قيمة أكبر من 0، تسحب مهمة تعمل في الخلفية أحدث لقطة ومخطط دوريًا من تخزين الكائنات. القيمة الافتراضية: 30.
* `paimon_keeper_path` — مسار Keeper لحالة القراءة التزايدية. يجب ضبطه وأن يكون فريدًا لكل جدول؛ ويدعم ماكرو مثل `{database}` و`{table}` و`{uuid}`.
* `paimon_replica_name` — اسم النسخة المتماثلة لحالة القراءة التزايدية. يجب ضبطه وأن يكون فريدًا لكل نسخة متماثلة؛ ويدعم ماكرو مثل `{replica}`.

<div id="incremental-read-examples">
  ## أمثلة على القراءة التزايدية
</div>

القراءة التزايدية باستخدام حالة Keeper:

```sql
CREATE TABLE paimon_inc
ENGINE = PaimonS3(paimon_conf, filename = 'paimon_all_types')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/{database}/{uuid}',
    paimon_replica_name = '{replica}';
```

<div id="query-level-settings-for-incremental-read">
  ### إعدادات على مستوى الاستعلام للقراءة التزايدية
</div>

الإعدادات التالية هي **على مستوى الاستعلام** (تُمرَّر عبر `SELECT ... SETTINGS`، وليس ضمن `CREATE TABLE`). وهي تتحكم في سلوك القراءة التزايدية لكل استعلام على حدة:

* `paimon_target_snapshot_id` — يقرأ فقط دلتا اللقطة المحددة. ولا يتم تحريك قيمة الـ watermark المُعتمدة في Keeper إلى الأمام، لذا يمكن إعادة قراءة اللقطة نفسها أي عدد من المرات. القيمة الافتراضية: `-1` (معطّل).
* `max_consume_snapshots` — الحد الأقصى لعدد اللقطات التي يمكن استهلاكها في قراءة تزايدية واحدة. عندما يكون المصدر قد راكم عددًا كبيرًا من اللقطات غير المقروءة، يقيّد هذا الإعداد عدد اللقطات التي تُستهلك في كل استعلام للتحكم في حجم الدفعة. `0` تعني عدم وجود حد. القيمة الافتراضية: `0`.

**قراءة لقطة مستهدفة** — تُرجع دائمًا دلتا اللقطة 1، بغض النظر عن قيمة الـ watermark الحالية:

```sql
SELECT count()
FROM paimon_inc
SETTINGS paimon_target_snapshot_id = 1;
```

**الحد من عدد اللقطات في كل دفعة** — إذا كانت هناك ثلاث لقطات جديدة قيد الانتظار، فلا تستهلك أكثر من اثنتين في كل استعلام:

```sql
SELECT count()
FROM paimon_inc
SETTINGS max_consume_snapshots = 2;
```

<div id="paimon-to-mergetree-via-refresh-mv">
  ## من Paimon إلى MergeTree عبر عرض مادي قابل للتحديث
</div>

يمكنك إنشاء pipeline متكامل يزامن البيانات باستمرار من جدول Paimon إلى جدول MergeTree باستخدام عرض مادي قابل للتحديث في وضع `APPEND`. وفي كل دورة تحديث، لا تُقرأ من Paimon إلا البيانات التزايدية الجديدة ثم تُلحَق بجدول الوجهة.

**الخطوة 1 — أنشئ جدول المصدر في Paimon مع تمكين القراءة التزايدية وتحديث البيانات الوصفية.**

يستخدم المثال أدناه `PaimonLocal`. استبدل الـ engine بـ `PaimonS3` أو `PaimonAzure` أو `PaimonHDFS` أو الـ engine `Paimon` ذي الاكتشاف التلقائي، بحسب ما يناسب الواجهة الخلفية للتخزين لديك:

```sql
SET allow_experimental_paimon_storage_engine = 1;

-- Local storage
CREATE TABLE paimon_mv_source
ENGINE = PaimonLocal('/path/to/paimon/table')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;

-- S3 storage (the `Paimon` engine defaults to the S3 implementation when no `disk` is specified)
CREATE TABLE paimon_mv_source
ENGINE = Paimon('http://minio:9000/bucket/path/to/table', 'access_key', 'secret_key')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;
```

يضبط `paimon_metadata_refresh_interval_sec` الفاصل الزمني لتحديث البيانات الوصفية في الخلفية، بالثواني. وعندما تكون قيمته أكبر من 0، تجلب مهمة تعمل في الخلفية بشكل دوري أحدث لقطة ومخطط من تخزين الكائنات، بحيث تتمكن دورة تحديث MV من رؤية البيانات التي جرى اعتمادها حديثًا دون انتظار استعلام ليؤدي إلى تشغيل تحديث البيانات الوصفية. القيمة الافتراضية هي 30. استخدمه بحذر عند التعامل مع عدد كبير من الجداول لتجنب الإفراط في عمليات I/O على تخزين الكائنات وKeeper.

**الخطوة 2 — أنشئ جدول الوجهة MergeTree (مع استنساخ مخطط من جدول Paimon):**

```sql
CREATE TABLE paimon_mv_dest AS paimon_mv_source
ENGINE = MergeTree()
ORDER BY tuple();
```

**الخطوة 3 — أنشئ العرض المادي القابل للتحديث:**

```sql
CREATE MATERIALIZED VIEW paimon_mv
REFRESH EVERY 10 SECOND
APPEND
TO paimon_mv_dest
AS SELECT * FROM paimon_mv_source;
```

كل 10 ثوانٍ، يُنفَّذ في MV الاستعلام `SELECT * FROM paimon_mv_source`، والذي لا يعيد إلا الصفوف المضافة منذ آخر لقطة معتمدة، ويُضيفها إلى `paimon_mv_dest`.

**التنظيف:**

```sql
SYSTEM STOP VIEW paimon_mv;
DROP VIEW IF EXISTS paimon_mv SYNC;
DROP TABLE IF EXISTS paimon_mv_dest SYNC;
DROP TABLE IF EXISTS paimon_mv_source SYNC;
```

:::note
أوقِف الـ MV قبل إسقاطه لمنع التحديث في الخلفية من تعطيل عمليات DDL.
:::

<div id="limitations">
  ## القيود
</div>

* تتطلب القراءة التزايدية تهيئة Keeper ‏(ZooKeeper).
* تتطلب القراءة التزايدية ضبط `paimon_keeper_path` بحيث يكون فريدًا لكل جدول.
* يجب أن يكون `paimon_replica_name` فريدًا لكل نسخة متماثلة ضمن مسار Keeper نفسه.
* تستخدم القراءة التزايدية آلية تسليم من نوع at-most-once: إذ تُقدَّم اللقطة المُعتمدة عند جمع ملفات البيانات، قبل استهلاك البيانات فعليًا. وإذا فشل الاستعلام بعد جمع الملفات، فلن تُعاد قراءة اللقطات التي تم تخطيها عند إعادة المحاولة.
* محرك الجدول للقراءة فقط؛ ولا يدعم تعديل البيانات.
* لا تتعامل القراءة التزايدية مع حذف البيانات التاريخية من مصدر Paimon. فإذا حُذفت بيانات Paimon المصدرية أو جرى تحديثها، فلن تُزال تلقائيًا الصفوف المقابلة التي كُتبت بالفعل إلى جدول وجهة MergeTree في ClickHouse. ويجب عليك تنفيذ `ALTER TABLE ... DELETE` يدويًا على جدول MergeTree لتنظيف البيانات المتقادمة.

<div id="aliases">
  ## الأسماء المستعارة
</div>

يكتشف محرك الجدول `Paimon` الواجهة الخلفية للتخزين تلقائيًا من إعداد `disk`، ثم يوجّه التنفيذ إلى `PaimonS3` أو `PaimonAzure` أو `PaimonLocal` وفقًا لذلك. وإذا لم يتم تحديد `disk`، فسيُستخدم تنفيذ `PaimonS3` افتراضيًا.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.
* `_etag` — قيمة etag للملف. النوع: `LowCardinality(String)`. إذا كانت قيمة etag غير معروفة، تكون القيمة `NULL`.

<div id="data-types-supported">
  ## أنواع البيانات المدعومة
</div>

| نوع بيانات Paimon                 | نوع بيانات ClickHouse     |
| --------------------------------- | ------------------------- |
| BOOLEAN                           | Int8                      |
| TINYINT                           | Int8                      |
| SMALLINT                          | Int16                     |
| INTEGER                           | Int32                     |
| BIGINT                            | Int64                     |
| FLOAT                             | Float32                   |
| DOUBLE                            | Float64                   |
| STRING,VARCHAR,BYTES,VARBINARY    | String                    |
| DATE                              | Date                      |
| TIME(p),TIME                      | Time(&#39;UTC&#39;)       |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | DateTime64                |
| TIMESTAMP(p)                      | DateTime64(&#39;UTC&#39;) |
| CHAR                              | FixedString(1)            |
| BINARY(n)                         | FixedString(n)            |
| DECIMAL(P,S)                      | Decimal(P,S)              |
| ARRAY                             | Array                     |
| MAP                               | Map                       |

<div id="partition-supported">
  ## التقسيمات المدعومة
</div>

أنواع البيانات المدعومة في مفاتيح التقسيم في Paimon:

* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`