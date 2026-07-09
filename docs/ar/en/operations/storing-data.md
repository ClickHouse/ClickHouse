---
description: 'توثيق لـ highlight-next-line'
sidebar_label: 'الأقراص الخارجية لتخزين البيانات'
sidebar_position: 68
slug: /operations/storing-data
title: 'الأقراص الخارجية لتخزين البيانات'
doc_type: 'guide'
---

تُخزَّن البيانات التي تُعالَج في ClickHouse عادةً في نظام الملفات المحلي
للجهاز الذي يعمل عليه ClickHouse server. ويتطلّب ذلك أقراصًا ذات سعة كبيرة،
وقد تكون مكلفة. ولتجنّب تخزين البيانات محليًا، تتوفر خيارات تخزين متعددة:

1. تخزين الكائنات [Amazon S3](https://aws.amazon.com/s3/).
2. [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).
3. غير مدعوم: نظام ملفات Hadoop الموزع ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html))

<br />

:::note
يدعم ClickHouse أيضًا محركات الجداول الخارجية، وهي تختلف عن خيار التخزين الخارجي
الموصوف في هذه الصفحة، لأنها تتيح قراءة البيانات
المخزنة بتنسيقات ملفات عامة معيّنة (مثل Parquet). في هذه الصفحة نشرح
تهيئة التخزين لجداول عائلة `MergeTree` أو عائلة `Log` في ClickHouse.

1. للعمل مع البيانات المخزنة على أقراص `Amazon S3`، استخدم محرك الجداول [S3](/ar/engines/table-engines/integrations/s3.md).
2. للعمل مع البيانات المخزنة في Azure Blob Storage، استخدم محرك الجداول [AzureBlobStorage](/ar/engines/table-engines/integrations/azureBlobStorage.md).
3. للعمل مع البيانات في نظام ملفات Hadoop الموزع (غير مدعوم)، استخدم محرك الجداول [HDFS](/ar/engines/table-engines/integrations/hdfs.md).
   :::

<div id="configuring-external-storage">
  ## تكوين التخزين الخارجي
</div>

يمكن لمحركات الجداول من عائلة [`MergeTree`](/ar/engines/table-engines/mergetree-family/mergetree.md) و[`Log`](/ar/engines/table-engines/log-family/log.md)
تخزين البيانات في `S3` و`AzureBlobStorage` و`HDFS` (غير مدعوم) باستخدام قرص من الأنواع `s3`،
و`azure_blob_storage` و`hdfs` (غير مدعوم) على التوالي.

يتطلب تكوين القرص ما يلي:

1. قسم `type`، وتكون قيمته إحدى القيم `s3` أو `azure_blob_storage` أو `hdfs` (غير مدعوم) أو `local_blob_storage` أو `web`.
2. تكوين نوع محدد من أنواع التخزين الخارجي.

بدءًا من إصدار ClickHouse ‏24.1، أصبح من الممكن استخدام خيار تكوين جديد.
ويتطلب ذلك تحديد ما يلي:

1. قيمة `type` تساوي `object_storage`
2. `object_storage_type`، وتكون قيمته إحدى القيم `s3` أو `azure_blob_storage` (أو فقط `azure` بدءًا من `24.3`) أو `hdfs` (غير مدعوم) أو `local_blob_storage` (أو فقط `local` بدءًا من `24.3`) أو `web`.

<br />

اختياريًا، يمكن تحديد `metadata_type` (وتكون قيمته `local` افتراضيًا)، كما يمكن أيضًا ضبطه على `plain` أو `web`، وبدءًا من `24.4` على `plain_rewritable`.
ويُشرح استخدام نوع البيانات الوصفية `plain` في [قسم التخزين البسيط](/ar/operations/storing-data#plain-storage)، ولا يمكن استخدام نوع البيانات الوصفية `web` إلا مع نوع تخزين الكائنات `web`، أما نوع البيانات الوصفية `local` فيخزّن ملفات البيانات الوصفية محليًا (ويحتوي كل ملف بيانات وصفية على ربط بالملفات الموجودة في تخزين الكائنات، إلى جانب بعض المعلومات الوصفية الإضافية عنها).

على سبيل المثال:

```xml
<s3>
    <type>s3</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

يعادل الإعداد التالي (اعتبارًا من الإصدار `24.1`):

```xml
<s3>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>local</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

الإعداد التالي:

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

يساوي:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

فيما يلي مثال على تهيئة التخزين الكاملة:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

اعتبارًا من الإصدار 24.1، قد يبدو أيضًا على النحو التالي:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>local</metadata_type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

لجعل نوع تخزين معيّن الخيار الافتراضي لجميع جداول `MergeTree`،
أضِف القسم التالي إلى ملف الإعداد:

```xml
<clickhouse>
    <merge_tree>
        <storage_policy>s3</storage_policy>
    </merge_tree>
</clickhouse>
```

إذا كنت تريد تهيئة سياسة تخزين محددة لجدول معيّن،
يمكنك تحديدها في الإعدادات أثناء إنشاء الجدول:

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3';
```

يمكنك أيضًا استخدام `disk` بدلًا من `storage_policy`. في هذه الحالة، لا حاجة إلى
وجود قسم `storage_policy` في ملف الإعدادات، ويكفي قسم `disk`.

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = 's3';
```

<div id="refresh-parts-interval-and-table-disk">
  ## refresh_parts_interval and table_disk
</div>

هذا الإعداد مخصّص لجداول MergeTree غير المكرّرة، حيث قد تُكتَب أجزاء البيانات خارجيًا ويستلزم الأمر تحديث اكتشاف البيانات الوصفية من طبقة التخزين.

يتيح إعداد MergeTree ‏`refresh_parts_interval` التحديث الدوري لقائمة أجزاء البيانات من طبقة التخزين الأساسية (على سبيل المثال، لاكتشاف الأجزاء المكتوبة خارجيًا). والفرق المهم هنا هو بين **البيانات الوصفية المشتركة بين النسخ المتماثلة** و**البيانات الوصفية المحلية لكل نسخة متماثلة** (مثل S3 مع بيانات وصفية محلية لكل نسخة متماثلة): لا تصبح أجزاء البيانات الجديدة مرئية لجميع النسخ المتماثلة إلا إذا كانت البيانات الوصفية مشتركة. ولا يعني استخدام تخزين الكائنات وحده بالضرورة وجود بيانات وصفية مشتركة.

* **تخزين الكائنات (مثل `disk = 's3'`) لا يعني بالضرورة وجود بيانات وصفية مشتركة.** عندما تُخزَّن البيانات الوصفية محليًا لكل نسخة متماثلة (وهو السلوك الافتراضي)، تدير كل نسخة متماثلة بشكل مستقل مؤشرات `blobs` الخاصة بها في تخزين الكائنات. ولا تظهر التغييرات التي تُجرى على إحدى النسخ المتماثلة للنسخ الأخرى. وفي هذه الحالة، لن يجعل `refresh_parts_interval` أجزاء البيانات الجديدة مرئية عبر النسخ المتماثلة، لأن البيانات الوصفية التي تقرؤها كل نسخة متماثلة محلية لها.

* **يتطلب التحديث التلقائي لأجزاء البيانات أن تكون بيانات نظام الملفات الوصفية مشتركة** (أو أن يستخدم الجدول بيانات وصفية مملوكة للجدول وبوضع `readonly` بحيث يصبح التحديث قابلًا للتطبيق). ويُعد ضبط `table_disk = true` مع `disk` محلي على مستوى الجدول (مثل `SETTINGS disk = disk(type=object_storage, ...), table_disk = true`) إحدى الطرق للحصول على الدلالات الصحيحة: إذ يمتلك الجدول دورة حياة البيانات الوصفية، وتُعامَل وحدة التخزين على أنها `readonly`، لذلك يعمل `refresh_parts_interval` ويمكن اكتشاف الأجزاء المضافة خارجيًا.

* **عند استخدام `disk` معرّف على المستوى العام** (مثل `disk = 's3'` في `storage_configuration`) مع البيانات الوصفية المحلية الافتراضية، تكون لكل نسخة متماثلة حالتها الخاصة من البيانات الوصفية. وحتى إذا كانت `blobs` موجودة في S3، فلا تُعد وحدة التخزين مشتركة لأغراض `refresh_parts_interval`، ولن تُكتشف أجزاء البيانات الجديدة التي أُنشئت خارج ClickHouse أو على نسخة متماثلة أخرى.

للتحديث التلقائي لأجزاء البيانات، تأكد من أن البيانات الوصفية مشتركة، أو استخدم `disk` على مستوى الجدول مع `table_disk = true` كما هو موضح أعلاه. أما الاعتماد على `refresh_parts_interval` وحده مع بيانات وصفية محلية لكل نسخة متماثلة فلن يؤدي إلى تحديث أجزاء البيانات كما هو متوقع.

:::note
لا يُستخدم `refresh_parts_interval` مع جداول ReplicatedMergeTree.
فالجداول المكرّرة تزامن أجزاء البيانات بالفعل عبر آلية النسخ المتماثل.
ولا ينطبق هذا الإعداد إلا على جداول MergeTree غير المكرّرة التي تُكتَب أجزاء بياناتها خارجيًا ويستلزم الأمر فيها تحديث البيانات الوصفية.
:::

<div id="dynamic-configuration">
  ## التهيئة الديناميكية
</div>

يمكن أيضًا تحديد تهيئة التخزين من دون تعريف
قرص مسبقًا في ملف التهيئة، وذلك عبر
إعدادات استعلام `CREATE`/`ATTACH`.

يعتمد استعلام Example التالي على تهيئة القرص الديناميكية المذكورة أعلاه،
ويوضح كيفية استخدام قرص محلي للتخزين المؤقت لبيانات جدول مخزَّن على URL.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=web,
    endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
  );
  -- highlight-end
```

يوضح المثال أدناه إضافة ذاكرة تخزين مؤقت إلى التخزين الخارجي.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
-- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
-- highlight-end
```

في الإعدادات المميزة أدناه، لاحظ أن القرص ذي `type=web` متداخل ضمن
القرص ذي `type=cache`.

:::note
يستخدم المثال `type=web`، ولكن يمكن تهيئة أي نوع من الأقراص كتكوين ديناميكي،
بما في ذلك القرص المحلي. تتطلب الأقراص المحلية أن تكون وسيطة path ضمن
معلمة config الخاصة بالخادم `custom_local_disks_base_directory`، والتي لا
تملك قيمة افتراضية، لذا اضبطها أيضًا عند استخدام قرص محلي.
:::

من الممكن أيضًا الجمع بين تهيئة تستند إلى config وتهيئة معرّفة عبر SQL:

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  -- highlight-end
```

حيث إن `web` مأخوذ من ملف إعدادات الخادم:

```xml
<storage_configuration>
    <disks>
        <web>
            <type>web</type>
            <endpoint>'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'</endpoint>
        </web>
    </disks>
</storage_configuration>
```

<div id="s3-storage">
  ### استخدام مساحة تخزين S3
</div>

<div id="required-parameters-s3">
  #### المعلمات المطلوبة
</div>

| المعلمة             | الوصف                                                                                                                                                                                   |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint`          | URL نقطة نهاية S3 بنمطَي `path` أو `virtual hosted` [styles](https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html). يجب أن يتضمن الـ bucket ومسار الجذر لتخزين البيانات. |
| `access_key_id`     | معرّف مفتاح الوصول إلى S3 المستخدم للمصادقة.                                                                                                                                            |
| `secret_access_key` | مفتاح الوصول السري إلى S3 المستخدم للمصادقة.                                                                                                                                            |

<div id="optional-parameters-s3">
  #### المعلمات الاختيارية
</div>

| المعلمة                                                                                                  | الوصف                                                                                                                                                                                                                                                                                                    | القيمة الافتراضية                        |
| -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `region`                                                                                                 | اسم منطقة S3.                                                                                                                                                                                                                                                                                            | *                                        |
| `support_batch_delete`                                                                                   | يتحكم في ما إذا كان سيتم التحقق من دعم الحذف الدفعي. اضبطه على `false` عند استخدام Google Cloud Storage ‏(GCS)، لأن GCS لا يدعم الحذف الدفعي.                                                                                                                                                            | `true`                                   |
| `use_environment_credentials`                                                                            | يقرأ بيانات اعتماد AWS من متغيرات البيئة: `AWS_ACCESS_KEY_ID` و`AWS_SECRET_ACCESS_KEY` و`AWS_SESSION_TOKEN` إن كانت موجودة. ملاحظة: بيانات اعتماد البيئة مشتركة بين جميع أقراص S3. ولاستخدام بيانات اعتماد مختلفة لأقراص مختلفة، حدِّد `access_key_id` و`secret_access_key` صراحةً لكل قرص بدلًا من ذلك. | `false`                                  |
| `use_insecure_imds_request`                                                                              | إذا كانت القيمة `true`، يُستخدَم طلب IMDS غير آمن عند جلب بيانات الاعتماد من بيانات Amazon EC2 الوصفية.                                                                                                                                                                                                  | `false`                                  |
| `expiration_window_seconds`                                                                              | فترة سماح (بالثواني) للتحقق مما إذا كانت بيانات الاعتماد ذات تاريخ انتهاء الصلاحية قد انتهت صلاحيتها.                                                                                                                                                                                                    | `120`                                    |
| `proxy`                                                                                                  | إعداد الوكيل لنقطة نهاية S3. يجب أن يتضمن كل عنصر `uri` داخل كتلة `proxy` عنوان URL لوكيل.                                                                                                                                                                                                               | -                                        |
| `connect_timeout_ms`                                                                                     | مهلة الاتصال بالمقبس بالمللي ثانية.                                                                                                                                                                                                                                                                      | `10000` (10 ثوانٍ)                       |
| `request_timeout_ms`                                                                                     | مهلة الطلب بالمللي ثانية.                                                                                                                                                                                                                                                                                | `5000` (5 ثوانٍ)                         |
| `retry_attempts`                                                                                         | عدد محاولات إعادة المحاولة للطلبات التي فشلت.                                                                                                                                                                                                                                                            | `10`                                     |
| `single_read_retries`                                                                                    | عدد محاولات إعادة المحاولة عند انقطاع الاتصال أثناء القراءة.                                                                                                                                                                                                                                             | `4`                                      |
| `min_bytes_for_seek`                                                                                     | الحد الأدنى لعدد البايتات اللازم لاستخدام عملية seek بدلًا من القراءة التسلسلية.                                                                                                                                                                                                                         | `1 MB`                                   |
| `metadata_path`                                                                                          | مسار نظام الملفات المحلي لتخزين ملفات البيانات الوصفية الخاصة بـ S3.                                                                                                                                                                                                                                     | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`                                                                                      | إذا كانت القيمة `true`، يتم تخطي عمليات التحقق من الوصول إلى القرص عند بدء التشغيل.                                                                                                                                                                                                                      | `false`                                  |
| `header`                                                                                                 | يضيف ترويسة HTTP المحددة إلى الطلبات. ويمكن تحديدها عدة مرات.                                                                                                                                                                                                                                            | *                                        |
| `server_side_encryption_customer_key_base64`                                                             | الرؤوس المطلوبة للوصول إلى كائنات S3 المشفّرة باستخدام SSE-C.                                                                                                                                                                                                                                            | -                                        |
| `server_side_encryption_kms_key_id`                                                                      | الترويسات المطلوبة للوصول إلى كائنات S3 باستخدام [تشفير SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html). تستخدم السلسلة الفارغة مفتاح S3 المُدار من قِبل AWS.                                                                                                    | *                                        |
| `server_side_encryption_kms_encryption_context`                                                          | ترويسة سياق التشفير الخاصة بـ SSE-KMS (تُستخدم مع `server_side_encryption_kms_key_id`).                                                                                                                                                                                                                  | -                                        |
| `server_side_encryption_kms_bucket_key_enabled`                                                          | يُفعّل مفاتيح حاوية S3 لـ SSE-KMS (تُستخدم مع `server_side_encryption_kms_key_id`).                                                                                                                                                                                                                      | يطابق الإعداد على مستوى الـbucket        |
| `s3_max_put_rps`                                                                                         | الحد الأقصى لطلبات PUT في الثانية قبل بدء throttling.                                                                                                                                                                                                                                                    | `0` (غير محدود)                          |
| `s3_max_put_burst`                                                                                       | الحد الأقصى لطلبات PUT المتزامنة قبل الوصول إلى حد RPS.                                                                                                                                                                                                                                                  | مثل `s3_max_put_rps`                     |
| `s3_max_get_rps`                                                                                         | الحد الأقصى لطلبات GET في الثانية قبل بدء throttling.                                                                                                                                                                                                                                                    | `0` (بدون حد)                            |
| `s3_max_get_burst`                                                                                       | الحد الأقصى لطلبات GET المتزامنة قبل الوصول إلى حد RPS.                                                                                                                                                                                                                                                  | مثل `s3_max_get_rps`                     |
| `read_resource`                                                                                          | اسم المورد المستخدم في [جدولة](/ar/operations/workload-scheduling.md) طلبات القراءة.                                                                                                                                                                                                                        | String فارغ (معطّل)                      |
| `write_resource`                                                                                         | اسم المورد المستخدَم في [جدولة](/ar/operations/workload-scheduling.md) طلبات الكتابة.                                                                                                                                                                                                                       | String فارغ (معطّل)                      |
| `key_template`                                                                                           | يحدّد صيغة توليد مفتاح الكائن باستخدام صياغة [re2](https://github.com/google/re2/wiki/Syntax). يتطلب الخيار `storage_metadata_write_full_object_key`. غير متوافق مع `root path` في `endpoint`. ويتطلب `key_compatibility_prefix`.                                                                        | *                                        |
| `key_compatibility_prefix`                                                                               | مطلوب مع `key_template`. يحدّد قيمة `root path` السابقة ضمن `endpoint` لقراءة الإصدارات الأقدم من البيانات الوصفية.                                                                                                                                                                                      | -                                        |
| `read_only`                                                                                              | يسمح بالقراءة من وحدة التخزين فقط.                                                                                                                                                                                                                                                                       | *                                        |
| :::note                                                                                                  |                                                                                                                                                                                                                                                                                                          |                                          |
| يُدعَم أيضًا Google Cloud Storage ‏(GCS) عبر النوع `s3`. راجع [GCS backed MergeTree](/ar/integrations/gcs). |                                                                                                                                                                                                                                                                                                          |                                          |
| :::                                                                                                      |                                                                                                                                                                                                                                                                                                          |                                          |

<div id="plain-storage">
  ### استخدام التخزين البسيط
</div>

في الإصدار `22.10`، أُضيف نوع قرص جديد هو `s3_plain`، ويوفّر تخزينًا للكتابة مرة واحدة.
ومَعلمات إعداده هي نفسها مَعلمات نوع القرص `s3`.
لكن بخلاف نوع القرص `s3`، فإنه يخزّن البيانات كما هي. وبعبارة أخرى،
فبدلًا من استخدام أسماء blob مُولَّدة عشوائيًا، يستخدم أسماء ملفات عادية
(بالطريقة نفسها التي يخزّن بها ClickHouse الملفات على قرص محلي)، ولا يخزّن أي
metadata محليًا. فعلى سبيل المثال، تُستمد من البيانات الموجودة على `s3`.

يتيح نوع القرص هذا الاحتفاظ بنسخة ثابتة من الجدول، لأنه لا
يسمح بتنفيذ merges على البيانات الموجودة ولا يسمح بإدراج بيانات
جديدة. ومن حالات الاستخدام لهذا النوع من الأقراص إنشاء backups عليه، ويمكن تنفيذ ذلك
عبر `BACKUP TABLE data TO Disk('plain_disk_name', 'backup_name')`. وبعد ذلك،
يمكنك تنفيذ `RESTORE TABLE data AS data_restored FROM Disk('plain_disk_name', 'backup_name')`
أو استخدام `ATTACH TABLE data (...) ENGINE = MergeTree() SETTINGS disk = 'plain_disk_name'`.

الإعداد:

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

بدءًا من `24.1`، أصبح من الممكن تهيئة أي قرص تخزين الكائنات (`s3`, `azure`, `hdfs` (غير مدعوم), `local`) باستخدام
نوع البيانات الوصفية `plain`.

التهيئة:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>azure</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

<div id="s3-plain-rewritable-storage">
  ### استخدام تخزين S3 البسيط القابل لإعادة الكتابة
</div>

تم تقديم نوع القرص الجديد `s3_plain_rewritable` في `24.4`.
وعلى غرار نوع القرص `s3_plain`، فإنه لا يتطلب مساحة تخزين إضافية لملفات
البيانات الوصفية. وبدلًا من ذلك، تُخزَّن البيانات الوصفية في S3.
وعلى خلاف نوع القرص `s3_plain`، يتيح `s3_plain_rewritable` تنفيذ عمليات الدمج
ويدعم عمليات `INSERT`.
أما [mutations](/ar/sql-reference/statements/alter#mutations) والنسخ المتماثل للجداول فغير مدعومين.

تتمثل إحدى حالات الاستخدام لهذا النوع من الأقراص في جداول `MergeTree` غير المكررة. وعلى الرغم من أن
نوع القرص `s3` مناسب لجداول `MergeTree` غير المكررة، فقد تختار
نوع القرص `s3_plain_rewritable` إذا كنت لا تحتاج إلى بيانات وصفية محلية
للجدول، وكنت مستعدًا لقبول مجموعة محدودة من العمليات. وقد يكون ذلك
مفيدًا، على سبيل المثال، للجداول النظامية.

التكوين:

```xml
<s3_plain_rewritable>
    <type>s3_plain_rewritable</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

يساوي

```xml
<s3_plain_rewritable>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain_rewritable</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

اعتبارًا من `24.5`، أصبح من الممكن تهيئة أي قرص تخزين كائني
(`s3`, `azure`, `local`) باستخدام نوع البيانات الوصفية `plain_rewritable`.

<div id="azure-blob-storage">
  ### استخدام Azure Blob Storage
</div>

يمكن لمحركات جداول عائلة `MergeTree` تخزين البيانات في [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/)
باستخدام قرص من النوع `azure_blob_storage`.

صيغة التهيئة:

```xml
<storage_configuration>
    ...
    <disks>
        <blob_storage_disk>
            <type>azure_blob_storage</type>
            <storage_account_url>http://account.blob.core.windows.net</storage_account_url>
            <container_name>container</container_name>
            <account_name>account</account_name>
            <account_key>pass123</account_key>
            <metadata_path>/var/lib/clickhouse/disks/blob_storage_disk/</metadata_path>
            <cache_path>/var/lib/clickhouse/disks/blob_storage_disk/cache/</cache_path>
            <skip_access_check>false</skip_access_check>
        </blob_storage_disk>
    </disks>
    ...
</storage_configuration>
```

<div id="azure-blob-storage-connection-parameters">
  #### معلمات الاتصال
</div>

| المعلمة                       | الوصف                                                                                                                                                                                | القيمة الافتراضية   |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------- |
| `storage_account_url` (مطلوب) | عنوان URL لحساب Azure Blob Storage. أمثلة: `http://account.blob.core.windows.net` أو `http://azurite1:10000/devstoreaccount1`.                                                       | -                   |
| `container_name`              | اسم الحاوية المستهدفة.                                                                                                                                                               | `default-container` |
| `container_already_exists`    | يتحكم في سلوك إنشاء الحاوية: <br />- `false`: ينشئ حاوية جديدة <br />- `true`: يتصل مباشرةً بحاوية موجودة <br />- غير معيّن: يتحقق مما إذا كانت الحاوية موجودة، ثم ينشئها عند الحاجة | -                   |

معلمات المصادقة (سيحاول القرص جميع طرق المصادقة المتاحة **و** Managed Identity Credential):

| المعلمة             | الوصف                                                     |
| ------------------- | --------------------------------------------------------- |
| `connection_string` | للمصادقة باستخدام سلسلة الاتصال.                          |
| `account_name`      | للمصادقة باستخدام Shared Key (يُستخدم مع `account_key`).  |
| `account_key`       | للمصادقة باستخدام Shared Key (يُستخدم مع `account_name`). |

<div id="azure-blob-storage-limit-parameters">
  #### معلمات الحدود
</div>

| المعلمة                              | الوصف                                                                  |
| ------------------------------------ | ---------------------------------------------------------------------- |
| `s3_max_single_part_upload_size`     | الحد الأقصى لحجم رفع كتلة واحدة إلى Blob Storage.                      |
| `min_bytes_for_seek`                 | الحد الأدنى لحجم نطاق يدعم `seek`.                                     |
| `max_single_read_retries`            | الحد الأقصى لعدد محاولات قراءة جزء من البيانات من Blob Storage.        |
| `max_single_download_retries`        | الحد الأقصى لعدد محاولات تنزيل مخزن مؤقت قابل للقراءة من Blob Storage. |
| `thread_pool_size`                   | الحد الأقصى لعدد الخيوط المستخدمة في إنشاء `IDiskRemote`.              |
| `s3_max_inflight_parts_for_one_file` | الحد الأقصى لعدد طلبات PUT المتزامنة لكائن واحد.                       |

<div id="azure-blob-storage-other-parameters">
  #### معلمات أخرى
</div>

| المعلمة                          | الوصف                                                                              | القيمة الافتراضية                        |
| -------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------- |
| `metadata_path`                  | مسار نظام الملفات المحلي لتخزين ملف البيانات الوصفية لـ Blob Storage.              | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`              | إذا كانت `true`، يتم تخطي فحوصات الوصول إلى القرص أثناء بدء التشغيل.               | `false`                                  |
| `read_resource`                  | اسم المورد لطلبات القراءة الخاصة بـ [الجدولة](/ar/operations/workload-scheduling.md). | سلسلة فارغة (معطّل)                      |
| `write_resource`                 | اسم المورد لطلبات الكتابة الخاصة بـ [الجدولة](/ar/operations/workload-scheduling.md). | سلسلة فارغة (معطّل)                      |
| `metadata_keep_free_space_bytes` | مقدار المساحة الحرة التي يجب حجزها على قرص البيانات الوصفية.                       | -                                        |

يمكن العثور على أمثلة على إعدادات عاملة في دليل اختبارات التكامل (راجع مثلًا [test&#95;merge&#95;tree&#95;azure&#95;blob&#95;storage](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_merge_tree_azure_blob_storage/configs/config.d/storage_conf.xml) أو [test&#95;azure&#95;blob&#95;storage&#95;zero&#95;copy&#95;replication](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_azure_blob_storage_zero_copy_replication/configs/config.d/storage_conf.xml)).

:::note النسخ المتماثل دون نسخ غير جاهز لبيئة الإنتاج
يكون النسخ المتماثل دون نسخ معطّلًا افتراضيًا في ClickHouse الإصدار 22.8 وما بعده. لا يُنصح باستخدام هذه الميزة في بيئة الإنتاج.
:::

<div id="using-hdfs-storage-unsupported">
  ## استخدام تخزين HDFS (غير مدعوم)
</div>

في مثال التهيئة هذا:

* القرص من النوع `hdfs` (غير مدعوم)
* تتم استضافة البيانات على `hdfs://hdfs1:9000/clickhouse/`

بالمناسبة، HDFS غير مدعوم، لذا قد تواجه بعض المشكلات عند استخدامه. لا تتردد في تقديم pull request يتضمن الإصلاح إذا ظهرت أي مشكلة.

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <hdfs>
                <type>hdfs</type>
                <endpoint>hdfs://hdfs1:9000/clickhouse/</endpoint>
                <skip_access_check>true</skip_access_check>
            </hdfs>
            <hdd>
                <type>local</type>
                <path>/</path>
            </hdd>
        </disks>
        <policies>
            <hdfs>
                <volumes>
                    <main>
                        <disk>hdfs</disk>
                    </main>
                    <external>
                        <disk>hdd</disk>
                    </external>
                </volumes>
            </hdfs>
        </policies>
    </storage_configuration>
</clickhouse>
```

ضع في اعتبارك أن HDFS قد لا يعمل في بعض الحالات النادرة.

<div id="encrypted-virtual-file-system">
  ### استخدام تشفير البيانات
</div>

يمكنك تشفير البيانات المخزّنة على الأقراص الخارجية [S3](/ar/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3) أو [HDFS](#using-hdfs-storage-unsupported) (غير مدعوم)، أو على قرص محلي. لتفعيل وضع التشفير، يجب تعريف قرص من النوع `encrypted` في ملف الإعدادات، ثم اختيار القرص الذي ستُحفَظ عليه البيانات. يقوم القرص `encrypted` بتشفير جميع الملفات المكتوبة تلقائيًا أثناء الكتابة، وعند قراءة الملفات من قرص `encrypted` يفك تشفيرها تلقائيًا. لذلك يمكنك التعامل مع قرص `encrypted` كما تتعامل مع قرص عادي.

مثال على إعدادات القرص:

```xml
<disks>
  <disk1>
    <type>local</type>
    <path>/path1/</path>
  </disk1>
  <disk2>
    <type>encrypted</type>
    <disk>disk1</disk>
    <path>path2/</path>
    <key>_16_ascii_chars_</key>
  </disk2>
</disks>
```

على سبيل المثال، عندما يكتب ClickHouse بيانات أحد الجداول إلى الملف `store/all_1_1_0/data.bin` على `disk1`، فإن هذا الملف يُكتب فعليًا على القرص الفعلي عند المسار `/path1/store/all_1_1_0/data.bin`.

وعند كتابة الملف نفسه على `disk2`، فإنه يُكتب فعليًا على القرص الفعلي عند المسار `/path1/path2/store/all_1_1_0/data.bin` بوضع مشفّر.

<div id="required-parameters-encrypted-disk">
  ### المعلمات المطلوبة
</div>

| المعلمة | النوع  | الوصف                                                                                                                     |
| ------- | ------ | ------------------------------------------------------------------------------------------------------------------------- |
| `type`  | String | يجب ضبطه على `encrypted` لإنشاء قرص مشفّر.                                                                                |
| `disk`  | String | نوع القرص المستخدم للتخزين الأساسي.                                                                                       |
| `key`   | Uint64 | مفتاح للتشفير وفك التشفير. يمكن تحديده بصيغة سداسية عشرية باستخدام `key_hex`. ويمكن تحديد عدة مفاتيح باستخدام السمة `id`. |

<div id="optional-parameters-encrypted-disk">
  ### المعلمات الاختيارية
</div>

| المعلمة          | النوع  | الافتراضي     | الوصف                                                                                                                                                        |
| ---------------- | ------ | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `path`           | String | الدليل الجذر  | الموقع على القرص الذي ستُحفَظ فيه البيانات.                                                                                                                  |
| `current_key_id` | String | -             | معرّف المفتاح المستخدم للتشفير. ويمكن استخدام جميع المفاتيح المحددة لفك التشفير.                                                                             |
| `algorithm`      | Enum   | `AES_128_CTR` | خوارزمية التشفير. الخيارات: <br />- `AES_128_CTR` (مفتاح بطول 16 بايت) <br />- `AES_192_CTR` (مفتاح بطول 24 بايت) <br />- `AES_256_CTR` (مفتاح بطول 32 بايت) |

مثال على تكوين القرص:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <disk_s3>
                <type>s3</type>
                <endpoint>...
            </disk_s3>
            <disk_s3_encrypted>
                <type>encrypted</type>
                <disk>disk_s3</disk>
                <algorithm>AES_128_CTR</algorithm>
                <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
                <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
        </disks>
    </storage_configuration>
</clickhouse>
```

<div id="using-local-cache">
  ### استخدام ذاكرة التخزين المؤقت المحلية
</div>

يمكن تهيئة ذاكرة تخزين مؤقت محلية فوق الأقراص ضمن إعدادات التخزين بدءًا من الإصدار 22.3.
بالنسبة إلى الإصدارات 22.3 - 22.7، لا تكون ذاكرة التخزين المؤقت مدعومة إلا لنوع القرص `s3`. وبالنسبة إلى الإصدارات &gt;= 22.8، تكون ذاكرة التخزين المؤقت مدعومة لأي نوع قرص: S3 وAzure وLocal وEncrypted وما إلى ذلك.
بالنسبة إلى الإصدارات &gt;= 23.5، لا تكون ذاكرة التخزين المؤقت مدعومة إلا لأنواع الأقراص البعيدة: S3 وAzure وHDFS (غير مدعوم).
تستخدم ذاكرة التخزين المؤقت سياسة `LRU`.

مثال على الإعداد للإصدارات 22.8 وما بعدها:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
            </s3>
            <cache>
                <type>cache</type>
                <disk>s3</disk>
                <path>/s3_cache/</path>
                <max_size>10Gi</max_size>
            </cache>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>cache</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

مثال على تكوين للإصدارات الأقدم من 22.8:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
                <data_cache_enabled>1</data_cache_enabled>
                <data_cache_max_size>10737418240</data_cache_max_size>
            </s3>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

إعدادات **تكوين القرص** في File Cache:

يجب تعريف هذه الإعدادات في قسم تكوين القرص.

| Parameter                             | Type    | Default    | Description                                                                                                                                                                                |
| ------------------------------------- | ------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `path`                                | String  | -          | **مطلوب**. المسار إلى الدليل الذي سيُخزَّن فيه التخزين المؤقت.                                                                                                                             |
| `max_size`                            | Size    | -          | **مطلوب**. الحد الأقصى لحجم التخزين المؤقت بالبايت أو بصيغة مقروءة (مثل `10Gi`). تُزال الملفات وفق سياسة LRU عند الوصول إلى هذا الحد. ويدعم تنسيقات `ki` و`Mi` و`Gi` (منذ الإصدار v22.10). |
| `cache_on_write_operations`           | Boolean | `false`    | يفعّل التخزين المؤقت بالكتابة المباشرة لاستعلامات `INSERT` وعمليات الدمج في الخلفية. ويمكن تجاوز هذا الإعداد لكل query على حدة باستخدام `enable_filesystem_cache_on_write_operations`.     |
| `enable_filesystem_query_cache_limit` | Boolean | `false`    | يفعّل حدود حجم التخزين المؤقت لكل query استنادًا إلى `max_query_cache_size`.                                                                                                               |
| `enable_cache_hits_threshold`         | Boolean | `false`    | عند تفعيله، لا تُخزَّن البيانات مؤقتًا إلا بعد قراءتها عدة مرات.                                                                                                                           |
| `cache_hits_threshold`                | Integer | `0`        | عدد عمليات القراءة المطلوبة قبل تخزين البيانات مؤقتًا (يتطلب `enable_cache_hits_threshold`).                                                                                               |
| `enable_bypass_cache_with_threshold`  | Boolean | `false`    | يتخطّى التخزين المؤقت لنطاقات القراءة الكبيرة.                                                                                                                                             |
| `bypass_cache_threshold`              | Size    | `256Mi`    | حجم نطاق القراءة الذي يؤدي إلى تخطي التخزين المؤقت (يتطلب `enable_bypass_cache_with_threshold`).                                                                                           |
| `max_file_segment_size`               | Size    | `8Mi`      | الحد الأقصى لحجم ملف تخزين مؤقت واحد بالبايت أو بصيغة مقروءة.                                                                                                                              |
| `max_elements`                        | Integer | `10000000` | الحد الأقصى لعدد ملفات التخزين المؤقت.                                                                                                                                                     |
| `load_metadata_threads`               | Integer | `16`       | عدد مؤشرات الترابط المستخدمة لتحميل البيانات الوصفية للتخزين المؤقت عند بدء التشغيل.                                                                                                       |
| `use_split_cache`                     | Boolean | `false`    | يستخدم فصل الملفات إلى ملفات النظام والبيانات.                                                                                                                                             |
| `split_cache_ratio`                   | Double  | `0.1`      | نسبة الجزء الخاص بالنظام إلى الحجم الإجمالي للتخزين المؤقت في split&#95;cache.                                                                                                             |

> **ملاحظة**: تدعم قيم الحجم وحدات مثل `ki` و`Mi` و`Gi` وغيرها (مثل `10Gi`).

<div id="file-cache-query-profile-settings">
  ## إعدادات الاستعلام/ملف التعريف لـ File Cache
</div>

| الإعداد                                                                 | النوع   | الافتراضي               | الوصف                                                                                                                                                              |
| ----------------------------------------------------------------------- | ------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `enable_filesystem_cache`                                               | Boolean | `true`                  | يفعّل/يعطّل استخدام ذاكرة التخزين المؤقت لكل query، حتى عند استخدام نوع قرص هو `cache`.                                                                           |
| `read_from_filesystem_cache_if_exists_otherwise_bypass_cache`           | Boolean | `false`                 | عند تفعيله، يستخدم ذاكرة التخزين المؤقت فقط إذا كانت البيانات موجودة؛ أما البيانات الجديدة فلن تُخزَّن فيها.                                                       |
| `enable_filesystem_cache_on_write_operations`                           | Boolean | `false` (Cloud: `true`) | يفعّل التخزين المؤقت بنمط write-through. ويتطلب `cache_on_write_operations` في إعدادات `cache`.                                                                    |
| `enable_filesystem_cache_log`                                           | Boolean | `false`                 | يفعّل تسجيلًا تفصيليًا لاستخدام ذاكرة التخزين المؤقت في `system.filesystem_cache_log`.                                                                             |
| `filesystem_cache_allow_background_download`                            | Boolean | `true`                  | يسمح بإكمال segments التي نُزِّلت جزئيًا في الخلفية. عطّله لإبقاء التنزيلات في المقدّمة ضمن query/session الحالية.                                                 |
| `max_query_cache_size`                                                  | الحجم   | `false`                 | الحد الأقصى لحجم ذاكرة التخزين المؤقت لكل query. ويتطلب `enable_filesystem_query_cache_limit` في إعدادات `cache`.                                                  |
| `filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit` | Boolean | `true`                  | يتحكم في السلوك عند بلوغ `max_query_cache_size`: <br />- `true`: يوقف تنزيل البيانات الجديدة <br />- `false`: يزيل البيانات القديمة لإفساح المجال للبيانات الجديدة |

:::warning
تتوافق إعدادات تهيئة `cache` وإعدادات query الخاصة بها مع أحدث إصدار من ClickHouse،
أما في الإصدارات الأقدم فقد لا تكون بعض الميزات مدعومة.
:::

<div id="cache-system-tables-file-cache">
  #### جداول النظام الخاصة بذاكرة التخزين المؤقت
</div>

| اسم الجدول                    | الوصف                                                            | المتطلبات                                  |
| ----------------------------- | ---------------------------------------------------------------- | ------------------------------------------ |
| `system.filesystem_cache`     | يعرض الحالة الحالية لذاكرة التخزين المؤقت لنظام الملفات.         | لا توجد                                    |
| `system.filesystem_cache_log` | يوفّر إحصاءات مفصلة عن استخدام ذاكرة التخزين المؤقت لكل استعلام. | يتطلب `enable_filesystem_cache_log = true` |

<div id="cache-commands-file-cache">
  #### أوامر التخزين المؤقت
</div>

<div id="system-clear-filesystem-cache-on-cluster">
  ##### `SYSTEM CLEAR|DROP FILESYSTEM CACHE (<cache_name>) (ON CLUSTER)` -- `ON CLUSTER`
</div>

لا يكون هذا الأمر مدعومًا إلا عند عدم تحديد `<cache_name>`

<div id="show-filesystem-caches">
  ##### `SHOW FILESYSTEM CACHES`
</div>

يعرض قائمة بذاكرات التخزين المؤقت لنظام الملفات التي جرى تهيئتها على الخادم.
(في الإصدارات الأقدم من `22.8` أو المساوية له، يكون اسم الأمر `SHOW CACHES`)

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="describe-filesystem-cache">
  ##### `DESCRIBE FILESYSTEM CACHE '<cache_name>'`
</div>

اعرض تهيئة ذاكرة التخزين المؤقت لنظام الملفات وبعض الإحصاءات العامة لذاكرة تخزين مؤقت معيّنة.
يمكن أخذ اسم ذاكرة التخزين المؤقت من الأمر `SHOW FILESYSTEM CACHES`. (في الإصدارات الأقدم
من `22.8` أو المساوية له، يكون اسم الأمر `DESCRIBE CACHE`)

```sql title="Query"
DESCRIBE FILESYSTEM CACHE 's3_cache'
```

```text title="Response"
┌────max_size─┬─max_elements─┬─max_file_segment_size─┬─boundary_alignment─┬─cache_on_write_operations─┬─cache_hits_threshold─┬─current_size─┬─current_elements─┬─path───────┬─background_download_threads─┬─enable_bypass_cache_with_threshold─┐
│ 10000000000 │      1048576 │             104857600 │            4194304 │                         1 │                    0 │         3276 │               54 │ /s3_cache/ │                           2 │                                  0 │
└─────────────┴──────────────┴───────────────────────┴────────────────────┴───────────────────────────┴──────────────────────┴──────────────┴──────────────────┴────────────┴─────────────────────────────┴────────────────────────────────────┘
```

| مقاييس التخزين المؤقت الحالية | مقاييس التخزين المؤقت غير المتزامنة | أحداث profile للتخزين المؤقت                                                              |
| ----------------------------- | ----------------------------------- | ----------------------------------------------------------------------------------------- |
| `FilesystemCacheSize`         | `FilesystemCacheBytes`              | `CachedReadBufferReadFromSourceBytes`, `CachedReadBufferReadFromCacheBytes`               |
| `FilesystemCacheElements`     | `FilesystemCacheFiles`              | `CachedReadBufferReadFromSourceMicroseconds`, `CachedReadBufferReadFromCacheMicroseconds` |
|                               |                                     | `CachedReadBufferCacheWriteBytes`, `CachedReadBufferCacheWriteMicroseconds`               |
|                               |                                     | `CachedWriteBufferCacheWriteBytes`, `CachedWriteBufferCacheWriteMicroseconds`             |

<div id="web-storage">
  ### استخدام تخزين الويب الثابت (للقراءة فقط)
</div>

هذا `قرص` مخصص للقراءة فقط. لا تُقرأ بياناته إلا دون أن تُعدَّل مطلقًا. يُحمَّل `table` جديد
على هذا `قرص` عبر `ATTACH TABLE` `query` (انظر المثال أدناه). ولا يُستخدم `قرص` المحلي
فعليًا، إذ تؤدي كل `SELECT` `query` إلى إرسال `http` `request` لجلب
البيانات المطلوبة. وأي تعديل على بيانات `table` سيؤدي إلى
`exception`، أي إن الأنواع التالية من `queries` غير مسموح بها: [`CREATE TABLE`](/ar/sql-reference/statements/create/table.md),
[`ALTER TABLE`](/ar/sql-reference/statements/alter/index.md), [`RENAME TABLE`](/ar/sql-reference/statements/rename#rename-table),
[`DETACH TABLE`](/ar/sql-reference/statements/detach.md) و[`TRUNCATE TABLE`](/ar/sql-reference/statements/truncate.md).
يمكن استخدام تخزين الويب لأغراض القراءة فقط. ومن أمثلة الاستخدام استضافة
بيانات `sample` أو ترحيل البيانات. توجد أداة باسم `clickhouse-static-files-uploader`
تُعِد `directory` بيانات لـ `table` معيّن (`SELECT data_paths FROM system.tables WHERE name = 'table_name'`).
وبالنسبة إلى كل `table` تحتاجه، ستحصل على `directory` من الملفات. ويمكن رفع هذه الملفات
إلى خادم ويب يستضيف ملفات ثابتة، على سبيل المثال. وبعد هذا الإعداد،
يمكنك تحميل هذا `table` إلى أي `ClickHouse server` عبر `DiskWeb`.

في نموذج `configuration` هذا:

* `قرص` من النوع `web`
* تُستضاف البيانات على `http://nginx:80/test1/`
* يُستخدم `cache` على `storage` المحلي

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>http://nginx:80/test1/</endpoint>
            </web>
            <cached_web>
                <type>cache</type>
                <disk>web</disk>
                <path>cached_web_cache/</path>
                <max_size>100000000</max_size>
            </cached_web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
            <cached_web>
                <volumes>
                    <main>
                        <disk>cached_web</disk>
                    </main>
                </volumes>
            </cached_web>
        </policies>
    </storage_configuration>
</clickhouse>
```

:::tip
يمكن أيضًا إعداد التخزين مؤقتًا داخل الاستعلام إذا لم يكن من المتوقع استخدام مجموعة بيانات على الويب
بشكل اعتيادي؛ راجع [التهيئة الديناميكية](#dynamic-configuration) وتخطَّ
تحرير ملف الإعدادات.

تُستضاف [مجموعة بيانات تجريبية](https://github.com/ClickHouse/web-tables-demo) على GitHub.  ولإعداد جداولك الخاصة للتخزين على الويب،
راجِع الأداة [clickhouse-static-files-uploader](/ar/operations/utilities/static-files-disk-uploader)
:::

في استعلام `ATTACH TABLE` هذا، يطابق `UUID` المقدَّم اسم دليل البيانات، وتكون نقطة النهاية هي عنوان URL للمحتوى الخام على GitHub.

```sql
-- highlight-next-line
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      );
  -- highlight-end
```

حالة اختبار جاهزة. عليك إضافة هذا الإعداد إلى config:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>https://clickhouse-datasets.s3.yandex.net/disk-with-static-files-tests/test-hits/</endpoint>
            </web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
        </policies>
    </storage_configuration>
</clickhouse>
```

ثم نفِّذ هذا الاستعلام:

```sql
ATTACH TABLE test_hits UUID '1ae36516-d62d-4218-9ae3-6516d62da218'
(
    WatchID UInt64,
    JavaEnable UInt8,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RegionID UInt32,
    UserID UInt64,
    CounterClass Int8,
    OS UInt8,
    UserAgent UInt8,
    URL String,
    Referer String,
    URLDomain String,
    RefererDomain String,
    Refresh UInt8,
    IsRobot UInt8,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    FlashMinor2 String,
    NetMajor UInt8,
    NetMinor UInt8,
    UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    MobilePhone UInt8,
    MobilePhoneModel String,
    Params String,
    IPNetworkID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    IsArtifical UInt8,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,
    SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,
    PageCharset String,
    CodeVersion UInt32,
    IsLink UInt8,
    IsDownload UInt8,
    IsNotBounce UInt8,
    FUniqID UInt64,
    HID UInt32,
    IsOldCounter UInt8,
    IsEvent UInt8,
    IsParameter UInt8,
    DontCountHits UInt8,
    WithHash UInt8,
    HitColor FixedString(1),
    UTCEventTime DateTime,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),
    SocialNetwork String,
    SocialAction String,
    HTTPError UInt16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    RedirectTiming Int32,
    DOMInteractiveTiming Int32,
    DOMContentLoadedTiming Int32,
    DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,
    LoadEventEndTiming Int32,
    NSToDOMContentLoadedTiming Int32,
    FirstPaintTiming Int32,
    RedirectCount Int8,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    GoalsReached Array(UInt32),
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    RefererHash UInt64,
    URLHash UInt64,
    CLID UInt32,
    YCLID UInt64,
    ShareService String,
    ShareURL String,
    ShareTitle String,
    ParsedParams Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    IslandID FixedString(16),
    RequestNum UInt32,
    RequestTry UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy='web';
```

<div id="required-parameters-s3">
  #### المعلمات المطلوبة
</div>

| المعلمة    | الوصف                                                                                                           |
| ---------- | --------------------------------------------------------------------------------------------------------------- |
| `type`     | `web`، وإلا فلن يتم إنشاء القرص.                                                                         |
| `endpoint` | عنوان URL لنقطة النهاية بتنسيق `path`. ويجب أن يتضمن عنوان URL لنقطة النهاية مسار جذر لتخزين البيانات المرفوعة. |

<div id="optional-parameters-s3">
  #### المعلمات الاختيارية
</div>

| المعلمة                             | الوصف                                                                    | القيمة الافتراضية |
| ----------------------------------- | ------------------------------------------------------------------------ | ----------------- |
| `min_bytes_for_seek`                | الحد الأدنى لعدد البايتات لاستخدام عملية seek بدلًا من القراءة التسلسلية | `1` MB            |
| `remote_fs_read_backoff_threashold` | الحد الأقصى لوقت الانتظار عند محاولة قراءة البيانات من قرص بعيد          | `10000` ثانية     |
| `remote_fs_read_backoff_max_tries`  | الحد الأقصى لعدد محاولات القراءة باستخدام backoff                        | `5`               |

إذا فشل الاستعلام مع الاستثناء `DB:Exception Unreachable URL`، فيمكنك محاولة ضبط الإعدادات التالية: [http&#95;connection&#95;timeout](/ar/operations/settings/settings.md/#http_connection_timeout)، و[http&#95;receive&#95;timeout](/ar/operations/settings/settings.md/#http_receive_timeout)، و[keep&#95;alive&#95;timeout](/ar/operations/server-configuration-parameters/settings#keep_alive_timeout).

للحصول على ملفات من أجل الرفع، شغّل:
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>` (يمكن العثور على `--metadata-path` في الاستعلام `SELECT data_paths FROM system.tables WHERE name = 'table_name'`).

عند تحميل الملفات بواسطة `endpoint`، يجب تحميلها إلى المسار `<endpoint>/store/`، لكن يجب أن يحتوي config على `endpoint` فقط.

إذا كان URL غير متاح عند تحميل القرص أثناء بدء الخادم للجداول، فسيتم التقاط جميع الأخطاء. وإذا حدثت أخطاء في هذه الحالة، يمكن إعادة تحميل الجداول (لتصبح مرئية) عبر `DETACH TABLE table_name` -&gt; `ATTACH TABLE table_name`. وإذا تم تحميل `metadata` بنجاح عند بدء تشغيل الخادم، فستكون الجداول متاحة مباشرة.

استخدم الإعداد [http&#95;max&#95;single&#95;read&#95;retries](/ar/operations/storing-data#web-storage) لتقييد الحد الأقصى لعدد محاولات إعادة القراءة أثناء عملية قراءة HTTP واحدة.

<div id="zero-copy">
  ### النسخ المتماثل دون نسخ (غير جاهز لبيئات الإنتاج)
</div>

النسخ المتماثل دون نسخ ممكن، لكنه غير موصى به، مع أقراص `S3` و`HDFS` (غير مدعوم). ويعني النسخ المتماثل دون نسخ أنه إذا كانت البيانات مخزنة عن بُعد على عدة أجهزة وتحتاج إلى المزامنة، فلا تُنسخ إلا البيانات الوصفية (مسارات أجزاء البيانات)، دون البيانات نفسها.

:::note النسخ المتماثل دون نسخ غير جاهز لبيئات الإنتاج
يكون النسخ المتماثل دون نسخ معطّلًا افتراضيًا في ClickHouse الإصدار 22.8 وما بعده. لا يُنصح باستخدام هذه الميزة في بيئات الإنتاج.
:::