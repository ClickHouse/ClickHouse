---
description: 'يوفّر هذا المحرك تكاملًا بوضع القراءة فقط مع جداول Delta Lake الموجودة في Amazon S3.'
sidebar_label: 'DeltaLake'
sidebar_position: 40
slug: /engines/table-engines/integrations/deltalake
title: 'محرك الجدول DeltaLake'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="deltalake-table-engine">
  # محرك الجدول DeltaLake
</div>

يوفّر هذا المحرك تكاملًا مع جداول [Delta Lake](https://github.com/delta-io/delta) الموجودة في S3 وGCP وAzure Storage، ويدعم عمليتَي القراءة والكتابة (اعتبارًا من v25.10).

<div id="create-table">
  ## إنشاء جدول DeltaLake
</div>

لإنشاء جدول DeltaLake، يجب أن يكون موجودًا بالفعل في S3 أو GCP أو تخزين Azure. الأوامر أدناه لا تقبل معاملات DDL لإنشاء جدول جديد.

<Tabs>
  <TabItem value="S3" label="S3" default>
    **البنية**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
    ```

    **معلمات المحرك**

    * `url` — URL للـ bucket مع المسار إلى جدول Delta Lake الحالي.
    * `aws_access_key_id`, `aws_secret_access_key` - بيانات اعتماد طويلة الأجل لمستخدم حساب [AWS](https://aws.amazon.com/). يمكنك استخدامها لمصادقة طلباتك. هذه المعلمة اختيارية. إذا لم يتم تحديد بيانات الاعتماد، فسيتم استخدامها من ملف الإعداد.
    * `extra_credentials` - اختياري. يُستخدم لتمرير `role_arn` للوصول المستند إلى الأدوار في ClickHouse Cloud. راجع [Secure S3](/ar/cloud/data-sources/secure-s3) للاطلاع على خطوات الإعداد.

    يمكن تحديد معلمات المحرك باستخدام [Named Collections](/ar/operations/named-collections.md).

    **مثال**

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
    ```

    باستخدام المجموعات المُسمّاة:

    ```xml
    <clickhouse>
        <named_collections>
            <deltalake_conf>
                <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
                <access_key_id>ABC123</access_key_id>
                <secret_access_key>Abc+123</secret_access_key>
            </deltalake_conf>
        </named_collections>
    </clickhouse>
    ```

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake(deltalake_conf, filename = 'test_table')
    ```
  </TabItem>

  <TabItem value="GCP" label="GCP" default>
    **البنية**

    ```sql
    -- استخدام HTTPS URL (موصى به)
    CREATE TABLE table_name
    ENGINE = DeltaLake('https://storage.googleapis.com/<bucket>/<path>/', '<access_key_id>', '<secret_access_key>')
    ```

    :::note[URI ‏`gsutil` غير مدعوم]
    URI الخاص بـ `gsutil` مثل `gs://clickhouse-docs-example-bucket` غير مدعوم، يُرجى استخدام URL يبدأ بـ `https://storage.googleapis.com`
    :::

    **الوسيطات**

    * `url` — URL لـ GCS bucket المؤدي إلى جدول Delta Lake. يجب استخدام `https://storage.googleapis.com/<bucket>/<path>/`
      بهذا التنسيق (نقطة نهاية GCS XML API)، أو `gs://<bucket>/<path>/` الذي يُحوَّل تلقائيًا.
    * `access_key_id` — مفتاح الوصول إلى GCS. أنشِئه عبر Google Cloud Console ← Cloud Storage ← Settings ← Interoperability.
    * `secret_access_key` — المفتاح السري لـ GCS.

    **المجموعات المُسمّاة**

    يمكنك أيضًا استخدام المجموعات المُسمّاة.
    على سبيل المثال:

    ```sql
    CREATE NAMED COLLECTION gcs_creds AS
    access_key_id = '<access_key>',
    secret_access_key = '<secret>';

    CREATE TABLE gcpDeltaLake
    ENGINE = DeltaLake(gcs_creds, url = 'https://storage.googleapis.com/<bucket>/<path>')
    ```
  </TabItem>

  <TabItem value="Azure" label="Azure" default>
    **البنية**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    ```

    **الوسيطات**

    * `connection_string` — سلسلة اتصال Azure
    * `storage_account_url` — URL لحساب التخزين في Azure (على سبيل المثال: https://account.blob.core.windows.net)
    * `container_name` — اسم حاوية Azure
    * `blobpath` — المسار إلى جدول Delta Lake داخل الحاوية
    * `account_name` — اسم حساب التخزين في Azure
    * `account_key` — مفتاح حساب التخزين في Azure
  </TabItem>
</Tabs>

<div id="insert-data">
  ## كتابة البيانات باستخدام جدول DeltaLake
</div>

بمجرد إنشاء جدول باستخدام محرك الجدول DeltaLake، يمكنك إدراج البيانات فيه باستخدام:

```sql
SET allow_delta_lake_writes = 1;

INSERT INTO deltalake(id, firstname, lastname, gender, age)
VALUES (1, 'John', 'Smith', 'M', 32);
```

:::note
لا تكون الكتابة باستخدام محرك الجدول مدعومة إلا عبر delta kernel.
لا تزال الكتابة إلى Azure غير مدعومة، لكنها تعمل مع S3 وGCS.

تُعد الكتابة إلى Delta Lake ميزة Beta ويجب تمكينها باستخدام `SET allow_delta_lake_writes = 1` (متاحة بدءًا من الإصدار 26.7؛ وفي الإصدارات الأقدم استخدم `SET allow_experimental_delta_lake_writes = 1`).
:::

<div id="data-cache">
  ### ذاكرة التخزين المؤقت للبيانات
</div>

يدعم محرك الجدول `DeltaLake` ودالة الجدول التخزين المؤقت للبيانات، تمامًا كما هو الحال في وحدات التخزين `S3` و`AzureBlobStorage` و`HDFS`. راجع [&quot;محرك الجدول S3&quot;](../../../engines/table-engines/integrations/s3.md#data-cache) لمزيد من التفاصيل.

<div id="see-also">
  ## انظر أيضًا
</div>

* [دالة الجدول deltaLake](../../../sql-reference/table-functions/deltalake.md)