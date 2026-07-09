---
description: 'امتداد للدالة الجدولية s3، يتيح معالجة الملفات
  من Amazon S3 وGoogle Cloud Storage بالتوازي باستخدام عدد كبير من العقد ضمن
  عنقود محدد.'
sidebar_label: 's3Cluster'
sidebar_position: 181
slug: /sql-reference/table-functions/s3Cluster
title: 's3Cluster'
doc_type: 'مرجع'
---

هذا امتداد للدالة الجدولية [s3](/ar/sql-reference/table-functions/s3.md).

يتيح معالجة الملفات من [Amazon S3](https://aws.amazon.com/s3/) وGoogle Cloud Storage [Google Cloud Storage](https://cloud.google.com/storage/) بالتوازي باستخدام عدد كبير من العقد ضمن عنقود محدد. على العقدة المُبادِرة، ينشئ اتصالًا بجميع العقد في العنقود، ويوسّع أحرف البدل (*) في مسار ملف S3، ثم يوزّع كل ملف ديناميكيًا. وعلى العقدة العاملة، يطلب من المُبادِر المهمة التالية لمعالجتها ثم ينفّذها. ويتكرر ذلك حتى تكتمل جميع المهام.

<div id="syntax">
  ## الصياغة
</div>

```sql
s3Cluster(cluster_name, url[, NOSIGN | access_key_id, secret_access_key,[session_token]][, format][, structure][, compression_method][, headers][, extra_credentials])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## الوسيطات
</div>

| الوسيطة                                 | الوصف                                                                                                                                                                                                                                                                                                 |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                          | اسم عنقود يُستخدم لبناء مجموعة من العناوين ومعلمات الاتصال بالخوادم البعيدة والمحلية.                                                                                                                                                                                                                 |
| `url`                                   | مسار إلى ملف أو مجموعة من الملفات. يدعم محارف البدل التالية في وضع القراءة فقط: `*`, `**`, `?`, `{'abc','def'}` و `{N..M}` حيث إن `N` و `M` — أرقام، و`abc` و `def` — سلاسل نصية. لمزيد من المعلومات، راجع [محارف البدل في المسار](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `NOSIGN`                                | إذا جرى تمرير هذه الكلمة المفتاحية بدلًا من بيانات الاعتماد، فلن تُوقَّع أي من الطلبات.                                                                                                                                                                                                               |
| `access_key_id` and `secret_access_key` | مفاتيح تحدد بيانات الاعتماد المطلوب استخدامها مع نقطة النهاية المحددة. اختيارية.                                                                                                                                                                                                                      |
| `session_token`                         | رمز جلسة لاستخدامه مع المفاتيح المحددة. ويكون اختياريًا عند تمرير المفاتيح.                                                                                                                                                                                                                           |
| `format`                                | [تنسيق](/ar/sql-reference/formats) الملف.                                                                                                                                                                                                                                                                |
| `structure`                             | بنية الجدول. التنسيق هو `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                |
| `compression_method`                    | المعلمة اختيارية. القيم المدعومة: `none` و`gzip` أو `gz` و`brotli` أو `br` و`xz` أو `LZMA` و`zstd` أو `zst`. افتراضيًا، سيجري اكتشاف طريقة الضغط تلقائيًا من امتداد الملف.                                                                                                                            |
| `headers`                               | المعلمة اختيارية. تسمح بتمرير رؤوس في طلب S3. مرّرها بالتنسيق `headers(key=value)` مثل `headers('x-amz-request-payer' = 'requester')`. راجع [هنا](/ar/sql-reference/table-functions/s3#accessing-requester-pays-buckets) للاطلاع على مثال للاستخدام.                                                     |
| `extra_credentials`                     | اختياري. يمكن تمرير `roleARN` عبر هذه المعلمة. راجع [هنا](/ar/cloud/data-sources/secure-s3#access-your-s3-bucket-with-the-clickhouseaccess-role) للاطلاع على مثال.                                                                                                                                       |

يمكن أيضًا تمرير الوسيطات باستخدام [المجموعات المسماة](/ar/operations/named-collections.md). في هذه الحالة، تعمل `url` و`access_key_id` و`secret_access_key` و`format` و`structure` و`compression_method` بالطريقة نفسها، كما تُدعَم بعض المعلمات الإضافية:

| الوسيطة                       | الوصف                                                                                                                                                                                                                  |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `filename`                    | يُلحَق بعنوان `url` إذا جرى تحديده.                                                                                                                                                                                    |
| `use_environment_credentials` | مفعّلة افتراضيًا، وتسمح بتمرير معلمات إضافية باستخدام متغيرات البيئة `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | معطّلة افتراضيًا.                                                                                                                                                                                                      |
| `expiration_window_seconds`   | القيمة الافتراضية هي 120.                                                                                                                                                                                              |

<div id="returned_value">
  ## القيمة المعادة
</div>

جدول ذو البنية المحددة لقراءة البيانات أو كتابتها في الملف المحدد.

<div id="examples">
  ## أمثلة
</div>

استعلم عن البيانات من جميع الملفات في المجلدين `/root/data/clickhouse` و`/root/data/database/`، باستخدام جميع العُقد في العنقود `cluster_simple`:

```sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'ClickHouse_Minio_P@ssw0rd',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

احسب العدد الإجمالي للصفوف في جميع الملفات في العنقود `cluster_simple`:

:::tip
إذا كانت قائمة الملفات لديك تحتوي على نطاقات رقمية بأصفار بادئة، فاستخدم الصيغة التي تتضمن أقواسًا لكل رقم على حدة أو استخدم `?`.
:::

في حالات الاستخدام الخاصة ببيئات الإنتاج، يُوصى باستخدام [المجموعات المُسمّاة](/ar/operations/named-collections.md). إليك المثال:

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'ClickHouse_Minio_P@ssw0rd';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

<div id="accessing-private-and-public-buckets">
  ## الوصول إلى الحاويات الخاصة والعامة
</div>

يمكن للمستخدمين استخدام الأساليب نفسها الموضحة لدالة s3 [هنا](/ar/sql-reference/table-functions/s3#accessing-public-buckets).

<div id="optimizing-performance">
  ## تحسين الأداء
</div>

للاطلاع على تفاصيل تحسين أداء الدالة `s3`، راجع [دليلنا المفصّل](/ar/integrations/s3/performance).

<div id="related">
  ## مواضيع ذات صلة
</div>

* [محرك S3](../../engines/table-engines/integrations/s3.md)
* [الدالة الجدولية S3](../../sql-reference/table-functions/s3.md)