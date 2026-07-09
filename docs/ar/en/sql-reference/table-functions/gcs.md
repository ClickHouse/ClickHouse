---
description: 'يوفّر واجهة شبيهة بالجداول لتنفيذ `SELECT` و`INSERT` على البيانات من Google
  Cloud Storage. يتطلب دور IAM `Storage Object User`.'
keywords: ['gcs', 'حاوية']
sidebar_label: 'gcs'
sidebar_position: 70
slug: /sql-reference/table-functions/gcs
title: 'gcs'
doc_type: 'reference'
---

يوفّر واجهة شبيهة بالجداول لتنفيذ `SELECT` و`INSERT` على البيانات من [Google Cloud Storage](https://cloud.google.com/storage/). ويتطلب [دور IAM `Storage Object User`](https://cloud.google.com/storage/docs/access-control/iam-roles).

هذا اسم مستعار لـ [دالة الجدول s3](../../sql-reference/table-functions/s3.md).

إذا كانت لديك عدة نُسخ متماثلة في العنقود، فيمكنك بدلًا من ذلك استخدام [الدالة s3Cluster](../../sql-reference/table-functions/s3Cluster.md) (التي تعمل مع GCS) لتنفيذ عمليات الإدراج بالتوازي.

<div id="syntax">
  ## الصياغة
</div>

```sql
gcs(url [, NOSIGN | hmac_key, hmac_secret] [,format] [,structure] [,compression_method])
gcs(named_collection[, option=value [,..]])
```

:::tip GCS
تتكامل دالة الجدول GCS مع Google Cloud Storage باستخدام GCS XML API ومفاتيح HMAC.
راجع [مستندات التشغيل البيني من Google](https://cloud.google.com/storage/docs/interoperability) لمزيد من التفاصيل حول نقطة النهاية وHMAC.
:::

<div id="arguments">
  ## الوسيطات
</div>

| Argument                     | Description                                                                                                                                                                     |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                        | مسار الحاوية إلى الملف. يدعم أحرف البدل التالية في وضع `readonly`: `*` و`**` و`?` و`{abc,def}` و`{N..M}`، حيث إن `N` و`M` — أرقام، و`'abc'` و`'def'` — سلاسل نصية.               |
| `NOSIGN`                     | إذا استُخدمت هذه الكلمة المفتاحية بدلًا من بيانات الاعتماد، فلن يتم توقيع أي من الطلبات.                                                                                        |
| `hmac_key` and `hmac_secret` | مفاتيح تحدد بيانات الاعتماد المطلوب استخدامها مع نقطة النهاية المحددة. هذا الوسيط اختياري.                                                                                      |
| `format`                     | [تنسيق](/ar/sql-reference/formats) الملف.                                                                                                                                          |
| `structure`                  | بنية الجدول. التنسيق: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                            |
| `compression_method`         | هذا الوسيط اختياري. القيم المدعومة: `none` أو `gzip` أو `gz` أو `brotli` أو `br` أو `xz` أو `LZMA` أو `zstd` أو `zst`. افتراضيًا، سيُكتشف أسلوب الضغط تلقائيًا من امتداد الملف. |

:::note GCS
يكون مسار GCS بهذا التنسيق لأن نقطة النهاية الخاصة بـ Google XML API تختلف عن JSON API:

```text
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
```

وليس ~~https://storage.cloud.google.com~~.
:::

يمكن أيضًا تمرير الوسيطات باستخدام [المجموعات المسماة](/ar/operations/named-collections.md). في هذه الحالة، تعمل `url` و`format` و`structure` و`compression_method` بالطريقة نفسها، كما أن بعض المعلمات الإضافية مدعومة أيضًا:

| Parameter                     | Description                                                                                                                                                                                                           |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `access_key_id`               | ‏`hmac_key`، اختياري.                                                                                                                                                                                                 |
| `secret_access_key`           | ‏`hmac_secret`، اختياري.                                                                                                                                                                                              |
| `filename`                    | يُضاف إلى عنوان URL إذا تم تحديده.                                                                                                                                                                                    |
| `use_environment_credentials` | مُفعّل افتراضيًا، ويتيح تمرير معلمات إضافية باستخدام متغيرات البيئة `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | غير مُفعّل افتراضيًا.                                                                                                                                                                                                 |
| `expiration_window_seconds`   | القيمة الافتراضية هي 120.                                                                                                                                                                                             |

<div id="returned_value">
  ## القيمة المعادة
</div>

جدول ذو البنية المحددة لقراءة البيانات من الملف المحدد أو كتابتها فيه.

<div id="examples">
  ## أمثلة
</div>

اختيار أول صفَّين من ملف GCS على الرابط `https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz`. يُكتشَف أسلوب الضغط تلقائيًا من امتداد الملف `.gz`:

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

الاستعلام نفسه أعلاه، ولكن مع تحديد أسلوب الضغط `gzip` صراحةً بدلًا من الاعتماد على الاكتشاف التلقائي:

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32', 'gzip')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="usage">
  ## الاستخدام
</div>

لنفترض أن لدينا عدة ملفات بعناوين URI التالية على GCS:

* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;4.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;4.csv&#39;

احسب عدد الصفوف في الملفات التي تنتهي أسماؤها بالأرقام من 1 إلى 3:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      18 │
└─────────┘
```

احسب العدد الإجمالي للصفوف في جميع الملفات الموجودة في هذين الدليلين:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      24 │
└─────────┘
```

:::warning
إذا كانت قائمة ملفاتك تتضمن نطاقات رقمية تبدأ بأصفار، فاستخدم الصيغة التي تحتوي على أقواس لكل رقم على حدة، أو استخدم `?`.
:::

احسب العدد الإجمالي للصفوف في الملفات المسماة `file-000.csv` و`file-001.csv` و... و`file-999.csv`:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'name String, value UInt32');
```

```text
┌─count()─┐
│      12 │
└─────────┘
```

أدرِج البيانات في ملف `test-data.csv.gz`:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

أدرِج البيانات في الملف `test-data.csv.gz` من جدول موجود:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

يمكن استخدام Glob ** لاجتياز الدليل تكراريًا. انظر إلى المثال أدناه، إذ سيجلب جميع الملفات من الدليل `my-test-bucket-768` تكراريًا:

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

يسترجع ما يلي البيانات من جميع ملفات `test-data.csv.gz` الموجودة داخل أي مجلد ضمن الدليل `my-test-bucket` بشكلٍ تكراري:

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

للاستخدام في بيئات الإنتاج، يُوصى باستخدام [المجموعات المسماة](/ar/operations/named-collections.md). فيما يلي مثال:

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = '***',
        secret_access_key = '***';
SELECT count(*)
FROM gcs(creds, url='https://s3-object-url.csv')
```

<div id="partitioned-write">
  ## الكتابة المُقسَّمة حسب القسم
</div>

إذا حددت تعبير `PARTITION BY` عند إدراج البيانات في جدول `GCS`، فسيُنشأ ملف منفصل لكل قيمة قسم. ويساعد تقسيم البيانات إلى ملفات منفصلة على تحسين كفاءة عمليات القراءة.

**أمثلة**

1. يؤدي استخدام معرّف القسم في المفتاح إلى إنشاء ملفات منفصلة:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```

ونتيجةً لذلك، تُكتب البيانات في ثلاثة ملفات: `file_x.csv` و`file_y.csv` و`file_z.csv`.

2. يؤدي استخدام معرّف القسم في اسم الحاوية إلى إنشاء ملفات في حاويات مختلفة:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```

وبناءً على ذلك، تُكتب البيانات في ثلاثة ملفات ضمن ثلاث حاويات تخزين مختلفة: `my_bucket_1/file.csv`، و`my_bucket_10/file.csv`، و`my_bucket_20/file.csv`.

<div id="related">
  ## مواضيع ذات صلة
</div>

* [دالة الجدول S3](s3.md)
* [محرك S3](../../engines/table-engines/integrations/s3.md)