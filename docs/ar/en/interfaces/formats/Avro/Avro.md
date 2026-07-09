---
alias: []
description: 'وثائق تنسيق Avro'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
doc_type: 'مرجع'
---

import DataTypeMapping from './_snippets/data-types-matching.md'

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✔       |                |

<div id="description">
  ## الوصف
</div>

يُعد [Apache Avro](https://avro.apache.org/) تنسيق تسلسل قائمًا على الصفوف يستخدم الترميز الثنائي لمعالجة البيانات بكفاءة. يدعم التنسيق `Avro` قراءة [ملفات بيانات Avro](https://avro.apache.org/docs/current/specification/#object-container-files) وكتابتها. ويتوقع هذا التنسيق رسائل ذاتية الوصف تتضمن مخططًا مضمّنًا. إذا كنت تستخدم Avro مع سجل المخططات، فارجع إلى التنسيق [`AvroConfluent`](./AvroConfluent.md).

<div id="data-type-mapping">
  ## تعيين أنواع البيانات
</div>

<DataTypeMapping />

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| الإعداد                                    | الوصف                                                                                                                                                            | الافتراضي |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `input_format_avro_allow_missing_fields`   | ما إذا كانت ستُستخدم قيمة افتراضية بدلًا من إصدار خطأ عند عدم العثور على حقل في المخطط.                                                                          | `0`       |
| `input_format_avro_null_as_default`        | ما إذا كانت ستُستخدم قيمة افتراضية بدلًا من إصدار خطأ عند إدراج قيمة `null` في عمود لا يقبل القيم الخالية.                                                       | `0`       |
| `output_format_avro_codec`                 | خوارزمية الضغط لملفات إخراج Avro. القيم الممكنة: `null`، `deflate`، `snappy`، `zstd`.                                                                            |           |
| `output_format_avro_sync_interval`         | تكرار وسم المزامنة في ملفات Avro (بالبايت).                                                                                                                      | `16384`   |
| `output_format_avro_string_column_pattern` | تعبير نمطي لتحديد الأعمدة من النوع `String` لاستخدام تعيين نوع السلسلة في Avro. افتراضيًا، تُكتب أعمدة `String` في ClickHouse على أنها من النوع `bytes` في Avro. |           |
| `output_format_avro_rows_in_file`          | الحد الأقصى لعدد الصفوف في كل ملف إخراج Avro. عند بلوغ هذا الحد، يُنشأ ملف جديد (إذا كان نظام التخزين يدعم تقسيم الملفات).                                       | `1`       |

<div id="examples">
  ## أمثلة
</div>

<div id="reading-avro-data">
  ### قراءة بيانات Avro
</div>

لقراءة البيانات من ملف Avro وإدخالها إلى جدول ClickHouse:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

يجب أن يكون المخطط الجذري لملف Avro الذي تم استيعابه من النوع `record`.

للعثور على المطابقة بين أعمدة الجدول وحقول مخطط Avro، يقارن ClickHouse بين أسمائها.
هذه المقارنة حساسة لحالة الأحرف، وتُتخطّى الحقول غير المستخدمة.

قد تختلف أنواع بيانات أعمدة جدول ClickHouse عن الحقول المقابلة لها في بيانات Avro المُدرجة. عند إدراج البيانات، يفسّر ClickHouse أنواع البيانات وفقًا للجدول أعلاه، ثم [يحوّل](/ar/sql-reference/functions/type-conversion-functions#CAST) البيانات إلى نوع العمود المقابل.

أثناء استيراد البيانات، إذا لم يُعثر على حقل في المخطط وكان الإعداد [`input_format_avro_allow_missing_fields`](/ar/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) مفعّلًا، فستُستخدم القيمة الافتراضية بدلًا من إصدار خطأ.

<div id="writing-avro-data">
  ### كتابة بيانات Avro
</div>

لكتابة البيانات من جدول في ClickHouse إلى ملف Avro:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

يجب أن تستوفي أسماء الأعمدة ما يلي:

* أن تبدأ بـ `[A-Za-z_]`
* أن يليها فقط `[A-Za-z0-9_]`

يمكن تهيئة ضغط المخرجات وفاصل المزامنة لملفات Avro باستخدام الإعدادين [`output_format_avro_codec`](/ar/operations/settings/settings-formats.md/#output_format_avro_codec) و[`output_format_avro_sync_interval`](/ar/operations/settings/settings-formats.md/#output_format_avro_sync_interval) على التوالي.

<div id="inferring-the-avro-schema">
  ### استنتاج مخطط Avro
</div>

باستخدام الدالة [`DESCRIBE`](/ar/sql-reference/statements/describe-table) في ClickHouse، يمكنك بسرعة عرض التنسيق المستنتَج لملف Avro كما في المثال التالي.
يتضمن هذا المثال عنوان URL لملف Avro متاحًا للعامة في bucket العامة لـ ClickHouse S3:

```sql
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro', 'Avro');

┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```