---
alias: []
description: 'وثائق تنسيق AvroConfluent'
input_format: true
keywords: ['AvroConfluent']
output_format: true
slug: /interfaces/formats/AvroConfluent
title: 'AvroConfluent'
doc_type: 'reference'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| المدخل | المخرج | الاسم المستعار |
| ------ | ------ | -------------- |
| ✔      | ✔      |                |

<div id="description">
  ## الوصف
</div>

يُعد [Apache Avro](https://avro.apache.org/) تنسيق تسلسل موجَّهًا نحو الصفوف يستخدم الترميز الثنائي لمعالجة البيانات بكفاءة. ويدعم تنسيق `AvroConfluent` قراءة الرسائل المرمَّزة بـ Avro وكتابتها باستخدام [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) (أو الخدمات المتوافقة مع واجهة برمجة التطبيقات).

تستخدم كل رسالة تنسيق wire الخاص بـ Confluent: بايت سحري (`0x00`) يتبعه معرّف مخطط مكوَّن من 4 بايتات بترتيب big-endian، ثم قيمة Avro الثنائية. عند القراءة، يحدِّد ClickHouse معرّف المخطط من خلال الاستعلام عن سجل المخططات. وعند الكتابة، يسجِّل ClickHouse المخطط المُشتق من أعمدة الإخراج ويضيف المعرّف الناتج في بداية كل صف. وتُخزَّن المخططات مؤقتًا للحصول على أفضل أداء.

<a id="data-types-matching" />

<div id="data-type-mapping">
  ## مطابقة أنواع البيانات
</div>

<DataTypesMatching />

<div id="format-settings">
  ## إعدادات التنسيق
</div>

[//]: # "ملاحظة: يمكن تعيين هذه الإعدادات على مستوى الجلسة، لكن هذا غير شائع، وقد يؤدّي إبراز ذلك في التوثيق بشكل كبير إلى إرباك المستخدمين."

| الإعداد                                          | الوصف                                                                                                                                             | الافتراضي |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `input_format_avro_allow_missing_fields`         | ما إذا كان سيتم استخدام قيمة افتراضية بدلًا من إطلاق خطأ عند عدم العثور على حقل في المخطط.                                                        | `0`       |
| `input_format_avro_null_as_default`              | ما إذا كان سيتم استخدام قيمة افتراضية بدلًا من إطلاق خطأ عند إدراج قيمة `null` في عمود لا يقبل القيمة NULL.                                       | `0`       |
| `format_avro_schema_registry_url`                | عنوان URL الخاص بـ Confluent سجل المخططات. في حالة المصادقة الأساسية، يمكن تضمين بيانات الاعتماد المرمّزة بتنسيق URL مباشرةً في مسار URL.      |           |
| `format_avro_schema_registry_connection_timeout` | مهلة الاتصال، بالثواني، لعميل HTTP الخاص بـ سجل المخططات (تُستخدم لكلٍّ من جلب المخطط والتسجيل). يجب أن تكون أكبر من 0 وأقل من 600 (10 دقائق). | `1`       |
| `format_avro_schema_registry_send_timeout`       | مهلة الإرسال، بالثواني، لعميل HTTP الخاص بـ سجل المخططات. يجب أن تكون أكبر من 0 وأقل من 600 (10 دقائق).                                        | `1`       |
| `format_avro_schema_registry_receive_timeout`    | مهلة الاستلام، بالثواني، لعميل HTTP الخاص بـ سجل المخططات. يجب أن تكون أكبر من 0 وأقل من 600 (10 دقائق).                                       | `1`       |
| `output_format_avro_confluent_subject`           | للإخراج: اسم الـ subject الذي يُسجَّل تحته المخطط في سجل المخططات. وهو مطلوب عند الكتابة.                                                      |           |
| `output_format_avro_string_column_pattern`       | للإخراج: تعبير نمطي لأعمدة String التي تُسلسَل بصيغة Avro `string` (الافتراضي هو `bytes`).                                                        |           |

<div id="examples">
  ## أمثلة
</div>

<div id="reading-from-kafka">
  ### القراءة من Kafka
</div>

لقراءة topic في Kafka مُرمَّز بتنسيق Avro باستخدام [محرك جدول Kafka](/ar/engines/table-engines/integrations/kafka.md)، استخدم الإعداد `format_avro_schema_registry_url` لتحديد عنوان URL لسجل المخططات.

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url';

SELECT * FROM topic1_stream;
```

<div id="writing-to-kafka">
  ### الكتابة إلى Kafka
</div>

لكتابة رسائل AvroConfluent إلى topic في Kafka، عيّن كلًا من URL لسجل المخططات واسم الـsubject. يُسجَّل المخطط تلقائيًا في السجل عند أول عملية كتابة.

```sql
CREATE TABLE topic1_sink
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url',
output_format_avro_confluent_subject = 'topic1-value';

INSERT INTO topic1_sink VALUES ('hello', 'world');
```

<div id="using-basic-authentication">
  #### استخدام المصادقة الأساسية
</div>

إذا كان سجل المخططات لديك يتطلب المصادقة الأساسية (على سبيل المثال، إذا كنت تستخدم Confluent Cloud)، فيمكنك تمرير بيانات الاعتماد المُرمَّزة وفق URL في الإعداد `format_avro_schema_registry_url`.

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'https://<username>:<password>@schema-registry-url';
```

<div id="troubleshooting">
  ## استكشاف الأخطاء وإصلاحها
</div>

لمراقبة تقدّم إدخال البيانات وتصحيح الأخطاء في مستهلك Kafka، يمكنك الاستعلام عن [جدول النظام `system.kafka_consumers`](../../../operations/system-tables/kafka_consumers.md). وإذا كان النشر لديك يتضمن عدة نُسخ متماثلة (مثل ClickHouse Cloud)، فيجب استخدام [دالة الجدول `clusterAllReplicas`](../../../sql-reference/table-functions/cluster.md).

```sql
SELECT * FROM clusterAllReplicas('default',system.kafka_consumers)
ORDER BY assignments.partition_id ASC;
```

إذا واجهت مشكلات في تحديد المخطط، يمكنك استخدام [kafkacat](https://github.com/edenhill/kafkacat) مع [clickhouse-local](/ar/operations/utilities/clickhouse-local.md) لاستكشاف الأخطاء وإصلاحها:

```bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```