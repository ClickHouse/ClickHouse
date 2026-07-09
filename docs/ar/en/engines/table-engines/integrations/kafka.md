---
description: 'يمكن استخدام محرك جدول Kafka للعمل مع Apache Kafka، ويتيح لك نشر تدفقات البيانات أو الاشتراك فيها،
  وتنظيم تخزين متحمّل للأعطال، ومعالجة التدفقات فور توفرها.'
sidebar_label: 'Kafka'
sidebar_position: 110
slug: /engines/table-engines/integrations/kafka
title: 'محرك جدول Kafka'
keywords: ['Kafka', 'محرك جدول']
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="kafka-table-engine">
  # محرك جدول Kafka
</div>

:::tip
إذا كنت تستخدم ClickHouse Cloud، فنحن نوصي باستخدام [ClickPipes](/ar/integrations/clickpipes) بدلًا منه. يوفّر ClickPipes دعمًا أصليًا لاتصالات الشبكة الخاصة، وإمكانية توسيع موارد الاستيعاب وموارد العنقود كلٍّ على حدة، ومراقبة شاملة لتدفّق بيانات Kafka إلى ClickHouse.
:::

* انشر تدفقات البيانات أو اشترك فيها.
* نظّم تخزينًا متحمّلًا للأعطال.
* عالج التدفقات عند توفرها.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [ALIAS expr1],
    name2 [type2] [ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_security_protocol = '',]
    [kafka_sasl_mechanism = '',]
    [kafka_sasl_username = '',]
    [kafka_sasl_password = '',]
    [kafka_autodetect_client_rack = '',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0,]
    [kafka_client_id = '',]
    [kafka_poll_timeout_ms = 0,]
    [kafka_poll_max_batch_size = 0,]
    [kafka_flush_interval_ms = 0,]
    [kafka_consumer_reschedule_ms = 0,]
    [kafka_thread_per_consumer = 0,]
    [kafka_handle_error_mode = 'default',]
    [kafka_commit_on_select = false,]
    [kafka_consumer_acquire_timeout_ms = 30000,]
    [kafka_max_rows_per_message = 1,]
    [kafka_compression_codec = '',]
    [kafka_compression_level = -1];
```

المعلمات المطلوبة:

* `kafka_broker_list` — قائمة مفصولة بفواصل للوسطاء (على سبيل المثال، `localhost:9092`).
* `kafka_topic_list` — قائمة بموضوعات Kafka.
* `kafka_group_name` — مجموعة من مستهلكي Kafka. تُتبع إزاحات القراءة لكل مجموعة على حدة. إذا كنت لا تريد تكرار الرسائل في العنقود، فاستخدم اسم المجموعة نفسه في كل مكان.
* `kafka_format` — تنسيق الرسائل. يستخدم نفس الصياغة المستخدمة مع دالة SQL ‏`FORMAT`، مثل `JSONEachRow`. لمزيد من المعلومات، راجع قسم [التنسيقات](../../../interfaces/formats.md).

المعلمات الاختيارية:

* `kafka_security_protocol` - البروتوكول المستخدم للتواصل مع الوسطاء. القيم الممكنة: `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`.
* `kafka_sasl_mechanism` - آلية SASL المستخدمة للمصادقة. القيم الممكنة: `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `AWS_MSK_IAM`.
* `kafka_aws_region` - منطقة AWS لمصادقة MSK IAM. تُكتشف تلقائيًا من عنوان الوسيط إذا لم تُحدَّد. حدِّدها صراحةً عند استخدام الأسماء المستعارة لـ PrivateLink أو أسماء مضيفات DNS مخصصة لا تتضمن معلومات المنطقة. الافتراضي: فارغ (اكتشاف تلقائي).
* `kafka_sasl_username` - اسم مستخدم SASL للاستخدام مع الآليتين `PLAIN` و`SASL-SCRAM-..`.
* `kafka_sasl_password` - كلمة مرور SASL للاستخدام مع الآليتين `PLAIN` و`SASL-SCRAM-..`.
* `kafka_schema` — معلمة يجب استخدامها إذا كانت الصيغة تتطلب تعريف مخطط. على سبيل المثال، يتطلب [Cap&#39;n Proto](https://capnproto.org/) مسار ملف المخطط واسم الكائن الجذر `schema.capnp:Message`.
* `kafka_schema_registry_skip_bytes` — عدد البايتات التي يجب تخطيها من بداية كل رسالة عند استخدام schema registry مع ترويسات الغلاف (مثل AWS Glue Schema Registry الذي يتضمن غلافًا بطول 19 بايت). النطاق: `[0, 255]`. الافتراضي: `0`.
* `kafka_num_consumers` — عدد المستهلكين لكل جدول. حدِّد عددًا أكبر من المستهلكين إذا كانت إنتاجية مستهلك واحد غير كافية. يجب ألا يتجاوز العدد الإجمالي للمستهلكين عدد الأقسام في الموضوع، إذ لا يمكن إسناد أكثر من مستهلك واحد لكل قسم، كما يجب ألا يزيد على عدد الأنوية الفعلية في الخادم الذي يعمل عليه ClickHouse. الافتراضي: `1`.
* `kafka_max_block_size` — الحد الأقصى لحجم الدفعة (بالرسائل) لعملية poll. الافتراضي: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `kafka_skip_broken_messages` — مقدار تحمّل محلل رسائل Kafka للرسائل غير المتوافقة مع المخطط في كل كتلة. إذا كانت قيمة `kafka_skip_broken_messages = N`، فسيتخطى المحرك *N* من رسائل Kafka التي يتعذر تحليلها (الرسالة تعادل صفًا واحدًا من البيانات). الافتراضي: `0`.
* `kafka_commit_every_batch` — نفّذ commit لكل دفعة جرى استهلاكها ومعالجتها بدلًا من commit واحد بعد كتابة كتلة كاملة. الافتراضي: `0`.
* `kafka_client_id` — معرّف العميل. يكون فارغًا افتراضيًا.
* `kafka_poll_timeout_ms` — المهلة الزمنية لعملية poll واحدة من Kafka. الافتراضي: [stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
* `kafka_poll_max_batch_size` — الحد الأقصى لعدد الرسائل التي يمكن جلبها في عملية poll واحدة من Kafka. الافتراضي: [max&#95;block&#95;size](/ar/operations/settings/settings#max_block_size).
* `kafka_flush_interval_ms` — المهلة الزمنية لتفريغ البيانات من Kafka. الافتراضي: [stream&#95;flush&#95;interval&#95;ms](/ar/operations/settings/settings#stream_flush_interval_ms).
* `kafka_consumer_reschedule_ms` — الفاصل الزمني لإعادة الجدولة عند تعطل stream processing في Kafka (على سبيل المثال، عند عدم توفر رسائل للاستهلاك). يتحكم هذا الإعداد في مدة التأخير قبل أن يعاود المستهلك إجراء polling. يجب ألا يتجاوز `kafka_consumers_pool_ttl_ms`. الافتراضي: `500` ميلي ثانية.
* `kafka_thread_per_consumer` — يوفّر خيط تنفيذ مستقلاً لكل مستهلك. عند تمكينه، يقوم كل مستهلك بتفريغ البيانات بشكل مستقل وبالتوازي (وإلا فستُدمج الصفوف من عدة مستهلكين لتكوين كتلة واحدة). الافتراضي: `0`.
* `kafka_handle_error_mode` — كيفية التعامل مع الأخطاء في محرك Kafka. القيم الممكنة: default (سيُطرَح الاستثناء إذا فشل تحليل رسالة)، stream (ستُحفَظ رسالة الاستثناء والرسالة الخام في الأعمدة الافتراضية `_error` و`_raw_message`)، dead&#95;letter&#95;queue (ستُحفَظ البيانات المرتبطة بالخطأ في system.dead&#95;letter&#95;queue).
* `kafka_commit_on_select` —  نفّذ commit للرسائل عند إجراء استعلام `SELECT`. الافتراضي: `false`.
* `kafka_consumer_acquire_timeout_ms` — مهلة زمنية بالميلي ثانية للحصول على مستهلك Kafka أثناء استعلامات `SELECT` المباشرة على جدول `Kafka2` (مع تخزين الإزاحات المستند إلى Keeper). عند تشغيل عدة استعلامات `SELECT` مباشرة ومتزامنة على الجدول نفسه، يجب أن ينتظر كل استعلام حتى يصبح المستهلكون متاحين. تمنع هذه المهلة حدوث حالات deadlocks عندما تحتفظ الاستعلامات بمجموعات فرعية مختلفة من المستهلكين. الافتراضي: `30000`.
* `kafka_max_rows_per_message` — الحد الأقصى لعدد الصفوف المكتوبة في رسالة Kafka واحدة للتنسيقات المعتمدة على الصفوف. القيمة الافتراضية: `1`.
* `kafka_autodetect_client_rack` — يضبط تلقائيًا المعامل `client.rack` في `librdkafka` لتفضيل نُسخ Kafka المتماثلة الأقرب.
  المصادر المدعومة:
  `AWS_ZONE_ID` لمعرّف منطقة التوافر في AWS IMDSv2، على سبيل المثال `euc1-az1`;
  `AWS_ZONE_NAME` لاسم منطقة التوافر في AWS IMDSv2، على سبيل المثال `eu-central-1a`;
  `GCP_ZONE` لمنطقة خدمة البيانات الوصفية في GCP، على سبيل المثال `europe-central2-a`;
  `CLICKHOUSE` لاستخدام الاكتشاف الداخلي في ClickHouse، والذي قد يعتمد على البيانات الوصفية السحابية أو على الإعدادات;
  `AWS_ZONE_NAME_THEN_GCP_ZONE` لتجربة `AWS_ZONE_NAME` ثم `GCP_ZONE`.
  القيمة الافتراضية: سلسلة فارغة، مُعطَّل.
  نصيحة: تستخدم البيئات المختلفة تنسيقات مختلفة لمناطق التوافر. يستخدم Amazon MSK عادةً معرّفات المناطق، لذا يُفضَّل `AWS_ZONE_ID`. أما Confluent Cloud فعادةً ما يستخدم أسماء المناطق، لذا يُفضَّل `AWS_ZONE_NAME`. إذا لم تكن متأكدًا، فاستخدم `AWS_ZONE_NAME_THEN_GCP_ZONE` أو تحقّق من قيمة `broker.rack` في الـ cluster لديك.
  ملاحظة: يجب تهيئة وسطاء Kafka باستخدام `broker.rack` و `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector`.
* `kafka_compression_codec` — ترميز الضغط المستخدم لإنتاج الرسائل. القيم المدعومة: سلسلة فارغة، `none`، `gzip`، `snappy`، `lz4`، `zstd`. في حال كانت السلسلة فارغة، فلن يضبط الجدول ترميز الضغط، وبالتالي ستُستخدم القيم من ملفات الإعدادات أو القيمة الافتراضية من `librdkafka`. القيمة الافتراضية: سلسلة فارغة.
* `kafka_compression_level` — معامل مستوى الضغط للخوارزمية المحددة بواسطة kafka&#95;compression&#95;codec. تؤدي القيم الأعلى إلى ضغط أفضل على حساب زيادة CPU usage. يعتمد النطاق القابل للاستخدام على الخوارزمية: `[0-9]` لـ `gzip`؛ و`[0-12]` لـ `lz4`؛ و`0` فقط لـ `snappy`؛ و`[0-12]` لـ `zstd`؛ و`-1` = مستوى الضغط الافتراضي المعتمد على codec. القيمة الافتراضية: `-1`.
* `kafka_map_virtual_columns_on_write` — إذا كان مُمكّنًا، فستُربَط الأعمدة ذات الأسماء الخاصة `_key` و`_timestamp` و`_headers.name` و`_headers.value` في مخطط الجدول ببيانات Kafka الوصفية المقابلة للرسالة عند `INSERT`، وتُستبعَد من message payload. راجع [ربط الأعمدة ببيانات Kafka الوصفية للرسالة](#mapping-columns-to-kafka-message-metadata). القيمة الافتراضية: `false`.

أمثلة:

```sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092',
                            kafka_topic_list = 'topic',
                            kafka_group_name = 'group1',
                            kafka_format = 'JSONEachRow',
                            kafka_num_consumers = 4;

  CREATE TABLE queue3 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">
  <summary>طريقة مُهمَلة لإنشاء جدول</summary>

  :::note
  لا تستخدم هذه الطريقة في المشاريع الجديدة. وإذا أمكن، انقل المشاريع القديمة إلى الطريقة الموضحة أعلاه.
  :::

  ```sql
  Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
        [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_max_block_size,  kafka_skip_broken_messages, kafka_commit_every_batch, kafka_client_id, kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms, kafka_consumer_reschedule_ms, kafka_thread_per_consumer, kafka_handle_error_mode, kafka_commit_on_select, kafka_max_rows_per_message]);
  ```
</details>

:::info
لا يدعم محرك جدول Kafka الأعمدة التي تحتوي على [قيمة افتراضية](/ar/sql-reference/statements/create/table#default_values). وإذا كنت بحاجة إلى أعمدة ذات قيمة افتراضية، فيمكنك إضافتها ضمن العرض المادي (انظر أدناه).
:::

<div id="description">
  ## الوصف
</div>

تُتتبَّع الرسائل المُستلَمة تلقائيًا، لذلك لا تُحتسَب كل رسالة في المجموعة إلا مرة واحدة فقط. إذا كنت تريد الحصول على البيانات مرتين، فأنشئ نسخة من الجدول باسم مجموعة آخر.

المجموعات مرنة ومتزامنة على مستوى العنقود. على سبيل المثال، إذا كان لديك 10 مواضيع Kafka و5 نسخ من جدول في عنقود، فستحصل كل نسخة على موضوعين. وإذا تغيّر عدد النسخ، فستُعاد تلقائيًا إعادة توزيع المواضيع على النسخ. اقرأ المزيد عن ذلك هنا: http://kafka.apache.org/intro.

يوصى بأن يكون لكل موضوع Kafka مجموعة مستهلكين مخصّصة له، بما يضمن اقترانًا حصريًا بين الموضوع والمجموعة، خاصةً في البيئات التي قد تُنشأ فيها المواضيع وتُحذف ديناميكيًا (مثلًا في الاختبار أو staging).

لا يُعد `SELECT` مفيدًا كثيرًا لقراءة الرسائل (باستثناء debugging)، لأن كل رسالة لا يمكن قراءتها إلا مرة واحدة. والأكثر عملية هو إنشاء تدفقات في الوقت الفعلي باستخدام العروض المادية. وللقيام بذلك:

1. استخدم المحرك لإنشاء مستهلك Kafka واعتبره تدفق بيانات.
2. أنشئ جدولًا بالبنية المطلوبة.
3. أنشئ عرضًا ماديًا يحوّل البيانات من المحرك ويضعها في جدول تم إنشاؤه مسبقًا.

عندما ينضم `MATERIALIZED VIEW` إلى المحرك، يبدأ في جمع البيانات في الخلفية. يتيح لك هذا الاستمرار في تلقّي الرسائل من Kafka وتحويلها إلى التنسيق المطلوب باستخدام `SELECT`.
يمكن لجدول Kafka واحد أن يحتوي على أي عدد تريده من العروض المادية، وهي لا تقرأ البيانات من جدول Kafka مباشرةً، بل تستقبل سجلات جديدة (على شكل blocks)، وبهذه الطريقة يمكنك الكتابة إلى عدة جداول بمستويات مختلفة من التفصيل (مع التجميع وبدونه).

مثال:

```sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

لتحسين الأداء، تُجمَّع الرسائل المستلمة في كتل بحجم [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size). وإذا لم تتكوّن الكتلة خلال [stream&#95;flush&#95;interval&#95;ms](/ar/operations/settings/settings#stream_flush_interval_ms) ملّي ثانية، فستُكتب البيانات إلى الجدول بغضّ النظر عن اكتمال الكتلة.

لإيقاف تلقّي بيانات الموضوع أو تغيير منطق التحويل، افصل العرض المادي:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

إذا كنت تريد تغيير الجدول الهدف باستخدام `ALTER`، فنوصي بتعطيل العرض المادي لتجنب أي تعارض بين الجدول الهدف والبيانات الواردة من العرض.

<div id="configuration">
  ## التهيئة
</div>

على غرار GraphiteMergeTree، يدعم محرك Kafka التهيئة الموسَّعة باستخدام ملف تهيئة ClickHouse. هناك مفتاحا تهيئة يمكنك استخدامهما: على المستوى العام (ضمن `<kafka>`) وعلى مستوى الموضوع (ضمن `<kafka><kafka_topic>`). تُطبَّق التهيئة العامة أولًا، ثم تُطبَّق التهيئة على مستوى الموضوع (إن وُجدت).

```xml
  <kafka>
    <!-- Global configuration options for all tables of Kafka engine type -->
    <debug>cgrp</debug>
    <statistics_interval_ms>3000</statistics_interval_ms>

    <kafka_topic>
        <name>logs</name>
        <statistics_interval_ms>4000</statistics_interval_ms>
    </kafka_topic>

    <!-- Settings for consumer -->
    <consumer>
        <auto_offset_reset>smallest</auto_offset_reset>
        <kafka_topic>
            <name>logs</name>
            <fetch_min_bytes>100000</fetch_min_bytes>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <fetch_min_bytes>50000</fetch_min_bytes>
        </kafka_topic>
    </consumer>

    <!-- Settings for producer -->
    <producer>
        <kafka_topic>
            <name>logs</name>
            <retry_backoff_ms>250</retry_backoff_ms>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <retry_backoff_ms>400</retry_backoff_ms>
        </kafka_topic>
    </producer>
  </kafka>
```

للاطّلاع على قائمة بخيارات التهيئة المتاحة، راجع [مرجع تهيئة librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). استخدم الشرطة السفلية (`_`) بدلًا من النقطة في تهيئة ClickHouse. على سبيل المثال، سيتحوّل `check.crcs=true` إلى `<check_crcs>true</check_crcs>`.

<div id="kafka-aws-msk-iam">
  ### مصادقة AWS MSK IAM
</div>

:::note
تتطلب مصادقة AWS MSK IAM أن يكون ClickHouse مبنيًا مع تمكين دعم AWS S3.
:::

يدعم AWS MSK المصادقة المستندة إلى IAM، مما يتيح الاتصال بعناقيد Kafka باستخدام بيانات اعتماد AWS بدلًا من إدارة أسماء مستخدمين وكلمات مرور منفصلة.

**الإعداد الأساسي:**

عيّن `kafka_sasl_mechanism = 'AWS_MSK_IAM'` في إعدادات الجدول:

```sql
CREATE TABLE msk_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'b-1.mycluster.kafka.us-east-1.amazonaws.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM';
```

تُستخرج منطقة AWS تلقائيًا من نقطة نهاية الـ broker باستخدام مطابقة الأنماط:

* MSK المُهيَّأ: `b-X.cluster.kafka.<region>.amazonaws.com:9098`
* MSK بلا خوادم: `boot-X.kafka-serverless.<region>.amazonaws.com:9098`
* VPC Endpoint: `vpce-X.kafka.<region>.vpce.amazonaws.com:9098`

**بيانات اعتماد AWS:**

تُحمَّل بيانات الاعتماد دائمًا من `~/.aws/credentials` و`~/.aws/config` (ملفات تعريف AWS) عند توفرها. ولتمكين ملفات تعريف مثيل EC2 أيضًا، ومتغيرات البيئة (`AWS_ACCESS_KEY_ID` وغيرها)، وأدوار مهام ECS، ومصادر بيانات الاعتماد التلقائية الأخرى، أضِف ما يلي إلى تهيئة الخادم:

```xml
<kafka>
  <use_environment_credentials>true</use_environment_credentials>
</kafka>
```

لا يمكن تهيئة هذا الإعداد إلا من قِبل مسؤولي الخادم. القيمة الافتراضية: `false`.

**PrivateLink وDNS مخصّص:**

عند استخدام الأسماء المستعارة لـ PrivateLink أو أسماء مضيف DNS مخصّصة لا تتضمن معلومات المنطقة، حدِّد منطقة AWS صراحةً:

```sql
CREATE TABLE msk_privatelink_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'my-privatelink-alias.internal.example.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM',
    kafka_aws_region = 'us-east-1';
```

**أذونات IAM:**

أذونات المستهلك (لقراءة الرسائل):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:group/CLUSTER_NAME/CONSUMER_GROUP/*"
    ]
  }]
}
```

أذونات المُنتِج (لكتابة الرسائل):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*"
    ]
  }]
}
```

<div id="kafka-kerberos-support">
  ### دعم Kerberos
</div>

للتعامل مع Kafka المهيّأ لـ Kerberos، أضِف العنصر الفرعي `security_protocol` بالقيمة `sasl_plaintext`. ويكفي الحصول على تذكرة منح التذاكر الخاصة بـ Kerberos وتخزينها مؤقتًا عبر إمكانات نظام التشغيل.
يمكن لـ ClickHouse الاحتفاظ ببيانات اعتماد Kerberos باستخدام ملف keytab. راعِ العناصر الفرعية `sasl_kerberos_service_name` و`sasl_kerberos_keytab` و`sasl_kerberos_principal`.

مثال:

```xml
<!-- Kerberos-aware Kafka -->
<kafka>
  <security_protocol>SASL_PLAINTEXT</security_protocol>
  <sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
  <sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
</kafka>
```

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_topic` — موضوع Kafka. نوع البيانات: `LowCardinality(String)`.
* `_key` — مفتاح الرسالة. نوع البيانات: `String`.
* `_offset` — إزاحة الرسالة. نوع البيانات: `UInt64`.
* `_timestamp` — الطابع الزمني للرسالة. نوع البيانات: `Nullable(DateTime)`.
* `_timestamp_ms` — الطابع الزمني للرسالة بالمللي ثانية. نوع البيانات: `Nullable(DateTime64(3))`.
* `_partition` — الـ partition الخاصة بـ موضوع Kafka. نوع البيانات: `UInt64`.
* `_headers.name` — Array لمفاتيح ترويسات الرسالة. نوع البيانات: `Array(String)`.
* `_headers.value` — Array لقيم ترويسات الرسالة. نوع البيانات: `Array(String)`.

أعمدة افتراضية إضافية عندما تكون قيمة `kafka_handle_error_mode='stream'`:

* `_raw_message` - الرسالة الخام التي تعذّر parse لها بنجاح. نوع البيانات: `String`.
* `_error` - رسالة Exception التي حدثت أثناء parsing الفاشل. نوع البيانات: `String`.

ملاحظة: لا يتم ملء العمودين الافتراضيين `_raw_message` و `_error` إلا عند حدوث Exception أثناء parsing، ويظلان فارغين دائمًا عند parse الرسالة بنجاح.

<div id="mapping-columns-to-kafka-message-metadata">
  ## ربط الأعمدة بالبيانات الوصفية لرسائل Kafka
</div>

عند إنتاج الرسائل باستخدام `INSERT INTO`، يستخدم محرك Kafka دائمًا عمودًا باسم `_key` (من النوع `String`) كمفتاح لرسالة Kafka وعمودًا باسم `_timestamp` (من النوع `DateTime`) كطابع زمني لرسالة Kafka، إذا كانت هذه الأعمدة موجودة في الجدول. وبشكل افتراضي، تظهر هذه الأعمدة أيضًا في حمولة الرسالة المُنتَجة إلى جانب الأعمدة الأخرى.

مع `kafka_map_virtual_columns_on_write = 1`، يتغيّر السلوك:

* `_key` (النوع `String`) — يُربَط بمفتاح رسالة Kafka.
* `_timestamp` (النوع `DateTime`) — يُربَط بالطابع الزمني لرسالة Kafka.
* `_headers.name` (النوع `Array(String)`) و `_headers.value` (النوع `Array(String)`) — يُربَطان بـ ترويسات رسالة Kafka. ويصبح كل زوج `(_headers.name[i], _headers.value[i])` header واحدًا في Kafka. ولأن `_headers.name` و `_headers.value` يشتركان في البادئة المتداخلة `_headers`، فإن ClickHouse يتطلب أن يكون حجم المصفوفتين متماثلًا في كل صف.

تُستبعَد الأعمدة التي تحمل هذه الأسماء من **حمولة الرسالة** فقط إذا كانت أنواعها تطابق المذكور أعلاه؛ وإلا فإنها تبقى ضمن الحمولة، وبذلك تظل المخططات التي تصادف أنها تعيد استخدام هذه الأسماء لبيانات غير مرتبطة تعمل كما هي.

مثال:

```sql
CREATE TABLE kafka_out
(
    event_json String,
    `_key` String,
    `_timestamp` DateTime,
    `_headers.name` Array(String),
    `_headers.value` Array(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'events-producer',
    kafka_format = 'JSONEachRow',
    kafka_map_virtual_columns_on_write = 1;

INSERT INTO kafka_out VALUES
    ('{"a":1}', 'session-42', now(), ['source', 'trace_id'], ['api', 'abc-123']);
```

تتضمن رسالة Kafka المُنتَجة الحمولة `{"event_json":"{\"a\":1}"}`، والمفتاح `session-42`، والطابع الزمني الحالي، وترويستين هما `source=api` و `trace_id=abc-123`.

<div id="data-formats-support">
  ## دعم تنسيقات البيانات
</div>

يدعم محرك Kafka جميع [التنسيقات](../../../interfaces/formats.md) التي يدعمها ClickHouse.
يعتمد عدد الصفوف في رسالة Kafka واحدة على ما إذا كان التنسيق يعتمد على الصفوف أو على الكتل:

* بالنسبة إلى التنسيقات المعتمدة على الصفوف، يمكن التحكم في عدد الصفوف في رسالة Kafka واحدة عبر الإعداد `kafka_max_rows_per_message`.
* بالنسبة إلى التنسيقات المعتمدة على الكتل، لا يمكن تقسيم الكتلة إلى أجزاء أصغر، ولكن يمكن التحكم في عدد الصفوف في الكتلة الواحدة عبر الإعداد العام [max&#95;block&#95;size](/ar/operations/settings/settings#max_block_size).

<div id="engine-to-store-committed-offsets-in-clickhouse-keeper">
  ## محرك لتخزين الـ offsets المُثبَّتة في ClickHouse Keeper
</div>

<ExperimentalBadge />

إذا كان `allow_experimental_kafka_offsets_storage_in_keeper` مفعّلًا، فيمكن تحديد إعدادين إضافيين لمحرك جدول Kafka:

* يحدّد `kafka_keeper_path` المسار إلى الجدول في ClickHouse Keeper
* يحدّد `kafka_replica_name` اسم الـ replica في ClickHouse Keeper

يجب إما تحديد كلا الإعدادين أو عدم تحديد أيٍّ منهما. وعند تحديدهما معًا، سيُستخدم محرك Kafka جديد وتجريبي. لا يعتمد هذا المحرك الجديد على تخزين الـ offsets المُثبَّتة في Kafka، بل يخزّنها في ClickHouse Keeper. وهو لا يزال يحاول تنفيذ commit للـ offsets إلى Kafka، لكنه لا يعتمد على هذه الـ offsets إلا عند إنشاء الجدول. وفي أي حالة أخرى (مثل إعادة تشغيل الجدول أو استعادته بعد حدوث error)، ستُستخدم الـ offsets المخزّنة في ClickHouse Keeper كـ offset لمتابعة استهلاك الرسائل. وإلى جانب الـ offset المُثبَّتة، فإنه يخزّن أيضًا عدد الرسائل التي جرى استهلاكها في آخر Batch، بحيث إذا فشلت عملية insert، فسيُستهلك العدد نفسه من الرسائل، مما يتيح deduplication عند الحاجة.

مثال:

```sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/{uuid}',
  kafka_replica_name = '{replica}'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

<div id="known-limitations">
  ### القيود المعروفة
</div>

نظرًا لأن المحرك الجديد تجريبي، فهو ليس جاهزًا للاستخدام في بيئة الإنتاج بعد. وهناك بعض القيود المعروفة في هذا التنفيذ:

* قد يتسبب حذف الجدول بسرعة ثم إعادة إنشائه، أو تحديد مسار ClickHouse Keeper نفسه لمحركات مختلفة، في حدوث مشكلات. ومن أفضل الممارسات استخدام `{uuid}` في `kafka_keeper_path` لتجنب تعارض المسارات.
* لضمان قراءات قابلة للتكرار، لا يمكن استهلاك الرسائل من عدة أقسام على خيط تنفيذ واحد. ومن ناحية أخرى، يجب إجراء `poll` بانتظام لمستهلكات Kafka للحفاظ على بقائها نشطة. ونتيجةً لهذين المتطلبين، قررنا السماح بإنشاء عدة مستهلكات فقط إذا كان `kafka_thread_per_consumer` مُمكّنًا؛ وإلا فسيكون من المعقد جدًا تجنب المشكلات المتعلقة بإجراء `poll` للمستهلكات بانتظام.

**انظر أيضًا**

* [الأعمدة الافتراضية](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* [background&#95;message&#95;broker&#95;schedule&#95;pool&#95;size](/ar/operations/server-configuration-parameters/settings#background_message_broker_schedule_pool_size)
* [system.kafka&#95;consumers](../../../operations/system-tables/kafka_consumers.md)