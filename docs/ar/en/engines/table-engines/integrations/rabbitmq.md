---
description: 'يتيح هذا المحرك دمج ClickHouse مع RabbitMQ.'
sidebar_label: 'RabbitMQ'
sidebar_position: 170
slug: /engines/table-engines/integrations/rabbitmq
title: 'محرك جدول RabbitMQ'
doc_type: 'guide'
---

يتيح هذا المحرك دمج ClickHouse مع [RabbitMQ](https://www.rabbitmq.com).

يتيح لك `RabbitMQ` ما يلي:

* نشر تدفقات البيانات أو الاشتراك فيها.
* معالجة التدفقات فور توفرها.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port' [or rabbitmq_address = 'amqp(s)://guest:guest@localhost/vhost'],
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_secure = 0,]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N,]
    [rabbitmq_queue_settings_list = 'x-dead-letter-exchange=my-dlx,x-max-length=10,x-overflow=reject-publish',]
    [rabbitmq_queue_consume = false,]
    [rabbitmq_address = '',]
    [rabbitmq_vhost = '/',]
    [rabbitmq_username = '',]
    [rabbitmq_password = '',]
    [rabbitmq_commit_on_select = false,]
    [rabbitmq_max_rows_per_message = 1,]
    [rabbitmq_handle_error_mode = 'default']
```

المعلمات المطلوبة:

* `rabbitmq_host_port` – المضيف:المنفذ (على سبيل المثال، `localhost:5672`).
* `rabbitmq_exchange_name` – اسم الـ exchange في RabbitMQ.
* `rabbitmq_format` – تنسيق الرسالة. يستخدم الاصطلاح نفسه المستخدم في دالة SQL `FORMAT`، مثل `JSONEachRow`. لمزيد من المعلومات، راجع قسم [التنسيقات](../../../interfaces/formats.md).

المعلمات الاختيارية:

* `rabbitmq_exchange_type` – نوع exchange في RabbitMQ: `direct`, `fanout`, `topic`, `headers`, `consistent_hash`. القيمة الافتراضية: `fanout`.
* `rabbitmq_routing_key_list` – قائمة بمفاتيح التوجيه مفصولة بفواصل.
* `rabbitmq_schema` – معلمة يجب استخدامها إذا كان `format` يتطلب تعريف مخطط. على سبيل المثال، يتطلب [Cap&#39;n Proto](https://capnproto.org/) مسار ملف المخطط واسم الكائن الجذر `schema.capnp:Message`.
* `rabbitmq_num_consumers` – عدد المستهلكين لكل جدول. حدّد عدداً أكبر من المستهلكين إذا كان معدل النقل لمستهلك واحد غير كافٍ. القيمة الافتراضية: `1`
* `rabbitmq_num_queues` – العدد الإجمالي لقوائم الانتظار. يمكن أن تؤدي زيادة هذا العدد إلى تحسين الأداء بشكل كبير. القيمة الافتراضية: `1`.
* `rabbitmq_queue_base` - حدّد بادئةً لأسماء قوائم الانتظار. حالات استخدام هذا الإعداد موضحة أدناه.
* `rabbitmq_persistent` - إذا تم تعيينه إلى 1 (`true`)، فسيتم تعيين وضع تسليم insert query إلى 2 (ما يميّز الرسائل على أنها &#39;persistent&#39;). القيمة الافتراضية: `0`.
* `rabbitmq_skip_broken_messages` – مستوى تحمّل parser رسائل RabbitMQ للرسائل غير المتوافقة مع المخطط في كل block. إذا كانت `rabbitmq_skip_broken_messages = N`، فسيتخطى الـ engine عدد *N* من رسائل RabbitMQ التي يتعذر parse لها (كل رسالة تعادل row بيانات واحدة). القيمة الافتراضية: `0`.
* `rabbitmq_max_block_size` - عدد الـ row التي يتم جمعها قبل flush البيانات من RabbitMQ. القيمة الافتراضية: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `rabbitmq_flush_interval_ms` - مهلة flush البيانات من RabbitMQ. القيمة الافتراضية: [stream&#95;flush&#95;interval&#95;ms](/ar/operations/settings/settings#stream_flush_interval_ms).
* `rabbitmq_queue_settings_list` - يتيح تعيين إعدادات RabbitMQ عند إنشاء قائمة انتظار. الإعدادات المتاحة: `x-max-length`, `x-max-length-bytes`, `x-message-ttl`, `x-expires`, `x-priority`, `x-max-priority`, `x-overflow`, `x-dead-letter-exchange`, `x-queue-type`. يتم تمكين الإعداد `durable` تلقائياً لقائمة الانتظار.
* `rabbitmq_address` - عنوان الاتصال. استخدم هذا الإعداد أو `rabbitmq_host_port`.
* `rabbitmq_vhost` - قيمة vhost في RabbitMQ. القيمة الافتراضية: `'/'`.
* `rabbitmq_queue_consume` - استخدم قوائم انتظار معرّفة من قِبل المستخدم ولا تُجرِ أي إعداد في RabbitMQ، مثل تعريف exchanges أو queues أو bindings. القيمة الافتراضية: `false`.
* `rabbitmq_username` - اسم مستخدم RabbitMQ.
* `rabbitmq_password` - كلمة مرور RabbitMQ.
* `reject_unhandled_messages` - ارفض الرسائل (أرسل إقراراً سلبياً إلى RabbitMQ) عند حدوث أخطاء. يتم تمكين هذا الإعداد تلقائياً إذا كان `x-dead-letter-exchange` معرّفاً في `rabbitmq_queue_settings_list`.
* `rabbitmq_commit_on_select` - نفّذ commit للرسائل عند تنفيذ select query. القيمة الافتراضية: `false`.
* `rabbitmq_max_rows_per_message` — الحد الأقصى لعدد الـ rows المكتوبة في رسالة RabbitMQ واحدة في تنسيقات المعتمدة على الصفوف. القيمة الافتراضية: `1`.
* `rabbitmq_empty_queue_backoff_start_ms` — نقطة بداية backoff لإعادة جدولة القراءة إذا كانت قائمة انتظار RabbitMQ فارغة.
* `rabbitmq_empty_queue_backoff_end_ms` — نقطة نهاية backoff لإعادة جدولة القراءة إذا كانت قائمة انتظار RabbitMQ فارغة.
* `rabbitmq_empty_queue_backoff_step_ms` — قيمة خطوة backoff لإعادة جدولة القراءة إذا كانت قائمة انتظار RabbitMQ فارغة.
* `rabbitmq_handle_error_mode` — كيفية التعامل مع الأخطاء في محرك RabbitMQ. القيم الممكنة: default (سيتم throw للاستثناء إذا تعذّر parse رسالة)، stream (سيتم حفظ رسالة الاستثناء والرسالة الخام في virtual columns `_error` و`_raw_message`)، dead&#95;letter&#95;queue (سيتم حفظ البيانات المتعلقة بالخطأ في system.dead&#95;letter&#95;queue).

<div id="ssl-connection">
  ### اتصال SSL
</div>

استخدم إما `rabbitmq_secure = 1` أو `amqps` في عنوان connection: `rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`.
السلوك الافتراضي للمكتبة المستخدمة هو عدم التحقق مما إذا كان اتصال TLS المُنشأ آمنًا بالقدر الكافي. سواء كانت الشهادة منتهية الصلاحية أو موقَّعة ذاتيًا أو مفقودة أو غير صالحة، يُسمح بالاتصال ببساطة. وقد يُطبَّق في المستقبل تحقّق أكثر صرامة من الشهادات.

يمكن أيضًا إضافة إعدادات format إلى جانب الإعدادات المتعلقة بـ rabbitmq.

مثال:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5,
                            date_time_input_format = 'best_effort';
```

يجب إضافة تكوين خادم RabbitMQ باستخدام ملف إعدادات ClickHouse.

التكوين المطلوب:

```xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

إعدادات إضافية:

```xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

<div id="description">
  ## الوصف
</div>

لا يُعد `SELECT` مفيدًا بشكل خاص لقراءة الرسائل (إلا لأغراض Debug)، لأن كل رسالة لا يمكن قراءتها إلا مرة واحدة. ومن العملي أكثر إنشاء تدفقات في الوقت الفعلي باستخدام [العروض المادية](../../../sql-reference/statements/create/view.md). وللقيام بذلك:

1. استخدم المحرّك لإنشاء مستهلك RabbitMQ واعتبره دفق بيانات.
2. أنشئ جدولًا بالبنية المطلوبة.
3. أنشئ عرضًا ماديًا يحوّل البيانات من المحرّك ويضعها في جدول تم إنشاؤه مسبقًا.

عند ربط `MATERIALIZED VIEW` بالمحرّك، يبدأ في جمع البيانات في الخلفية. يتيح لك ذلك الاستمرار في تلقي الرسائل من RabbitMQ وتحويلها إلى التنسيق المطلوب باستخدام `SELECT`.
يمكن أن يحتوي جدول RabbitMQ واحد على أي عدد تريده من العروض المادية.

يمكن توجيه البيانات استنادًا إلى `rabbitmq_exchange_type` و`rabbitmq_routing_key_list` المحدَّدين.
لا يمكن أن يوجد أكثر من exchange واحد لكل جدول. ويمكن مشاركة exchange واحد بين عدة جداول، ما يتيح التوجيه إلى عدة جداول في الوقت نفسه.

خيارات نوع الـ exchange:

* `direct` - يعتمد التوجيه على التطابق التام للمفاتيح. مثال على قائمة مفاتيح الجدول: `key1,key2,key3,key4,key5`، ويمكن أن يساوي مفتاح الرسالة أيًا منها.
* `fanout` - توجيه إلى جميع الجداول (حيث يكون اسم الـ exchange نفسه) بغض النظر عن المفاتيح.
* `topic` - يعتمد التوجيه على أنماط بمفاتيح مفصولة بنقاط. أمثلة: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
* `headers` - يعتمد التوجيه على تطابقات `key=value` مع الإعداد `x-match=all` أو `x-match=any`. مثال على قائمة مفاتيح الجدول: `x-match=all,format=logs,type=report,year=2020`.
* `consistent_hash` - تُوزَّع البيانات بالتساوي بين جميع الجداول المرتبطة (حيث يكون اسم الـ exchange نفسه). لاحظ أنه يجب تمكين نوع الـ exchange هذا باستخدام إضافة RabbitMQ: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

يمكن استخدام الإعداد `rabbitmq_queue_base` في الحالات التالية:

* للسماح لجداول مختلفة بمشاركة قوائم انتظار، بحيث يمكن تسجيل عدة مستهلكين للـ قوائم الانتظار نفسها، مما يحسّن الأداء. وعند استخدام الإعدادين `rabbitmq_num_consumers` و/أو `rabbitmq_num_queues`، يتحقق التطابق التام للـ قوائم الانتظار إذا كانت هذه المعلمات متماثلة.
* للتمكن من استئناف القراءة من قوائم انتظار دائمة معيّنة عندما لا يتم استهلاك جميع الرسائل بنجاح. ولاستئناف الاستهلاك من قائمة انتظار محدد، عيّن اسمه في الإعداد `rabbitmq_queue_base` ولا تحدد `rabbitmq_num_consumers` و`rabbitmq_num_queues` (القيمة الافتراضية هي 1). ولاستئناف الاستهلاك من جميع الـ قوائم الانتظار التي أُعلنت لجدول معيّن، ما عليك سوى تحديد الإعدادات نفسها: `rabbitmq_queue_base` و`rabbitmq_num_consumers` و`rabbitmq_num_queues`. افتراضيًا، ستكون أسماء الـ قوائم الانتظار فريدة لكل جدول.
* لإعادة استخدام الـ قوائم الانتظار لأنها مُعلنة كـ durable ولا تُحذف تلقائيًا. (يمكن حذفها عبر أي من أدوات RabbitMQ CLI.)

لتحسين الأداء، تُجمَّع الرسائل المستلمة في blocks بحجم [max&#95;insert&#95;block&#95;size](/ar/operations/settings/settings#max_insert_block_size). وإذا لم يكتمل تكوين الـ block خلال [stream&#95;flush&#95;interval&#95;ms](../../../operations/server-configuration-parameters/settings.md) مللي ثانية، فستُكتب البيانات إلى الجدول بغض النظر عن اكتمال الـ block.

إذا تم تحديد الإعدادين `rabbitmq_num_consumers` و/أو `rabbitmq_num_queues` مع `rabbitmq_exchange_type`، فعندئذٍ:

* يجب تمكين إضافة `rabbitmq-consistent-hash-exchange`.
* يجب تحديد الخاصية `message_id` للرسائل المنشورة (وتكون فريدة لكل رسالة/Batch).

بالنسبة إلى insert query، توجد metadata للرسالة تُضاف إلى كل رسالة منشورة: `messageID` والعَلم `republished` (تكون قيمته true إذا نُشرت أكثر من مرة) — ويمكن الوصول إليهما عبر headers الرسالة.

لا تستخدم الجدول نفسه لعمليات inserts والعروض المادية.

Example:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_exchange_type = 'headers',
                            rabbitmq_routing_key_list = 'format=logs,type=report,year=2020',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_exchange_name` - اسم الـ exchange في RabbitMQ. نوع البيانات: `String`.
* `_channel_id` - معرّف القناة (ChannelID) التي أُعلن عليها المستهلك الذي استلم الرسالة. نوع البيانات: `String`.
* `_delivery_tag` - قيمة DeliveryTag للرسالة المستلمة؛ ونطاقها يقتصر على كل قناة. نوع البيانات: `UInt64`.
* `_redelivered` - العلامة `redelivered` الخاصة بالرسالة. نوع البيانات: `UInt8`.
* `_message_id` - معرّف الرسالة (messageID) للرسالة المستلمة؛ ويكون غير فارغ إذا تم تعيينه عند نشر الرسالة. نوع البيانات: `String`.
* `_timestamp` - الطابع الزمني للرسالة المستلمة؛ ويكون غير فارغ إذا تم تعيينه عند نشر الرسالة. نوع البيانات: `UInt64`.

أعمدة افتراضية إضافية عندما تكون القيمة `rabbitmq_handle_error_mode='stream'`:

* `_raw_message` - الرسالة الخام التي تعذر تحليلها بنجاح. نوع البيانات: `Nullable(String)`.
* `_error` - رسالة الاستثناء التي ظهرت أثناء فشل التحليل. نوع البيانات: `Nullable(String)`.

ملاحظة: لا يتم ملء العمودين الافتراضيين `_raw_message` و `_error` إلا عند حدوث استثناء أثناء التحليل، ويكونان دائمًا `NULL` عند تحليل الرسالة بنجاح.

<div id="caveats">
  ## محاذير
</div>

على الرغم من أنه يمكنك تحديد [تعبيرات الأعمدة الافتراضية](/ar/sql-reference/statements/create/table.md/#default_values) (مثل `DEFAULT` و`MATERIALIZED` و`ALIAS`) في تعريف الجدول، فسيجري تجاهلها. وبدلًا من ذلك، ستُملأ الأعمدة بالقيم الافتراضية المقابلة لأنواعها.

<div id="data-formats-support">
  ## دعم تنسيقات البيانات
</div>

يدعم محرك RabbitMQ جميع [التنسيقات](../../../interfaces/formats.md) التي يدعمها ClickHouse.
يعتمد عدد الصفوف في رسالة RabbitMQ الواحدة على ما إذا كان التنسيق يعتمد على الصفوف أو على الكتل:

* بالنسبة إلى التنسيقات المعتمدة على الصفوف، يمكن التحكم في عدد الصفوف في رسالة RabbitMQ واحدة عبر الإعداد `rabbitmq_max_rows_per_message`.
* بالنسبة إلى التنسيقات المعتمدة على الكتل، لا يمكن تقسيم block إلى أجزاء أصغر، ولكن يمكن التحكم في عدد الصفوف في block واحد عبر الإعداد العام [max&#95;block&#95;size](/ar/operations/settings/settings#max_block_size).