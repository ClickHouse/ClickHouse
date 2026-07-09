---
description: 'يوفّر هذا المحرك تكاملًا مع منظومة Amazon S3 ويتيح الاستيراد المتدفق.
  وهو مشابه لمحركَي Kafka وRabbitMQ، لكنه يوفّر ميزات خاصة بـ S3.'
sidebar_label: 'S3Queue'
sidebar_position: 181
slug: /engines/table-engines/integrations/s3queue
title: 'محرك الجدول S3Queue'
doc_type: 'reference'
---

import ScalePlanFeatureBadge from '@theme/badges/ScalePlanFeatureBadge'

<div id="s3queue-table-engine">
  # محرك الجدول S3Queue
</div>

يوفّر هذا المحرك تكاملًا مع منظومة [Amazon S3](https://aws.amazon.com/s3/) ويتيح الاستيراد المتدفق. وهو مشابه لمحركي [Kafka](../../../engines/table-engines/integrations/kafka.md) و[RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md)، لكنه يوفّر ميزات خاصة بـ S3.

من المهم فهم هذه الملاحظة الواردة في [طلب السحب الأصلي لتنفيذ S3Queue](https://github.com/ClickHouse/ClickHouse/pull/49086/files#diff-e1106769c9c8fbe48dd84f18310ef1a250f2c248800fde97586b3104e9cd6af8R183): عند ربط `MATERIALIZED VIEW` بهذا المحرك، يبدأ محرك الجدول S3Queue في جمع البيانات في الخلفية.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE s3_queue_engine_table (name String, value UInt32)
    ENGINE = S3Queue(path, [NOSIGN, | aws_access_key_id, aws_secret_access_key,] format, [compression], [headers], [extra_credentials])
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    [loading_retries = 10,]
    [processing_threads_num = 16,]
    [parallel_inserts = false,]
    [enable_logging_to_queue_log = true,]
    [last_processed_path = "",]
    [tracked_files_limit = 1000,]
    [tracked_file_ttl_sec = 0,]
    [polling_min_timeout_ms = 1000,]
    [polling_max_timeout_ms = 600000,]
    [polling_backoff_ms = 30000,]
    [cleanup_interval_min_ms = 60000,]
    [cleanup_interval_max_ms = 60000,]
    [buckets = 0,]
    [list_objects_batch_size = 1000,]
    [enable_hash_ring_filtering = 0,]
    [max_processed_files_before_commit = 100,]
    [max_processed_rows_before_commit = 0,]
    [max_processed_bytes_before_commit = 0,]
    [max_processing_time_sec_before_commit = 0,]
```

:::warning
قبل الإصدار `24.7`، يجب استخدام البادئة `s3queue_` لجميع الإعدادات باستثناء `mode` و `after_processing` و `keeper_path`.
:::

**معلمات المحرك**

معلمات `S3Queue` هي نفسها المعلمات التي يدعمها محرك الجدول `S3`. راجع قسم المعلمات [هنا](../../../engines/table-engines/integrations/s3.md#parameters).

**مثال**

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered';
```

باستخدام named collections:

```xml
<clickhouse>
    <named_collections>
        <s3queue_conf>
            <url>https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </s3queue_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue(s3queue_conf, format = 'CSV', compression_method = 'gzip')
SETTINGS
    mode = 'ordered';
```

<div id="settings">
  ## الإعدادات
</div>

للحصول على قائمة بالإعدادات المهيأة للجدول، استخدم جدول `system.s3_queue_settings`. وهو متاح بدءًا من `24.10`.

:::note أسماء الإعدادات (24.7+)
بدءًا من الإصدار 24.7، يمكن تحديد إعدادات S3Queue بالبادئة `s3queue_` أو بدونها:

* **الصيغة الحديثة** (24.7+): `processing_threads_num`, `tracked_file_ttl_sec`، إلخ.
* **الصيغة القديمة** (جميع الإصدارات): `s3queue_processing_threads_num`, `s3queue_tracked_file_ttl_sec`، إلخ.

كلتا الصيغتين مدعومتان في 24.7+. وتستخدم الأمثلة في هذه الصفحة الصيغة الحديثة من دون بادئة.
:::

<div id="mode">
  ### الوضع
</div>

القيم الممكنة:

* unordered — في وضع unordered، يُتتبَّع جميع الملفات التي تمت معالجتها بالفعل باستخدام عقد دائمة في ZooKeeper.
* ordered — في وضع ordered، تُعالَج الملفات بترتيب معجمي. وهذا يعني أنه إذا تمت معالجة ملف باسم &#39;BBB&#39; في وقتٍ ما ثم أُضيف لاحقًا ملف باسم &#39;AA&#39; إلى الـ حاوية، فسيتم تجاهله. ولا يُخزَّن في ZooKeeper إلا أكبر اسم (بالمعنى المعجمي) للملف الذي تمت قراءته بنجاح، وأسماء الملفات التي ستُعاد محاولة معالجتها بعد فشل محاولة التحميل.

القيمة الافتراضية: `ordered` في الإصدارات السابقة لـ 24.6. واعتبارًا من 24.6، لم تعد هناك قيمة افتراضية، وأصبح من الضروري تحديد هذا الإعداد يدويًا. أما بالنسبة إلى الجداول التي أُنشئت في إصدارات أقدم، فستظل القيمة الافتراضية `Ordered` حفاظًا على التوافق.

<div id="after_processing">
  ### `after_processing`
</div>

كيفية التعامل مع الملف بعد نجاح المعالجة.

القيم الممكنة:

* keep.
* delete.
* move.
* tag.

القيمة الافتراضية: `keep`.

يتطلب `move` إعدادات إضافية. في حال النقل داخل الحاوية نفسها، يجب توفير بادئة مسار جديدة على شكل `after_processing_move_prefix`.

أما النقل إلى حاوية S3 أخرى، فيتطلب تحديد URI لحاوية الوجهة على شكل `after_processing_move_uri`، وبيانات اعتماد S3 على شكل `after_processing_move_access_key_id` و`after_processing_move_secret_access_key`.

مثال:

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered',
    after_processing = 'move',
    after_processing_retries = 20,
    after_processing_move_prefix = 'dst_prefix',
    after_processing_move_uri = 'https://clickhouse-public-datasets.s3.amazonaws.com/dst-bucket',
    after_processing_move_access_key_id = 'test',
    after_processing_move_secret_access_key = 'test';
```

يتطلب النقل من حاوية Azure إلى حاوية Azure أخرى استخدام سلسلة اتصال Blob Storage بصفتها `after_processing_move_connection_string`، واسم الحاوية بصفته `after_processing_move_container`. راجع [إعدادات AzureQueue](../../../engines/table-engines/integrations/azure-queue.md#settings).

يتطلب الوسم توفير مفتاح الوسم وقيمته على أنهما `after_processing_tag_key` و`after_processing_tag_value`.

<div id="after_processing_retries">
  ### `after_processing_retries`
</div>

عدد محاولات إعادة تنفيذ الإجراء المطلوب بعد المعالجة قبل الاستسلام.

القيم الممكنة:

* عدد صحيح غير سالب.

القيمة الافتراضية: `10`.

<div id="after_processing_move_access_key_id">
  ### `after_processing_move_access_key_id`
</div>

معرّف مفتاح الوصول لحاوية S3 التي ستُنقل إليها الملفات المُعالجة بنجاح، إذا كانت الوجهة حاوية S3 أخرى.

القيم الممكنة:

* String.

القيمة الافتراضية: سلسلة فارغة.

<div id="after_processing_move_prefix">
  ### `after_processing_move_prefix`
</div>

بادئة المسار التي تُنقل إليها الملفات التي تمت معالجتها بنجاح. ينطبق ذلك في كلتا الحالتين: النقل داخل حاوية نفسه أو إلى حاوية آخر.

القيم الممكنة:

* String.

القيمة الافتراضية: سلسلة فارغة.

<div id="after_processing_move_preserve_path">
  ### `after_processing_move_preserve_path`
</div>

إذا كانت القيمة `true`، فسيُضاف مسار الكائن المصدر بالكامل إلى `after_processing_move_prefix` عند نقل ملف تمت معالجته بنجاح، بحيث يُحفَظ هيكل الدليل المصدر داخل الـ حاوية في الوجهة. وإذا كانت القيمة `false`، فسيُستخدَم اسم الملف فقط ويُسطَّح هيكل الدليل المصدر.

القيم الممكنة:

* `true` / `false`.

القيمة الافتراضية: `false`.

<div id="after_processing_move_secret_access_key">
  ### `after_processing_move_secret_access_key`
</div>

مفتاح الوصول السري لحاوية S3 التي ستُنقل إليها الملفات التي تمت معالجتها بنجاح، إذا كانت الوجهة حاوية S3 أخرى.

القيم الممكنة:

* String.

القيمة الافتراضية: سلسلة فارغة.

<div id="after_processing_move_uri">
  ### `after_processing_move_uri`
</div>

عنوان URI لحاوية S3 التي تُنقل إليها الملفات التي تمت معالجتها بنجاح، إذا كانت الوجهة حاوية S3 أخرى.

القيم الممكنة:

* String.

القيمة الافتراضية: سلسلة فارغة.

<div id="after_processing_tag_key">
  ### `after_processing_tag_key`
</div>

مفتاح الوسم المُستخدَم لإضافة وسم إلى الملفات التي تمت معالجتها بنجاح، إذا كانت قيمة `after_processing='tag'`.

القيم الممكنة:

* String.

القيمة الافتراضية: سلسلة فارغة.

<div id="after_processing_tag_value">
  ### `after_processing_tag_value`
</div>

قيمة الوسم التي ستُطبَّق على الملفات التي تمت معالجتها بنجاح، إذا كانت `after_processing='tag'`.

القيم الممكنة:

* String.

القيمة الافتراضية: سلسلة فارغة.

<div id="keeper_path">
  ### `keeper_path`
</div>

المسار إلى البيانات الوصفية للطابور في ZooKeeper. إذا لم يُحدَّد صراحةً، فسيبني ClickHouse المسار من `s3queue_default_zookeeper_path`، ومعرّف UUID لقاعدة البيانات، ومعرّف UUID للجدول. تُستخدم القيم المطلقة (التي تبدأ بـ `/`) كما هي، بينما تُلحَق القيم النسبية بالبادئة المُعدّة. وتُوسَّع وحدات الماكرو مثل `{database}` أو `{uuid}` قبل أن يتصل المحرّك بـ ZooKeeper.

لاستهداف عنقود ZooKeeper إضافي، أضِف إلى بداية القيمة الاسم المُعدّ، على سبيل المثال `analytics_keeper:/clickhouse/queue/orders`. يجب أن يكون الاسم موجودًا في `<auxiliary_zookeepers>`؛ وإلا فسيعرض المحرّك الرسالة `Unknown auxiliary ZooKeeper name ...`. ويُحفَظ النص الكامل (بما في ذلك البادئة) في `SHOW CREATE TABLE` بحيث يمكن نسخ التعليمة حرفيًا كما هي.

القيم الممكنة:

* String.

القيمة الافتراضية: `/`.

<div id="loading_retries">
  ### `loading_retries`
</div>

إعادة محاولة تحميل الملف حتى عدد المرات المحدد.
القيم الممكنة:

* عدد صحيح غير سالب.

القيمة الافتراضية: `10`.

<div id="processing_threads_num">
  ### `processing_threads_num`
</div>

عدد الخيوط المستخدمة في المعالجة. ينطبق فقط على وضع `Unordered`.

القيمة الافتراضية: عدد وحدات المعالجة المركزية (CPU) أو 16.

<div id="parallel_inserts">
  ### `parallel_inserts`
</div>

بشكل افتراضي، سينفّذ `processing_threads_num` عملية `INSERT` واحدة، لذا سيقتصر الأمر على تنزيل الملفات وتحليلها باستخدام عدة خيوط.
لكن هذا يحدّ من التوازي، لذا للحصول على إنتاجية أفضل استخدم `parallel_inserts=true`، إذ يتيح ذلك إدراج البيانات بالتوازي (مع الانتباه إلى أن هذا سيؤدي إلى إنشاء عدد أكبر من أجزاء البيانات لعائلة MergeTree).

:::note
سيتم إنشاء عمليات `INSERT` وفقًا لإعدادات `max_process*_before_commit`.
:::

القيمة الافتراضية: `false`.

<div id="enable_logging_to_queue_log">
  ### `enable_logging_to_queue_log`
</div>

يُفعِّل التسجيل في `system.s3queue_log`.

القيمة الافتراضية: `1`.

<div id="polling_min_timeout_ms">
  ### `polling_min_timeout_ms`
</div>

يحدّد الحد الأدنى للوقت، بالمللي ثانية، الذي ينتظره ClickHouse قبل إجراء محاولة الاستقصاء التالية.

القيم الممكنة:

* عدد صحيح موجب.

القيمة الافتراضية: `1000`.

<div id="polling_max_timeout_ms">
  ### `polling_max_timeout_ms`
</div>

يحدّد الحد الأقصى للوقت، بالملي ثانية، الذي ينتظره ClickHouse قبل بدء محاولة الاستقصاء التالية.

القيم الممكنة:

* عدد صحيح موجب.

القيمة الافتراضية: `600000`.

<div id="polling_backoff_ms">
  ### `polling_backoff_ms`
</div>

يحدِّد مدة الانتظار الإضافية التي تُضاف إلى فاصل الاستقصاء السابق عند عدم العثور على ملفات جديدة. وتتم عملية الاستقصاء التالية بعد مجموع الفاصل السابق وقيمة backoff هذه، أو بعد الفاصل الأقصى، أيهما أقل.

القيم الممكنة:

* عدد صحيح موجب.

القيمة الافتراضية: `30000`.

<div id="tracked_files_limit">
  ### `tracked_files_limit`
</div>

يتيح تقييد عدد عُقد ZooKeeper عند استخدام وضع &#39;unordered&#39;، ولا يكون له أي تأثير في وضع &#39;ordered&#39;.
عند الوصول إلى الحد، تُحذف أقدم الملفات المُعالجة من عقدة ZooKeeper وتُعالَج مرة أخرى.

القيم الممكنة:

* عدد صحيح موجب.

القيمة الافتراضية: `1000`.

<div id="tracked_file_ttl_sec">
  ### `tracked_file_ttl_sec`
</div>

الحد الأقصى لعدد الثواني لتخزين الملفات المُعالجة في عقدة ZooKeeper (يُخزَّن إلى الأبد افتراضيًا) في وضع &#39;unordered&#39;، ولا تكون له أي فعالية في وضع &#39;ordered&#39;.
بعد انقضاء عدد الثواني المحدد، سيُعاد استيراد الملف.

القيم الممكنة:

* عدد صحيح موجب.

القيمة الافتراضية: `0`.

<div id="cleanup_interval_min_ms">
  ### `cleanup_interval_min_ms`
</div>

في وضع &#39;Ordered&#39;. يحدّد الحد الأدنى لفاصل إعادة الجدولة لمهمة تعمل في الخلفية، والمسؤولة عن الحفاظ على TTL للملفات المتتبعة والحد الأقصى لمجموعة الملفات المتتبعة.

القيمة الافتراضية: `60000`.

<div id="cleanup_interval_max_ms">
  ### `cleanup_interval_max_ms`
</div>

للوضع &#39;Ordered&#39;. يحدّد الحد الأقصى لفترة إعادة الجدولة لمهمة تعمل في الخلفية، والمسؤولة عن إدارة TTL للملفات المتتبَّعة والحد الأقصى لمجموعة الملفات المتتبَّعة.

القيمة الافتراضية: `60000`.

<div id="buckets">
  ### `buckets`
</div>

لوضع &#39;Ordered&#39;. متاح منذ `24.6`. إذا كان هناك عدة نُسخ متماثلة لجدول S3Queue، وكانت جميعها تعمل مع دليل البيانات الوصفية نفسه في Keeper، فيجب أن تكون قيمة `buckets` مساويةً على الأقل لعدد النُسخ المتماثلة. وإذا استُخدم أيضًا الإعداد `processing_threads`، فمن المنطقي زيادة قيمة الإعداد `buckets` أكثر، لأنه يحدد درجة التوازي الفعلية لمعالجة `S3Queue`.

<div id="use_persistent_processing_nodes">
  ### `use_persistent_processing_nodes`
</div>

بشكل افتراضي، كان جدول S3Queue يستخدم دائمًا عُقد معالجة مؤقتة، ما قد يؤدي إلى ظهور بيانات مكررة إذا انتهت صلاحية جلسة ZooKeeper قبل أن يُتمّ S3Queue تثبيت الملفات المُعالجة في ZooKeeper، ولكن بعد أن يكون قد بدأ معالجتها. يفرض هذا الإعداد على الخادوم منع احتمال حدوث تكرارات عند انتهاء صلاحية جلسة Keeper.

<div id="persistent_processing_node_ttl_seconds">
  ### `persistent_processing_node_ttl_seconds`
</div>

في حال إنهاء الخادم بشكل غير سليم، قد يحدث أنه إذا كان `use_persistent_processing_nodes` مفعّلًا، فتبقى بعض عُقد المعالجة بدون إزالة. يحدّد هذا الإعداد فترة زمنية يمكن خلالها تنظيف عُقد المعالجة هذه بأمان. ويُستخدم TTL نفسه أيضًا لقفل الـ حاوية في وضع `Ordered`، والذي قد يظل محتفَظًا به لمدة أطول من عقدة معالجة واحدة، لذا ينبغي أن تأخذ القيمة ذلك في الاعتبار أيضًا.

القيمة الافتراضية: `21600` (6 ساعات).

<div id="s3-settings">
  ## الإعدادات المتعلقة بـ S3
</div>

يدعم هذا المحرّك جميع الإعدادات المتعلقة بـ S3. لمزيد من المعلومات حول إعدادات S3، راجع [هنا](../../../engines/table-engines/integrations/s3.md).

<div id="s3-role-based-access">
  ## الوصول المستند إلى الأدوار في S3
</div>

<ScalePlanFeatureBadge feature="الوصول المستند إلى الأدوار في S3" />

يدعم محرك الجدول S3Queue الوصول المستند إلى الأدوار.
راجِع الوثائق [هنا](/ar/cloud/data-sources/secure-s3) للتعرّف على خطوات تهيئة دور للوصول إلى حاوية التخزين الخاصة بك.

بمجرد تهيئة الدور، يمكن تمرير `roleARN` من خلال المعلَمة `extra_credentials` كما هو موضح أدناه:

```sql
CREATE TABLE s3_table
(
    ts DateTime,
    value UInt64
)
ENGINE = S3Queue(
                'https://<your_bucket>/*.csv',
                extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/<your_role>')
                ,'CSV')
SETTINGS
    ...
```

<div id="ordered-mode">
  ## وضع `Ordered` في S3Queue
</div>

يتيح وضع المعالجة `S3Queue` تخزين قدر أقل من البيانات الوصفية في ZooKeeper، لكنه يفرض قيدًا يتمثل في أن الملفات التي تُضاف لاحقًا يجب أن تكون أسماؤها أكبر ترتيبًا أبجديًا رقميًا.

يدعم وضع `S3Queue` `ordered`، وكذلك `unordered`، الإعداد `(s3queue_)processing_threads_num` (وتكون البادئة `s3queue_` اختيارية)، والذي يتيح التحكم في عدد خيوط التنفيذ التي ستعالج ملفات `S3` محليًا على الخادم.

بالنسبة إلى وضع `ordered` من دون تقسيم، قد يستأنف ClickHouse سرد كائنات S3 من آخر `key` تمت معالجته لتجنّب إعادة سرد كامل سجل البادئة.
وفي وضع `ordered` المعتمد على `buckets`، تُختار نقطة الاستئناف بشكل متحفّظ على أنها أصغر `key` تمت معالجته عبر جميع `buckets` لتجنّب تخطي الملفات غير المعالجة.
ولا يُستخدم تحسين استئناف السرد هذا إلا مع الطوابير المعتمدة على S3 في وضع `ordered` من دون تقسيم (وليس مع AzureQueue أو عند تعيين `partitioning_mode`).
بالإضافة إلى ذلك، يقدّم وضع `ordered` أيضًا إعدادًا آخر يسمى `(s3queue_)buckets` ويعني &quot;خيوطًا منطقية&quot;. وهذا يعني أنه في بيئة موزعة، عندما توجد عدة خوادم تحتوي على نُسخ متماثلة من جدول `S3Queue`، فإن هذا الإعداد يحدد عدد وحدات المعالجة. فعلى سبيل المثال، سيحاول كل خيط معالجة في كل نسخة متماثلة من `S3Queue` قفل `حاوية` معيّن للمعالجة، ويُسنَد كل `حاوية` إلى ملفات معيّنة استنادًا إلى `hash` اسم الملف. لذلك، في البيئات الموزعة، يُوصى بشدة بأن تكون قيمة الإعداد `(s3queue_)buckets` مساوية على الأقل لعدد النسخ المتماثلة أو أكبر منه. ولا توجد مشكلة في أن يكون عدد `buckets` أكبر من عدد النسخ المتماثلة. وتكون الحالة المثلى عندما تساوي قيمة الإعداد `(s3queue_)buckets` حاصل ضرب `number_of_replicas` و `(s3queue_)processing_threads_num`.
لا يُنصح باستخدام الإعداد `(s3queue_)processing_threads_num` قبل الإصدار `24.6`.
يتوفر الإعداد `(s3queue_)buckets` بدءًا من الإصدار `24.6`.

<div id="select">
  ## SELECT من محرك الجدول S3Queue
</div>

تكون استعلامات SELECT محظورة افتراضيًا على جداول S3Queue. ويتماشى ذلك مع نمط قائمة الانتظار الشائع، حيث تُقرأ البيانات مرة واحدة ثم تُزال من قائمة الانتظار. ويُحظر SELECT لمنع فقدان البيانات غير المقصود.
ومع ذلك، قد يكون هذا مفيدًا في بعض الأحيان. وللقيام بذلك، تحتاج إلى ضبط الإعداد `stream_like_engine_allow_direct_select` على `True`.
يحتوي محرك S3Queue على إعداد خاص لاستعلامات SELECT: `commit_on_select`. اضبطه على `False` للاحتفاظ بالبيانات في قائمة الانتظار بعد قراءتها، أو على `True` لإزالتها.

<div id="description">
  ## الوصف
</div>

لا يُعد `SELECT` مفيدًا كثيرًا في الاستيراد المتدفق (إلا لأغراض تصحيح الأخطاء)، لأن كل ملف لا يمكن استيراده إلا مرة واحدة. والأكثر عملية هو إنشاء مسارات معالجة آنية باستخدام [العروض المادية](../../../sql-reference/statements/create/view.md). للقيام بذلك:

1. استخدم المحرك لإنشاء جدول يستهلك البيانات من المسار المحدد في S3، واعتبره تدفق بيانات.
2. أنشئ جدولًا بالبنية المطلوبة.
3. أنشئ عرضًا ماديًا يحوّل البيانات من المحرك ويضعها في جدول أُنشئ مسبقًا.

عند ربط `MATERIALIZED VIEW` بالمحرك، يبدأ في جمع البيانات في الخلفية.

مثال:

```sql
  CREATE TABLE s3queue_engine_table (name String, value UInt32)
    ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
    SETTINGS
        mode = 'unordered';

  CREATE TABLE stats (name String, value UInt32)
    ENGINE = MergeTree() ORDER BY name;

  CREATE MATERIALIZED VIEW consumer TO stats
    AS SELECT name, value FROM s3queue_engine_table;

  SELECT * FROM stats ORDER BY name;
```

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف.
* `_file` — اسم الملف.
* `_size` — حجم الملف.
* `_time` — وقت إنشاء الملف.

لمزيد من المعلومات عن الأعمدة الافتراضية، راجع [هذا القسم](../../../engines/table-engines/index.md#table_engines-virtual_columns).

<div id="wildcards-in-path">
  ## أحرف البدل في المسار
</div>

يمكن للوسيطة `path` تحديد عدة ملفات باستخدام أحرف بدل شبيهة بتلك المستخدمة في bash. ولكي يُعالَج الملف، يجب أن يكون موجودًا وأن يطابق نمط المسار بالكامل. ويُحدَّد سرد الملفات أثناء `SELECT` (وليس عند `CREATE`).

* `*` — يستبدل أي عدد من المحارف باستثناء `/`، بما في ذلك السلسلة الفارغة.
* `**` — يستبدل أي عدد من المحارف، بما في ذلك `/` وبما في ذلك السلسلة الفارغة.
* `?` — يستبدل أي محرف واحد.
* `{some_string,another_string,yet_another_one}` — يستبدل أيًّا من السلاسل `'some_string', 'another_string', 'yet_another_one'`.
* `{N..M}` — يستبدل أي رقم في النطاق من N إلى M، بما في ذلك الحدّان. ويمكن أن تحتوي N وM على أصفار بادئة، مثل `000..078`.

البُنى التي تستخدم `{}` مشابهة لدالة الجدول [remote](../../../sql-reference/table-functions/remote.md).

<div id="limitations">
  ## القيود
</div>

1. قد تنتج الصفوف المكررة عن:

* حدوث استثناء أثناء التحليل في منتصف معالجة الملف، مع تفعيل إعادة المحاولة عبر `s3queue_loading_retries`;

* ضبط `S3Queue` على عدة خوادم تشير إلى المسار نفسه في ZooKeeper، وانتهاء جلسة Keeper قبل أن يتمكن أحد الخوادم من تثبيت الملف المُعالَج، مما قد يؤدي إلى أن يتولى خادم آخر معالجة الملف، رغم أن الخادم الأول قد يكون عالجه جزئيًا أو كليًا؛ ومع ذلك، لم يعد هذا ينطبق اعتبارًا من الإصدار 25.8 إذا كانت `use_persistent_processing_nodes = 1`.

* توقف غير طبيعي للخادم.

2. إذا كان `S3Queue` مضبوطًا على عدة خوادم تشير إلى المسار نفسه في ZooKeeper وكان وضع `Ordered` مستخدمًا، فلن تعمل `s3queue_loading_retries`. سيُصلَح هذا قريبًا.

<div id="introspection">
  ## الفحص الداخلي
</div>

لإجراء الفحص الداخلي، استخدم الجدول عديم الحالة `system.s3queue_metadata_cache` والجدول الدائم `system.s3queue_log`.

1. `system.s3queue_metadata_cache`. هذا الجدول غير دائم ويعرض الحالة المخزنة في الذاكرة لـ `S3Queue`: أي الملفات التي تجري معالجتها حاليًا، وأيّ الملفات تمت معالجتها أو أخفقت.

```sql
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_metadata_cache
(
    `database` String,
    `table` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` String,
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64)
    `exception` String
)
ENGINE = SystemS3Queue
COMMENT 'Contains in-memory state of S3Queue metadata and currently processed rows per file.' │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

مثال:

```sql

SELECT *
FROM system.s3queue_metadata_cache

Row 1:
──────
zookeeper_path:        /clickhouse/s3queue/25ea5621-ae8c-40c7-96d0-cec959c5ab88/3b3f66a1-9866-4c2e-ba78-b6bfa154207e
file_name:             wikistat/original/pageviews-20150501-030000.gz
rows_processed:        5068534
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:31
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5068534,'SelectedBytes':198132283,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':2480,'S3QueueSetFileProcessedMicroseconds':9985,'S3QueuePullMicroseconds':273776,'LogTest':17}
exception:
```

2. ‏`system.s3queue_log`. جدول دائم. يحتوي على المعلومات نفسها الموجودة في `system.s3queue_metadata_cache`، ولكن بالنسبة إلى الملفات `processed` و`failed`.

للجدول البنية التالية:

```sql
SHOW CREATE TABLE system.s3queue_log

Query id: 0ad619c3-0f2a-4ee4-8b40-c73d86e04314

┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_log
(
    `event_date` Date,
    `event_time` DateTime,
    `table_uuid` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` Enum8('Processed' = 0, 'Failed' = 1),
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64),
    `exception` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

لاستخدام `system.s3queue_log`، حدِّد إعداداته في ملف إعدادات الخادم:

```xml
    <s3queue_log>
        <database>system</database>
        <table>s3queue_log</table>
    </s3queue_log>
```

مثال:

```sql
SELECT *
FROM system.s3queue_log

Row 1:
──────
event_date:            2023-10-13
event_time:            2023-10-13 13:10:12
table_uuid:
file_name:             wikistat/original/pageviews-20150501-020000.gz
rows_processed:        5112621
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:12
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5112621,'SelectedBytes':198577687,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':1934,'S3QueueSetFileProcessedMicroseconds':17063,'S3QueuePullMicroseconds':5841972,'LogTest':17}
exception:
```