---
description: 'الجداول التي تستخدم المحرك Distributed لا تخزّن أي بيانات خاصة بها،
  لكنها تتيح معالجة الاستعلامات الموزعة عبر عدة خوادم. تتم موازاة القراءة
  تلقائيًا. وأثناء القراءة، تُستخدم فهارس الجداول على الخوادم البعيدة إن
  وُجدت.'
sidebar_label: 'Distributed'
sidebar_position: 10
slug: /engines/table-engines/special/distributed
title: 'محرك الجدول Distributed'
doc_type: 'مرجع'
---

:::warning محرك Distributed في Cloud
لإنشاء محرك جدول موزّع في ClickHouse Cloud، يمكنك استخدام دوال الجدول [`remote` و`remoteSecure`](../../../sql-reference/table-functions/remote).
لا يمكن استخدام الصيغة `Distributed(...)` في ClickHouse Cloud.
:::

الجداول التي تستخدم المحرك Distributed لا تخزّن أي بيانات خاصة بها، لكنها تتيح معالجة الاستعلامات الموزعة عبر عدة خوادم.
تتم موازاة القراءة تلقائيًا. وأثناء القراءة، تُستخدم فهارس الجداول على الخوادم البعيدة إن وُجدت.

<div id="distributed-creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]])
[SETTINGS name=value, ...]
```

<div id="distributed-from-a-table">
  ### من جدول
</div>

إذا كان جدول `Distributed` يشير إلى جدول على الخادم الحالي، فيمكنك اعتماد مخطط هذا الجدول:

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster] AS [db2.]name2 ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]]) [SETTINGS name=value, ...]
```

<div id="distributed-parameters">
  ### معلمات Distributed
</div>

| المعلمة                  | الوصف                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster`                | اسم الـ عنقود في ملف إعدادات الخادم                                                                                                                                                                                                                                                                                                                                                                                                             |
| `database`               | اسم قاعدة بيانات بعيدة                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `table`                  | اسم جدول بعيد                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `sharding_key` (اختياري) | مفتاح التقسيم. <br /> يلزم تحديد `sharding_key` في الحالات التالية: <ul><li>عند تنفيذ عمليات `INSERT` في جدول موزّع (لأن محرك الجدول يحتاج إلى `sharding_key` لتحديد كيفية توزيع البيانات). ومع ذلك، إذا كان الإعداد `insert_distributed_one_random_shard` مفعّلًا، فلن تحتاج عمليات `INSERT` إلى مفتاح التقسيم.</li><li>عند استخدام `optimize_skip_unused_shards`، لأن `sharding_key` ضروري لتحديد الشظايا التي ينبغي الاستعلام عنها</li></ul> |
| `policy_name` (اختياري)  | اسم السياسة، وسيُستخدم لتخزين الملفات المؤقتة للإرسال في الخلفية                                                                                                                                                                                                                                                                                                                                                                                |

**انظر أيضًا**

* إعداد [distributed&#95;foreground&#95;insert](../../../operations/settings/settings.md#distributed_foreground_insert)
* راجع [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) للاطلاع على الأمثلة

<div id="distributed-settings">
  ### إعدادات Distributed
</div>

| الإعداد                                    | الوصف                                                                                                                                                                                                                                                                                                                                  | القيمة الافتراضية              |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| `fsync_after_insert`                       | نفّذ `fsync` لبيانات الملف بعد `insert` في الخلفية إلى Distributed. يضمن ذلك أن نظام التشغيل قد كتب كامل البيانات المُدرجة إلى ملف **على قرص عقدة initiator**.                                                                                                                                                                         | `false`                        |
| `fsync_directories`                        | نفّذ `fsync` للأدلة. يضمن ذلك أن نظام التشغيل قد حدّث البيانات الوصفية للدليل بعد العمليات المرتبطة بعمليات `insert` في الخلفية على جدول Distributed (بعد `insert`، وبعد إرسال البيانات إلى شظية، وما إلى ذلك).                                                                                                                     | `false`                        |
| `skip_unavailable_shards`                  | إذا كانت القيمة `true`، فسيتجاوز ClickHouse بصمت الـ الشظايا غير المتاحة. ويتحكم المعلَمة `skip_unavailable_shards_mode` في سلوك هذا الإعداد.                                                                                                                                                                                         | `false`                        |
| `skip_unavailable_shards_mode`             | يتحكم في الاستثناءات الواردة من شظية بعيد التي يتم تجاهلها عند تمكين `skip_unavailable_shards`: تتجاهل القيمة `unavailable` أخطاء connection فقط؛ وتَتجاهل `unavailable_or_table_missing` أيضًا غياب جدول أو database؛ وتَتجاهل `unavailable_or_exception_before_processing` أيضًا أي استثناء يتم استلامه قبل أن يعيد شظية البيانات. | `unavailable_or_table_missing` |
| `bytes_to_throw_insert`                    | إذا تجاوز عدد البايتات المضغوطة المعلّقة لعملية `INSERT` في الخلفية هذه القيمة، فسيتم إطلاق استثناء. `0` - لا تُطلق استثناءً.                                                                                                                                                                                                          | `0`                            |
| `bytes_to_delay_insert`                    | إذا تجاوز عدد البايتات المضغوطة المعلّقة لعملية `INSERT` في الخلفية هذه القيمة، فسيتم تأخير query. `0` - لا تؤخّر.                                                                                                                                                                                                                     | `0`                            |
| `max_delay_to_insert`                      | الحد الأقصى لتأخير إدراج البيانات في جدول Distributed، بالثواني، إذا كان هناك الكثير من البايتات المعلّقة للإرسال في الخلفية.                                                                                                                                                                                                          | `60`                           |
| `background_insert_batch`                  | مثل [`distributed_background_insert_batch`](../../../operations/settings/settings.md#distributed_background_insert_batch)                                                                                                                                                                                                              | `0`                            |
| `background_insert_split_batch_on_failure` | مثل [`distributed_background_insert_split_batch_on_failure`](../../../operations/settings/settings.md#distributed_background_insert_split_batch_on_failure)                                                                                                                                                                            | `0`                            |
| `background_insert_sleep_time_ms`          | مثل [`distributed_background_insert_sleep_time_ms`](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms)                                                                                                                                                                                              | `0`                            |
| `background_insert_max_sleep_time_ms`      | مثل [`distributed_background_insert_max_sleep_time_ms`](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms)                                                                                                                                                                                      | `0`                            |
| `flush_on_detach`                          | اكتب البيانات إلى العقد البعيدة عند `DETACH`/`DROP`/إيقاف تشغيل الخادم.                                                                                                                                                                                                                                                                | `true`                         |

:::note
**إعدادات المتانة** (`fsync_...`):

* تؤثر فقط في عمليات `INSERT` في الخلفية (أي `distributed_foreground_insert=false`) عندما تُخزَّن البيانات أولًا على قرص عقدة initiator ثم تُرسَل لاحقًا، في الخلفية، إلى الـ الشظايا.
* قد تقلل أداء `INSERT` بشكل ملحوظ
* تؤثر في كتابة البيانات المخزنة داخل مجلد جدول Distributed إلى **العقدة التي قبلت عملية insert**. إذا كنت بحاجة إلى ضمانات لكتابة البيانات إلى جداول MergeTree الأساسية، فراجع إعدادات المتانة (`...fsync...`) في `system.merge_tree_settings`

بالنسبة إلى **إعدادات حدود insert** (`..._insert`)، انظر أيضًا:

* الإعداد [`distributed_foreground_insert`](../../../operations/settings/settings.md#distributed_foreground_insert)
* الإعداد [`prefer_localhost_replica`](/ar/operations/settings/settings#prefer_localhost_replica)
* تتم معالجة `bytes_to_throw_insert` قبل `bytes_to_delay_insert`، لذلك يجب ألا تضبطه على قيمة أقل من `bytes_to_delay_insert`
  :::

**مثال**

```sql
CREATE TABLE hits_all AS hits
ENGINE = Distributed(logs, default, hits[, sharding_key[, policy_name]])
SETTINGS
    fsync_after_insert=0,
    fsync_directories=0;
```

ستُقرأ البيانات من جميع الخوادم في عنقود `logs`، من الجدول `default.hits` الموجود على كل خادم في العنقود. ولا يقتصر الأمر على قراءة البيانات فحسب، بل تُعالَج أيضًا جزئيًا على الخوادم البعيدة (بالقدر الممكن). على سبيل المثال، في استعلام يتضمن `GROUP BY`، ستُجمَّع البيانات على الخوادم البعيدة، وستُرسَل الحالات الوسيطة للدوال التجميعية إلى الخادم الذي أرسل الطلب. ثم ستُجمَّع البيانات مرةً أخرى.

بدلًا من اسم قاعدة البيانات، يمكنك استخدام تعبير ثابت يُرجع سلسلة نصية. على سبيل المثال: `currentDatabase()`.

<div id="distributed-clusters">
  ## المجموعات العنقودية
</div>

تُضبط المجموعات العنقودية في [ملف إعدادات الخادم](../../../operations/configuration-files.md):

```xml
<remote_servers>
    <logs>
        <!-- Inter-server per-cluster secret for Distributed queries
             default: no secret (no authentication will be performed)

             If set, then Distributed queries will be validated on shards, so at least:
             - such cluster should exist on the shard,
             - such cluster should have the same secret.

             And also (and which is more important), the initial_user will
             be used as current user for the query.
        -->
        <!-- <secret></secret> -->
        
        <!-- Optional. Whether distributed DDL queries (ON CLUSTER clause) are allowed for this cluster. Default: true (allowed). -->        
        <!-- <allow_distributed_ddl_queries>true</allow_distributed_ddl_queries> -->
        
        <shard>
            <!-- Optional. Shard weight when writing data. Default: 1. -->
            <weight>1</weight>
            <!-- Optional. The shard name.  Must be non-empty and unique among shards in the cluster. If not specified, will be empty. -->
            <name>shard_01</name>
            <!-- Optional. Whether to write data to just one of the replicas. Default: false (write data to all replicas). -->
            <internal_replication>false</internal_replication>
            <replica>
                <!-- Optional. Priority of the replica for load balancing (see also load_balancing setting). Default: 1 (less value has more priority). -->
                <priority>1</priority>
                <host>example01-01-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-01-2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <weight>2</weight>
            <name>shard_02</name>
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-02-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-02-2</host>
                <secure>1</secure>
                <port>9440</port>
            </replica>
        </shard>
    </logs>
</remote_servers>
```

هنا يُعرَّف عنقود باسم `logs` يتكوّن من شظيتين (shards)، يحتوي كلٌّ منهما على نسختين متماثلتين. تشير الشظايا إلى الخوادم التي تحتوي على أجزاء مختلفة من البيانات (ولقراءة جميع البيانات، يجب عليك الوصول إلى جميع الشظايا). أما النسخ المتماثلة فهي خوادم مكرَّرة (ولقراءة جميع البيانات، يمكنك الوصول إلى البيانات على أي واحدة من النسخ المتماثلة).

يجب ألا تحتوي أسماء العناقيد على نقاط.

تُحدَّد المعاملات `host` و`port`، واختياريًا `user` و`password` و`secure` و`compression` و`bind_host` لكل خادم:

| المعامل       | الوصف                                                                                                                                                                                                                                               | القيمة الافتراضية |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- |
| `host`        | عنوان الخادم البعيد. يمكنك استخدام اسم النطاق أو عنوان IPv4 أو IPv6. إذا حدّدت اسم النطاق، يرسل الخادم طلب DNS عند بدء التشغيل، وتُحفَظ النتيجة ما دام الخادم قيد التشغيل. إذا فشل طلب DNS، فلن يبدأ الخادم. وإذا غيّرت سجل DNS، فأعد تشغيل الخادم. | -                 |
| `port`        | منفذ TCP المستخدم للتواصل (`tcp_port` في ملف config، ويُضبط عادةً على 9000). يجب عدم الخلط بينه وبين `http_port`.                                                                                                                                   | -                 |
| `user`        | اسم المستخدم للاتصال بخادم بعيد. يجب أن تكون لهذا المستخدم صلاحية الاتصال بالخادم المحدد. تُضبط صلاحيات الوصول في ملف `users.xml`. لمزيد من المعلومات، راجع قسم [صلاحيات الوصول](../../../guides/sre/user-management/index.md).                     | `default`         |
| `password`    | كلمة المرور للاتصال بخادم بعيد (غير مخفية).                                                                                                                                                                                                         | &#39;&#39;        |
| `secure`      | ما إذا كان سيُستخدم اتصال SSL/TLS آمن. يتطلب ذلك عادةً أيضًا تحديد المنفذ (المنفذ الآمن الافتراضي هو `9440`). يجب أن يستمع الخادم على `<tcp_port_secure>9440</tcp_port_secure>` وأن يكون مهيأً بالشهادات الصحيحة.                                   | `false`           |
| `compression` | استخدام ضغط البيانات.                                                                                                                                                                                                                               | `true`            |
| `bind_host`   | عنوان المصدر الذي سيُستخدم عند الاتصال بالخادم البعيد من هذه العقدة. لا يُدعَم سوى عنوان IPv4. وهو مخصّص لحالات استخدام النشر المتقدمة التي تتطلب تعيين عنوان IP المصدر الذي تستخدمه الاستعلامات الموزعة في ClickHouse.                             | -                 |

عند تحديد النسخ المتماثلة، سيُختار أحد النسخ المتماثلة المتاحة لكل شظية عند القراءة. يمكنك ضبط خوارزمية موازنة الحمل (تفضيل النسخة المتماثلة التي سيتم الوصول إليها) — راجع الإعداد [load&#95;balancing](../../../operations/settings/settings.md#load_balancing). إذا لم يتم إنشاء اتصال بالخادم، فستُجرى محاولة اتصال بمهلة قصيرة. وإذا فشلت محاولة الاتصال، فسيتم اختيار النسخة المتماثلة التالية، وهكذا مع جميع النسخ المتماثلة. وإذا فشلت محاولة الاتصال مع جميع النسخ المتماثلة، فستُعاد المحاولة بالطريقة نفسها عدة مرات. وهذا يعزز المرونة، لكنه لا يوفر تحمّل أعطال كاملًا: فقد يقبل خادم بعيد الاتصال، لكنه قد لا يعمل، أو قد يعمل بشكل سيئ.

يمكنك تحديد شظية واحدة فقط (وفي هذه الحالة، ينبغي أن تُسمى معالجة الاستعلامات remote بدلًا من distributed) أو أي عدد من الشظايا. وفي كل شظية، يمكنك تحديد نسخة متماثلة واحدة أو أي عدد من النسخ المتماثلة. ويمكنك تحديد عدد مختلف من النسخ المتماثلة لكل شظية.

يمكنك تحديد أي عدد تريده من العناقيد في الإعدادات.

لعرض عناقيدك، استخدم الجدول `system.clusters`.

يتيح المحرك `Distributed` العمل مع العنقود كما لو كان خادمًا محليًا. ومع ذلك، لا يمكن تحديد إعداد العنقود ديناميكيًا، بل يجب تهيئته في ملف إعدادات الخادم. وعادةً ما تكون لدى جميع الخوادم في العنقود إعدادات العنقود نفسها (مع أن هذا ليس مطلوبًا). وتُحدَّث العناقيد من ملف الإعدادات أثناء التشغيل، من دون إعادة تشغيل الخادم.

إذا كنت بحاجة إلى إرسال استعلام إلى مجموعة غير معروفة من الشظايا والنسخ المتماثلة في كل مرة، فلست بحاجة إلى إنشاء جدول `Distributed` — استخدم بدلًا من ذلك دالة الجدول `remote`. راجع قسم [دوال الجداول](../../../sql-reference/table-functions/index.md).

<div id="distributed-writing-data">
  ## كتابة البيانات
</div>

هناك طريقتان لكتابة البيانات إلى عنقود:

أولًا، يمكنك تحديد الخوادم التي ستُكتب إليها كل بيانات، ثم تنفيذ الكتابة مباشرةً على كل شظية. وبعبارة أخرى، نفِّذ عبارات `INSERT` مباشرةً على الجداول البعيدة في العنقود التي يشير إليها جدول `Distributed`. وهذا هو الحل الأكثر مرونة، إذ يمكنك استخدام أي مخطط للتشطير، حتى لو كان معقدًا بسبب متطلبات المجال المعني. كما أنه الحل الأمثل أيضًا، لأن البيانات يمكن كتابتها إلى شظايا مختلفة باستقلالية تامة.

ثانيًا، يمكنك تنفيذ عبارات `INSERT` على جدول `Distributed`. في هذه الحالة، سيتولى الجدول بنفسه توزيع البيانات المُدرجة على الخوادم. ولكي تكتب إلى جدول `Distributed`، يجب أن يكون المُعامل `sharding_key` مُعدًّا (إلا إذا كانت هناك شظية واحدة فقط).

يمكن تعريف العنصر `<weight>` لكل شظية في ملف الإعدادات. وتكون القيمة الافتراضية للوزن هي `1`. وتُوزَّع البيانات على الشظايا بكميات تتناسب مع وزن كل شظية. تُجمع جميع أوزان الشظايا أولًا، ثم يُقسَّم وزن كل شظية على المجموع الكلي لتحديد نسبة كل شظية. على سبيل المثال، إذا كانت هناك شظيتان وكان وزن الأولى 1 ووزن الثانية 2، فسيُرسل إلى الأولى ثلث الصفوف المُدرجة (1 / 3)، وإلى الثانية الثلثان (2 / 3).

يمكن أيضًا تعريف المُعامل `internal_replication` لكل شظية في ملف الإعدادات. وإذا ضُبط هذا المُعامل على `true`، فستختار عملية الكتابة أول نسخة متماثلة سليمة وتكتب البيانات إليها. استخدم هذا إذا كانت الجداول التي يعتمد عليها جدول `Distributed` جداول replicated (مثل أيٍّ من محركات الجداول `Replicated*MergeTree`). ستستقبل إحدى النسخ المتماثلة الخاصة بالجدول عملية الكتابة، ثم ستُنسخ البيانات تلقائيًا إلى النسخ المتماثلة الأخرى.

إذا ضُبط `internal_replication` على `false` (وهو الإعداد الافتراضي)، فستُكتب البيانات إلى جميع النسخ المتماثلة. في هذه الحالة، يتولى جدول `Distributed` نسخ البيانات بنفسه. وهذا أسوأ من استخدام الجداول replicated، لأن اتساق النسخ المتماثلة لا يجري التحقق منه، ومع مرور الوقت ستحتوي على بيانات مختلفة قليلًا.

لاختيار الشظية التي سيُرسل إليها صف البيانات، يجري تحليل تعبير التشطير، ثم يُؤخذ باقي قسمته على مجموع أوزان الشظايا. ويُرسل الصف إلى الشظية التي تقابل نصف الفترة للبواقي من `prev_weights` إلى `prev_weights + weight`، حيث إن `prev_weights` هو مجموع أوزان الشظايا ذات الأرقام الأصغر، و`weight` هو وزن هذه الشظية. على سبيل المثال، إذا كانت هناك شظيتان، وكان وزن الأولى 9 ووزن الثانية 10، فسيُرسل الصف إلى الشظية الأولى إذا كان الباقي ضمن المجال [0, 9)، وإلى الثانية إذا كان الباقي ضمن المجال [9, 19).

يمكن أن يكون تعبير التشطير أي تعبير مكوَّن من ثوابت وأعمدة جدول ويُرجع عددًا صحيحًا. على سبيل المثال، يمكنك استخدام التعبير `rand()` لتوزيع البيانات عشوائيًا، أو `UserID` للتوزيع بحسب باقي قسمة معرّف المستخدم (وعندها ستوجد بيانات المستخدم الواحد على شظية واحدة، مما يسهّل تنفيذ `IN` و`JOIN` حسب المستخدمين). وإذا لم يكن أحد الأعمدة موزعًا بالتساوي بما يكفي، فيمكنك تغليفه بدالة hash مثل `intHash64(UserID)`.

إن الاعتماد على باقي القسمة وحده حل محدود للتشطير، وليس مناسبًا دائمًا. فهو يعمل مع أحجام البيانات المتوسطة والكبيرة (عشرات الخوادم)، لكنه لا يناسب أحجام البيانات الضخمة جدًا (مئات الخوادم أو أكثر). وفي الحالة الأخيرة، استخدم مخطط التشطير الذي يفرضه المجال المعني بدلًا من استخدام الإدخالات في جداول `Distributed`.

ينبغي أن تهتم بمخطط التشطير في الحالات التالية:

* عند استخدام استعلامات تتطلب ربط البيانات (`IN` أو `JOIN`) باستخدام مفتاح محدد. إذا كانت البيانات مُشطّرة وفقًا لهذا المفتاح، فيمكنك استخدام `IN` أو `JOIN` المحليين بدلًا من `GLOBAL IN` أو `GLOBAL JOIN`، وهذا أكثر كفاءة بكثير.
* عند استخدام عدد كبير من الخوادم (مئات أو أكثر) مع عدد كبير من الاستعلامات الصغيرة، مثل الاستعلامات الخاصة ببيانات عملاء أفراد (مثل مواقع الويب أو المعلنين أو الشركاء). ولكي لا تؤثر الاستعلامات الصغيرة في العنقود بأكمله، فمن المنطقي وضع بيانات العميل الواحد على شظية واحدة. وبدلًا من ذلك، يمكنك إعداد تشطير ثنائي المستوى: قسّم العنقود بأكمله إلى &quot;طبقات&quot;، وقد تتكون الطبقة من عدة شظايا. توضع بيانات العميل الواحد على طبقة واحدة، لكن يمكن إضافة شظايا إلى الطبقة عند الحاجة، وتُوزَّع البيانات عشوائيًا داخلها. وتُنشأ جداول `Distributed` لكل طبقة، ويُنشأ جدول distributed مشترك واحد للاستعلامات العامة.

تُكتب البيانات في الخلفية. عند إدراجها في الجدول، لا تُكتب كتلة البيانات إلا على نظام الملفات المحلي. وتُرسَل البيانات إلى الخوادم البعيدة في الخلفية بأسرع ما يمكن. ويتحكم الإعدادان [distributed&#95;background&#95;insert&#95;sleep&#95;time&#95;ms](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms) و[distributed&#95;background&#95;insert&#95;max&#95;sleep&#95;time&#95;ms](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms) في دورية إرسال البيانات. ويرسل محرك `Distributed` كل ملف يحتوي على بيانات مُدرجة بشكل منفصل، ولكن يمكنك تمكين الإرسال الدفعي للملفات باستخدام الإعداد [distributed&#95;background&#95;insert&#95;batch](../../../operations/settings/settings.md#distributed_background_insert_batch). ويحسّن هذا الإعداد أداء العنقود من خلال الاستفادة على نحو أفضل من موارد الخادم المحلي والشبكة. وينبغي التحقق من إرسال البيانات بنجاح عبر فحص قائمة الملفات (البيانات التي تنتظر الإرسال) في دليل الجدول: `/var/lib/clickhouse/data/database/table/`. ويمكن تعيين عدد سلاسل التنفيذ التي تنفذ المهام في الخلفية باستخدام الإعداد [background&#95;distributed&#95;schedule&#95;pool&#95;size](/ar/operations/server-configuration-parameters/settings#background_distributed_schedule_pool_size).

إذا لم يعد الخادم موجودًا أو تعرّض لإعادة تشغيل غير سليمة (على سبيل المثال، بسبب عطل في العتاد) بعد تنفيذ `INSERT` إلى جدول `Distributed`، فقد تُفقَد البيانات المُدرجة. وإذا اكتُشف جزء بيانات تالف في دليل الجدول، فسيُنقَل إلى الدليل الفرعي `broken` ولن يُستخدم بعد ذلك.

<div id="distributed-reading-data">
  ## قراءة البيانات
</div>

عند الاستعلام عن جدول `Distributed`، تُرسَل استعلامات `SELECT` إلى جميع الشظايا، وتعمل بغض النظر عن كيفية توزيع البيانات بينها (إذ يمكن أن تكون موزعة عشوائيًا بالكامل). وعند إضافة شظية جديدة، لا تحتاج إلى نقل البيانات القديمة إليها. وبدلًا من ذلك، يمكنك كتابة بيانات جديدة فيها باستخدام وزن أعلى — وستتوزع البيانات بشكل غير متساوٍ قليلًا، لكن الاستعلامات ستعمل بكفاءة وبصورة صحيحة.

عند تمكين الخيار `max_parallel_replicas`، تُعالَج الاستعلامات بالتوازي عبر جميع النسخ المتماثلة داخل شظية واحدة. لمزيد من المعلومات، راجع قسم [max&#95;parallel&#95;replicas](../../../operations/settings/settings.md#max_parallel_replicas).

لمعرفة المزيد حول كيفية معالجة استعلامات `in` و`global in` الموزعة، راجع [هذه الوثائق](/ar/sql-reference/operators/in#distributed-subqueries).

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

<div id="_shard_num">
  #### _Shard_num
</div>

`_shard_num` — يحتوي على قيمة `shard_num` من الجدول `system.clusters`. النوع: [UInt32](../../../sql-reference/data-types/int-uint.md).

:::note
نظرًا لأن دالتي الجدول [`remote`](../../../sql-reference/table-functions/remote.md) و[`cluster](../../../sql-reference/table-functions/cluster.md) تنشئان داخليًا جدولًا مؤقتًا من نوع Distributed، فإن `&#95;shard&#95;num&#96; متاح فيهما أيضًا.
:::

**انظر أيضًا**

* وصف [الأعمدة الافتراضية](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* الإعداد [`background_distributed_schedule_pool_size`](/ar/operations/server-configuration-parameters/settings#background_distributed_schedule_pool_size)
* الدالتان [`shardNum()`](../../../sql-reference/functions/other-functions.md#shardNum) و[`shardCount()`](../../../sql-reference/functions/other-functions.md#shardCount)