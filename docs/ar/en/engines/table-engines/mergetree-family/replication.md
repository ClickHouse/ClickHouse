---
description: 'نظرة عامة على تكرار البيانات باستخدام عائلة محركات الجداول Replicated* في ClickHouse'
sidebar_label: 'Replicated*'
sidebar_position: 20
slug: /engines/table-engines/mergetree-family/replication
title: 'محركات الجداول Replicated*'
doc_type: 'مرجع'
---

:::note
في ClickHouse Cloud، تتم إدارة التكرار نيابةً عنك. يُرجى إنشاء جداولك من دون إضافة وسائط. على سبيل المثال، في النص أدناه ستستبدل:

```sql
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/table_name',
    '{replica}'
)
```

مع:

```sql
ENGINE = ReplicatedMergeTree
```

:::

لا يدعم النسخ المتماثل إلا الجداول ضمن عائلة MergeTree

* ReplicatedSummingMergeTree
* ReplicatedCoalescingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedGraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree

يعمل النسخ المتماثل على مستوى كل جدول على حدة، وليس على مستوى الخادم بأكمله. ويمكن للخادم أن يخزّن جداول متماثلة وأخرى غير متماثلة في الوقت نفسه.

لا يعتمد النسخ المتماثل على sharding. فلكل shard نسخ متماثل مستقل خاص به.

تُنسخ البيانات المضغوطة الخاصة باستعلامات `INSERT` و`ALTER` متماثلًا (لمزيد من المعلومات، راجع وثائق [ALTER](/ar/sql-reference/statements/alter).

تُنفَّذ استعلامات `CREATE` و`DROP` و`ATTACH` و`DETACH` و`RENAME` على خادم واحد، ولا تُنسخ متماثلًا:

* ينشئ استعلام `CREATE TABLE` جدولًا جديدًا قابلًا للنسخ المتماثل على الخادم الذي يُنفَّذ عليه الاستعلام. وإذا كان هذا الجدول موجودًا بالفعل على خوادم أخرى، فإنه يضيف نسخة متماثلة جديدة.
* يحذف استعلام `DROP TABLE` النسخة المتماثلة الموجودة على الخادم الذي يُنفَّذ عليه الاستعلام.
* يعيد استعلام `RENAME` تسمية الجدول على إحدى النسخ المتماثلة. وبعبارة أخرى، يمكن أن تحمل الجداول المتماثلة أسماء مختلفة على نسخ متماثلة مختلفة.

يستخدم ClickHouse ‏[ClickHouse Keeper](/ar/guides/sre/keeper/index.md) لتخزين البيانات الوصفية الخاصة بالنسخ المتماثلة. ويمكن استخدام ZooKeeper بالإصدار 3.4.5 أو أحدث، ولكن يُوصى باستخدام ClickHouse Keeper.

لاستخدام النسخ المتماثل، اضبط المعلمات في قسم [zookeeper](/ar/operations/server-configuration-parameters/settings#zookeeper) ضمن إعدادات الخادم.

:::note
لا تُغفل إعدادات الأمان. يدعم ClickHouse مخطط `digest` لـ [ACL](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) ضمن النظام الأمني في ZooKeeper.
:::

مثال على تعيين عناوين عنقود ClickHouse Keeper:

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <node>
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

يدعم ClickHouse أيضًا تخزين المعلومات الوصفية للنسخ المتماثلة في عنقود ZooKeeper إضافي. ويتم ذلك عبر تمرير اسم مجموعة ZooKeeper والمسار كوسيطات للمحرك.
وبعبارة أخرى، فهو يدعم تخزين البيانات الوصفية للجداول المختلفة في مجموعات ZooKeeper مختلفة.

مثال على تعيين عناوين مجموعة ZooKeeper الإضافية:

```xml
<auxiliary_zookeepers>
    <zookeeper2>
        <node>
            <host>example_2_1</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_2</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_3</host>
            <port>2181</port>
        </node>
    </zookeeper2>
    <zookeeper3>
        <node>
            <host>example_3_1</host>
            <port>2181</port>
        </node>
    </zookeeper3>
</auxiliary_zookeepers>
```

لتخزين البيانات الوصفية للجدول في عنقود ZooKeeper إضافي بدلًا من عنقود ZooKeeper الافتراضي، يمكننا استخدام SQL لإنشاء الجدول باستخدام
محرك ReplicatedMergeTree كما يلي:

```sql
CREATE TABLE table_name ( ... ) ENGINE = ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'replica_name') ...
```

يمكنك تحديد أي عنقود ZooKeeper موجود، وسيستخدم النظام دليلاً عليه لبياناته الخاصة (يُحدَّد هذا الدليل عند إنشاء جدول قابل للنسخ المتماثل).

إذا لم يتم تعيين ZooKeeper في ملف الإعدادات، فلن تتمكن من إنشاء جداول متماثلة، وستصبح أي جداول متماثلة موجودة للقراءة فقط.

لا يُستخدم ZooKeeper في استعلامات `SELECT` لأن النسخ المتماثل لا تؤثر في أداء `SELECT`، وتُنفَّذ الاستعلامات بالسرعة نفسها كما في الجداول غير المتماثلة النسخ. عند الاستعلام عن الجداول الموزعة المتماثلة النسخ، يتحكم في سلوك ClickHouse الإعدادان [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](/ar/operations/settings/settings.md/#max_replica_delay_for_distributed_queries) و[fallback&#95;to&#95;stale&#95;replicas&#95;for&#95;distributed&#95;queries](/ar/operations/settings/settings.md/#fallback_to_stale_replicas_for_distributed_queries).

لكل استعلام `INSERT`، تُضاف نحو عشرة إدخالات إلى ZooKeeper عبر عدة معاملات. (وبشكل أدق، يحدث هذا لكل block بيانات مُدرَج؛ إذ يحتوي استعلام `INSERT` على block واحد، أو block واحد لكل `max_insert_block_size = 1048576` صفوف.) يؤدي هذا إلى زمن استجابة أطول قليلًا لاستعلامات `INSERT` مقارنةً بالجداول غير المتماثلة النسخ. ولكن إذا اتبعت التوصيات الخاصة بإدراج البيانات على دفعات، بحيث لا يزيد المعدل على استعلام `INSERT` واحد في الثانية، فلن يسبب ذلك أي مشكلات. ويمكن لعنقود ClickHouse الكامل المستخدم لتنسيق عنقود ZooKeeper واحد أن يتعامل إجمالًا مع عدة مئات من عمليات `INSERT` في الثانية. وتبقى الإنتاجية في إدراج البيانات (عدد الصفوف في الثانية) بنفس ارتفاعها في البيانات غير المتماثلة النسخ.

بالنسبة إلى العناقيد الكبيرة جدًا، يمكنك استخدام عناقيد ZooKeeper مختلفة لشظايا مختلفة. ومع ذلك، وبحسب خبرتنا، لم تثبت ضرورة ذلك في عناقيد production تضم نحو 300 خادم.

النسخ المتماثل غير متزامنة ومتعددة المصدر الرئيسي. يمكن إرسال استعلامات `INSERT` (وكذلك `ALTER`) إلى أي خادم متاح. تُدرَج البيانات على الخادم الذي يُنفَّذ عليه الاستعلام، ثم تُنسَخ إلى الخوادم الأخرى. وبما أنها غير متزامنة، تظهر البيانات المُدرَجة حديثًا على النسخ المتماثلة الأخرى بعد بعض زمن الاستجابة. وإذا لم تكن بعض النسخ المتماثلة متاحة، فستُكتَب البيانات عند عودتها للتوفر. وإذا كانت نسخة متماثلة متاحة، فإن زمن الاستجابة يساوي الوقت اللازم لنقل block البيانات المضغوط عبر الشبكة. ويمكن ضبط عدد threads التي تنفذ المهام الخلفية للجداول المتماثلة النسخ بواسطة الإعداد [background&#95;schedule&#95;pool&#95;size](/ar/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size).

يستخدم المحرك `ReplicatedMergeTree` thread pool منفصلًا لعمليات replicated fetches. ويُحدَّد حجم هذا pool بواسطة الإعداد [background&#95;fetches&#95;pool&#95;size](/ar/operations/server-configuration-parameters/settings#background_fetches_pool_size)، والذي يمكن ضبطه بعد إعادة تشغيل الخادم.

افتراضيًا، ينتظر استعلام `INSERT` تأكيد كتابة البيانات من نسخة متماثلة واحدة فقط. وإذا كُتبت البيانات بنجاح على نسخة متماثلة واحدة فقط ثم توقف الخادم الذي توجد عليه هذه النسخة المتماثلة عن الوجود، فستُفقَد البيانات المخزنة. لتمكين تلقي تأكيدات كتابة البيانات من عدة نسخ متماثلة، استخدم الخيار `insert_quorum`.

يُكتَب كل block بيانات بصورة atomic. ويُقسَّم استعلام `INSERT` إلى blocks تصل حتى `max_insert_block_size = 1048576` صفوف. وبعبارة أخرى، إذا كان استعلام `INSERT` يحتوي على أقل من 1048576 صفًا، فسيُنفَّذ بصورة atomic.

تُزال ازدواجية Data blocks. فعند كتابة block البيانات نفسه عدة مرات (blocks بيانات بالحجم نفسه وتحتوي على الصفوف نفسها وبالترتيب نفسه)، لا يُكتَب هذا block إلا مرة واحدة. والسبب في ذلك هو التعامل مع حالات فشل الشبكة عندما لا يعرف تطبيق العميل ما إذا كانت البيانات قد كُتبت إلى قاعدة البيانات، لذا يمكن ببساطة تكرار استعلام `INSERT`. ولا يهم إلى أي نسخة متماثلة أُرسلت عمليات `INSERT` التي تحتوي على بيانات متطابقة. إن عمليات `INSERT` متسمة بخاصية idempotent. وتتحكم معلمات deduplication في [merge&#95;tree](/ar/operations/server-configuration-parameters/settings.md/#merge_tree) server settings.

أثناء النسخ المتماثل، لا يُنقَل عبر الشبكة إلا بيانات المصدر المطلوب إدراجها. أما تحويل البيانات لاحقًا (merging) فيُنسَّق ويُنفَّذ على جميع النسخ المتماثلة بالطريقة نفسها. وهذا يقلل استخدام الشبكة إلى الحد الأدنى، ما يعني أن النسخ المتماثل تعمل جيدًا عندما تكون النسخ المتماثلة موجودة في datacenters مختلفة. (لاحظ أن تكرار البيانات في datacenters مختلفة هو الهدف الرئيسي من النسخ المتماثل.)

يمكنك امتلاك أي عدد من النسخ المتماثلة للبيانات نفسها. واستنادًا إلى خبرتنا، يمكن أن يستخدم حل موثوق ومناسب نسبيًا نسخًا متماثلًا مزدوجًا في بيئات production، مع استخدام كل خادم RAID-5 أو RAID-6 (وRAID-10 في بعض الحالات).

يراقب النظام تزامن البيانات على النسخ المتماثلة، وهو قادر على التعافي بعد الفشل. ويكون failover تلقائيًا (عند وجود اختلافات صغيرة في البيانات) أو شبه تلقائي (عندما تختلف البيانات بدرجة كبيرة، ما قد يشير إلى خطأ في configuration).

<div id="creating-replicated-tables">
  ## إنشاء الجداول ذات النسخ المتماثل
</div>

:::note
في ClickHouse Cloud، تتم إدارة النسخ المتماثل تلقائيًا.

أنشئ الجداول باستخدام [`MergeTree`](/ar/engines/table-engines/mergetree-family/mergetree) من دون وسيطات النسخ المتماثل. ويعيد النظام داخليًا كتابة [`MergeTree`](/ar/engines/table-engines/mergetree-family/mergetree) إلى [`SharedMergeTree`](/ar/cloud/reference/shared-merge-tree) لأغراض النسخ المتماثل وتوزيع البيانات.

تجنّب استخدام `ReplicatedMergeTree` أو تحديد معلمات النسخ المتماثل، إذ تتولى المنصة إدارة النسخ المتماثل.

:::

<div id="replicatedmergetree-parameters">
  ### معلمات Replicated*MergeTree
</div>

| المعلمة            | الوصف                                                                                |
| ------------------ | ------------------------------------------------------------------------------------ |
| `zoo_path`         | مسار الجدول في ClickHouse Keeper.                                                    |
| `replica_name`     | اسم النسخة المتماثلة في ClickHouse Keeper.                                           |
| `other_parameters` | معلمات المحرك المستخدم لإنشاء النسخة المتماثلة، مثل الإصدار في `ReplacingMergeTree`. |

مثال:

```sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
```

<details markdown="1">
  <summary>مثال بصياغة متقادمة</summary>

  ```sql
  CREATE TABLE table_name
  (
      EventDate DateTime,
      CounterID UInt32,
      UserID UInt32
  ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192);
  ```
</details>

كما يوضّح المثال، يمكن أن تحتوي هذه المعلَمات على استبدالات داخل `{}`. وتُؤخذ القيم المستبدلة من قسم [ماكرو](/ar/operations/server-configuration-parameters/settings.md/#macros) في ملف الإعدادات.

مثال:

```xml
<macros>
    <shard>02</shard>
    <replica>example05-02-1</replica>
</macros>
```

يجب أن يكون المسار إلى الجدول في ClickHouse Keeper فريدًا لكل جدول `Replicated`. ويجب أن تكون للجداول الموجودة على shardات مختلفة مسارات مختلفة.
في هذه الحالة، يتكوّن المسار من الأجزاء التالية:

`/clickhouse/tables/` هي البادئة المشتركة. نوصي باستخدام هذا المسار تحديدًا.

سيُوسَّع `{shard}` إلى معرّف الـ shard.

`table_name` هو اسم العقدة الخاصة بالجدول في ClickHouse Keeper. ومن الأفضل أن يكون مطابقًا لاسم الجدول. ويُعرَّف هذا الاسم صراحةً لأنه، بخلاف اسم الجدول، لا يتغير بعد تنفيذ استعلام RENAME.
*تلميح*: يمكنك أيضًا إضافة اسم قاعدة البيانات قبل `table_name`. على سبيل المثال: `db_name.table_name`

يمكن استخدام عمليتي الاستبدال المضمَّنتين `{database}` و`{table}`، إذ تتوسّعان إلى اسم الجدول واسم قاعدة البيانات على التوالي (ما لم تكن وحدات الـ macro هذه معرّفة في قسم `macros`). لذلك يمكن تحديد مسار ZooKeeper بالشكل `'/clickhouse/tables/{shard}/{database}/{table}'`.
كن حذرًا عند إعادة تسمية الجداول عند استخدام عمليتي الاستبدال المضمَّنتين هاتين. لا يمكن تغيير المسار في ClickHouse Keeper، وعند إعادة تسمية الجدول، ستتوسّع وحدات الـ macro إلى مسار مختلف، وسيشير الجدول إلى مسار غير موجود في ClickHouse Keeper، وسينتقل إلى وضع القراءة فقط.

يحدِّد اسم replica النسخ المتماثلة المختلفة للجدول نفسه. يمكنك استخدام اسم الخادم لهذا الغرض، كما في المثال. ولا يلزم أن يكون الاسم فريدًا إلا داخل كل shard.

يمكنك تعريف المَعلمات صراحةً بدلًا من استخدام عمليات الاستبدال. قد يكون هذا مناسبًا للاختبار وتهيئة عنقودات الصغيرة. ولكن لا يمكنك استخدام distributed DDL queries (`ON CLUSTER`) في هذه الحالة.

عند العمل مع عنقودات كبيرة، نوصي باستخدام عمليات الاستبدال لأنها تقلل احتمال حدوث أخطاء.

يمكنك تحديد الوسيطات الافتراضية لمحرك الجدول `Replicated` في ملف إعدادات الخادم. على سبيل المثال:

```xml
<default_replica_path>/clickhouse/tables/{shard}/{database}/{table}</default_replica_path>
<default_replica_name>{replica}</default_replica_name>
```

في هذه الحالة، يمكنك الاستغناء عن الوسائط عند إنشاء الجداول:

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree
ORDER BY x;
```

وهو يعادل:

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
ORDER BY x;
```

شغّل استعلام `CREATE TABLE` على كل نسخة متماثلة. ينشئ هذا الاستعلام جدولًا متماثلًا جديدًا، أو يضيف نسخة متماثلة جديدة إلى جدول موجود.

إذا أضفت نسخة متماثلة جديدة بعد أن كان الجدول يحتوي بالفعل على بعض البيانات في نُسخ متماثلة أخرى، فستُنسخ البيانات من النُسخ المتماثلة الأخرى إلى النسخة الجديدة بعد تشغيل الاستعلام. وبعبارة أخرى، تُزامن النسخة المتماثلة الجديدة نفسها مع بقية النُسخ.

لحذف نسخة متماثلة، شغّل `DROP TABLE`. ومع ذلك، لا تُحذف إلا نسخة متماثلة واحدة، وهي الموجودة على الخادم الذي تُشغّل عليه الاستعلام.

<div id="recovery-after-failures">
  ## الاستعادة بعد حالات الفشل
</div>

إذا لم يكن ClickHouse Keeper متاحًا عند بدء تشغيل الخادم، فإن الجداول المكررة تتحول إلى وضع القراءة فقط. ويحاول النظام دوريًا الاتصال بـ ClickHouse Keeper.

إذا لم يكن ClickHouse Keeper متاحًا أثناء تنفيذ `INSERT`، أو حدث خطأ عند التفاعل مع ClickHouse Keeper، فسيتم إطلاق استثناء.

بعد الاتصال بـ ClickHouse Keeper، يتحقق النظام مما إذا كانت مجموعة البيانات في نظام الملفات المحلي تطابق مجموعة البيانات المتوقعة (ويخزن ClickHouse Keeper هذه المعلومات). وإذا كانت هناك حالات عدم اتساق طفيفة، فإن النظام يعالجها بمزامنة البيانات مع النسخ المتماثلة.

إذا اكتشف النظام أجزاء بيانات تالفة (بأحجام ملفات غير صحيحة) أو أجزاء غير معروفة (أجزاء كُتبت في نظام الملفات ولكن لم تُسجَّل في ClickHouse Keeper)، فإنه ينقلها إلى الدليل الفرعي `detached` (ولا يحذفها). وتُنسخ أي أجزاء مفقودة من النسخ المتماثلة.

لاحظ أن ClickHouse لا ينفذ أي إجراءات مدمرة، مثل حذف كميات كبيرة من البيانات تلقائيًا.

عند بدء تشغيل الخادم (أو عند إنشاء جلسة جديدة مع ClickHouse Keeper)، فإنه يتحقق فقط من عدد جميع الملفات وأحجامها. وإذا تطابقت أحجام الملفات ولكن تغيرت البايتات في موضع ما في المنتصف، فلن يُكتشف ذلك فورًا، بل فقط عند محاولة قراءة البيانات لاستعلام `SELECT`. ويؤدي الاستعلام إلى إطلاق استثناء بشأن عدم تطابق checksum أو حجم كتلة مضغوطة. وفي هذه الحالة، تُضاف أجزاء البيانات إلى قائمة انتظار التحقق وتُنسخ من النسخ المتماثلة إذا لزم الأمر.

إذا اختلفت مجموعة البيانات المحلية كثيرًا عن المجموعة المتوقعة، فسيتم تفعيل آلية أمان. ويسجل الخادم ذلك في السجل ويرفض التشغيل. ويرجع السبب في ذلك إلى أن هذه الحالة قد تشير إلى خطأ في التهيئة، مثل أن تكون نسخة متماثلة على shard قد ضُبطت بالخطأ كما لو كانت نسخة متماثلة على shard مختلف. ومع ذلك، فإن الحدود الخاصة بهذه الآلية مضبوطة على قيم منخفضة نسبيًا، وقد يحدث هذا الوضع أثناء الاستعادة العادية بعد الفشل. وفي هذه الحالة، تُستعاد البيانات بشكل شبه تلقائي — عبر &quot;الضغط على زر&quot;.

لبدء الاستعادة، أنشئ العقدة `/path_to_table/replica_name/flags/force_restore_data` في ClickHouse Keeper بأي محتوى، أو شغّل الأمر لاستعادة جميع الجداول المكررة:

```bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

ثم أعد تشغيل الخادم. وعند بدء التشغيل، يحذف الخادم هذه العلامات ويبدأ عملية الاستعادة.

<div id="recovery-after-complete-data-loss">
  ## الاسترداد بعد الفقدان الكامل للبيانات
</div>

إذا اختفت جميع البيانات والبيانات الوصفية من أحد الخوادم، فاتبع الخطوات التالية للاسترداد:

1. ثبّت ClickHouse على الخادم. حدِّد الاستبدالات بشكل صحيح في ملف الإعدادات الذي يحتوي على معرّف shard والنسخ المتماثلة، إذا كنت تستخدمها.
2. إذا كانت لديك جداول غير متماثلة يجب نسخها يدويًا إلى الخوادم، فانسخ بياناتها من نسخة متماثلة (من الدليل `/var/lib/clickhouse/data/db_name/table_name/`).
3. انسخ تعريفات الجداول الموجودة في `/var/lib/clickhouse/metadata/` من نسخة متماثلة. إذا كان معرّف shard أو معرّف النسخة المتماثلة محددًا صراحةً في تعريفات الجداول، فصحّحه بحيث يتوافق مع هذه النسخة المتماثلة. (بدلًا من ذلك، شغّل الخادم ونفّذ جميع استعلامات `ATTACH TABLE` التي كان ينبغي أن تكون موجودة في ملفات ‎`.sql` ضمن `/var/lib/clickhouse/metadata/`.)
4. لبدء الاسترداد، أنشئ عقدة ClickHouse Keeper ‏`/path_to_table/replica_name/flags/force_restore_data` بأي محتوى، أو شغّل الأمر التالي لاسترداد جميع الجداول المتماثلة: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

ثم شغّل الخادم (أعِد تشغيله إذا كان قيد التشغيل بالفعل). سيتم تنزيل البيانات من النسخ المتماثلة.

ومن خيارات الاسترداد البديلة حذف معلومات النسخة المتماثلة المفقودة من ClickHouse Keeper ‏(`/path_to_table/replica_name`)، ثم إنشاء النسخة المتماثلة مرة أخرى كما هو موضح في &quot;[إنشاء الجداول المتماثلة](#creating-replicated-tables)&quot;.

لا تُفرض أي قيود على عرض النطاق الترددي للشبكة أثناء الاسترداد. ضع ذلك في الحسبان إذا كنت تسترد عددًا كبيرًا من النسخ المتماثلة في الوقت نفسه.

<div id="converting-from-mergetree-to-replicatedmergetree">
  ## التحويل من MergeTree إلى ReplicatedMergeTree
</div>

نستخدم المصطلح `MergeTree` للإشارة إلى جميع محركات الجداول ضمن `MergeTree family`، وكذلك الحال مع `ReplicatedMergeTree`.

إذا كان لديك جدول `MergeTree` أُعدّ له النسخ المتماثل يدويًا، فيمكنك تحويله إلى جدول متماثل. قد تحتاج إلى ذلك إذا كنت قد جمعت بالفعل كمية كبيرة من البيانات في جدول `MergeTree` وتريد الآن تمكين النسخ المتماثل.

تتيح عبارة [ATTACH TABLE ... AS REPLICATED](/ar/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) إرفاق جدول `MergeTree` في حالة `detached` كجدول `ReplicatedMergeTree`.

يمكن تحويل جدول `MergeTree` تلقائيًا عند إعادة تشغيل الخادم إذا تم تعيين العلامة `convert_to_replicated` في دليل بيانات الجدول (`/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/` لقاعدة البيانات `Atomic`).
أنشئ ملفًا فارغًا باسم `convert_to_replicated`، وسيُحمَّل الجدول كجدول متماثل عند إعادة تشغيل الخادم في المرة التالية.

يمكن استخدام هذا الاستعلام للحصول على مسار بيانات الجدول. وإذا كان للجدول عدة مسارات بيانات، فيجب استخدام أول مسار منها.

```sql
SELECT data_paths FROM system.tables WHERE table = 'table_name' AND database = 'database_name';
```

لاحظ أن جدول ReplicatedMergeTree سيُنشأ باستخدام قيم الإعدادين `default_replica_path` و`default_replica_name`.
ولإنشاء جدول مُحوَّل على النُسخ المتماثلة الأخرى، ستحتاج إلى تحديد مساره صراحةً في الوسيط الأول لمحرك `ReplicatedMergeTree`. ويمكن استخدام الاستعلام التالي للحصول على هذا المسار.

```sql
SELECT zookeeper_path FROM system.replicas WHERE table = 'table_name';
```

توجد أيضًا طريقة يدوية لتنفيذ ذلك.

إذا كانت البيانات تختلف بين النسخ المتماثلة المختلفة، فقم أولًا بمزامنتها، أو احذف هذه البيانات من جميع النسخ المتماثلة باستثناء واحدة.

أعِد تسمية جدول MergeTree الحالي، ثم أنشئ جدول `ReplicatedMergeTree` بالاسم القديم.
انقل البيانات من الجدول القديم إلى الدليل الفرعي `detached` داخل الدليل الذي يحتوي على بيانات الجدول الجديد (`/var/lib/clickhouse/data/db_name/table_name/`).
ثم نفّذ `ALTER TABLE ATTACH PARTITION` على إحدى النسخ المتماثلة لإضافة أجزاء البيانات هذه إلى المجموعة النشطة.

<div id="converting-from-replicatedmergetree-to-mergetree">
  ## التحويل من ReplicatedMergeTree إلى MergeTree
</div>

استخدم عبارة [ATTACH TABLE ... AS NOT REPLICATED](/ar/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) لإرفاق جدول `ReplicatedMergeTree` منفصل كجدول `MergeTree` على خادم واحد.

تتطلب طريقة أخرى لتنفيذ ذلك إعادة تشغيل الخادم. أنشئ جدول MergeTree باسم مختلف. انقل جميع البيانات من الدليل الذي يحتوي على بيانات جدول `ReplicatedMergeTree` إلى دليل بيانات الجدول الجديد. ثم احذف جدول `ReplicatedMergeTree` وأعد تشغيل الخادم.

إذا كنت تريد التخلص من جدول `ReplicatedMergeTree` من دون تشغيل الخادم:

* احذف ملف `.sql` المقابل في دليل البيانات الوصفية (`/var/lib/clickhouse/metadata/`).
* احذف المسار المقابل في ClickHouse Keeper (`/path_to_table/replica_name`).

بعد ذلك، يمكنك تشغيل الخادم، وإنشاء جدول `MergeTree`، ونقل البيانات إلى دليله، ثم إعادة تشغيل الخادم.

<div id="recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged">
  ## التعافي عند فقدان البيانات الوصفية في عنقود ClickHouse Keeper أو تلفها
</div>

إذا فُقدت البيانات في ClickHouse Keeper أو تعرضت للتلف، فيمكنك الحفاظ عليها بنقلها إلى جدول غير مكرّر كما هو موضح أعلاه.

**انظر أيضًا**

* [background&#95;schedule&#95;pool&#95;size](/ar/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size)
* [background&#95;fetches&#95;pool&#95;size](/ar/operations/server-configuration-parameters/settings.md/#background_fetches_pool_size)
* [execute&#95;merges&#95;on&#95;single&#95;replica&#95;time&#95;threshold](/ar/operations/settings/merge-tree-settings#execute_merges_on_single_replica_time_threshold)
* [max&#95;replicated&#95;fetches&#95;network&#95;bandwidth](/ar/operations/settings/merge-tree-settings.md/#max_replicated_fetches_network_bandwidth)
* [max&#95;replicated&#95;sends&#95;network&#95;bandwidth](/ar/operations/settings/merge-tree-settings.md/#max_replicated_sends_network_bandwidth)