---
description: 'يعتمد هذا المحرّك على محرّك Atomic. ويدعم النسخ المتماثل للبيانات الوصفية عبر
  سجل DDL الذي يُكتب إلى ZooKeeper ويُنفَّذ على جميع النُسخ المتماثلة لقاعدة
  بيانات معيّنة.'
sidebar_label: 'Replicated'
sidebar_position: 30
slug: /engines/database-engines/replicated
title: 'Replicated'
doc_type: 'reference'
---

يعتمد هذا المحرّك على محرّك [Atomic](../../engines/database-engines/atomic.md). ويدعم النسخ المتماثل للبيانات الوصفية عبر سجل DDL الذي يُكتب إلى ZooKeeper ويُنفَّذ على جميع النُسخ المتماثلة لقاعدة بيانات معيّنة.

يمكن لخادم ClickHouse واحد تشغيل عدة قواعد بيانات Replicated وتحديثها في الوقت نفسه. ولكن لا يمكن أن توجد عدة نُسخ متماثلة من قاعدة البيانات Replicated نفسها.

<div id="creating-a-database">
  ## إنشاء قاعدة بيانات
</div>

```sql
CREATE DATABASE testdb [UUID '...'] ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**معلمات المحرك**

* `zoo_path` — مسار ZooKeeper. يشير مسار ZooKeeper نفسه إلى قاعدة البيانات نفسها.
* `shard_name` — اسم الشارد. تُجمَّع نُسخ قاعدة البيانات المتماثلة في شاردات حسب `shard_name`.
* `replica_name` — اسم النسخة المتماثلة. يجب أن تختلف أسماء النسخ المتماثلة بين جميع النسخ المتماثلة ضمن الشارد نفسه.

يمكن إغفال المعلمات، وفي هذه الحالة تُستبدل المعلمات غير الموجودة بالقيم الافتراضية.

إذا كان `zoo_path` يحتوي على الماكرو `{uuid}`، فيلزم تحديد معرّف UUID صراحةً أو إضافة [ON CLUSTER](../../sql-reference/distributed-ddl.md) إلى عبارة CREATE لضمان أن تستخدم جميع النسخ المتماثلة معرّف UUID نفسه لقاعدة البيانات هذه.

بالنسبة إلى جداول [ReplicatedMergeTree](/ar/engines/table-engines/mergetree-family/replication)، إذا لم تُوفَّر أي وسيطات، فستُستخدم الوسيطات الافتراضية: `/clickhouse/tables/{uuid}/{shard}` و `{replica}`. ويمكن تغييرها في إعدادات الخادم [default&#95;replica&#95;path](../../operations/server-configuration-parameters/settings.md#default_replica_path) و [default&#95;replica&#95;name](../../operations/server-configuration-parameters/settings.md#default_replica_name). يُستبدل الماكرو `{uuid}` بمُعرّف UUID الخاص بالجدول، ويُستبدل `{shard}` و `{replica}` بالقيم الواردة من تهيئة الخادم، وليس من وسيطات محرك قاعدة البيانات. ولكن في المستقبل، سيكون من الممكن استخدام `shard_name` و `replica_name` لقاعدة بيانات Replicated.

يُدعَم أيضًا استخدام عنقود ZooKeeper إضافي لتخزين البيانات الوصفية لقاعدة بيانات متماثلة بدلًا من استخدام عنقود ZooKeeper الافتراضي. ويمكن استخدام SQL لإنشاء قاعدة البيانات المتماثلة مع عنقود ZooKeeper إضافي كما يلي:

```sql
CREATE DATABASE database_name ENGINE = Replicated('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'shard_name', 'replica_name')
```

<div id="specifics-and-recommendations">
  ## التفاصيل والتوصيات
</div>

تعمل استعلامات DDL مع قاعدة البيانات `Replicated` بطريقة مشابهة لاستعلامات [ON CLUSTER](../../sql-reference/distributed-ddl.md)، ولكن مع بعض الفروق الطفيفة.

أولًا، يحاول طلب DDL التنفيذ على المضيف البادئ (المضيف الذي تلقّى الطلب أصلًا من المستخدم). وإذا لم يُنفَّذ الطلب، يتلقى المستخدم فورًا خطأ، ولا تحاول المضيفات الأخرى تنفيذه. أما إذا اكتمل الطلب بنجاح على المضيف البادئ، فستعيد جميع المضيفات الأخرى المحاولة تلقائيًا إلى أن تكتمله. وسيحاول المضيف البادئ انتظار اكتمال الاستعلام على المضيفات الأخرى (لمدة لا تتجاوز [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout))، ثم يُرجع جدولًا بحالات تنفيذ الاستعلام على كل مضيف.

يُنظَّم السلوك في حالة الأخطاء بواسطة الإعداد [distributed&#95;ddl&#95;output&#95;mode](../../operations/settings/settings.md#distributed_ddl_output_mode)، وبالنسبة إلى قاعدة بيانات `Replicated` فمن الأفضل ضبطه على `null_status_on_timeout` — أي إذا لم تتمكن بعض المضيفات من تنفيذ الطلب خلال [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)، فلا يتم throw an exception، بل تُعرَض لها الحالة `NULL` في الجدول.

يحتوي جدول النظام [system.clusters](../../operations/system-tables/clusters.md) على عنقود يحمل اسمًا مطابقًا لقاعدة البيانات المكررة، ويتكوّن من جميع النسخ المتماثلة الخاصة بقاعدة البيانات. ويُحدَّث هذا العنقود تلقائيًا عند إنشاء نسخ متماثلة أو حذفها، ويمكن استخدامه مع جداول [Distributed](/ar/engines/table-engines/special/distributed).

عند إنشاء نسخة متماثلة جديدة لقاعدة البيانات، تنشئ هذه النسخة المتماثلة الجداول بنفسها. وإذا كانت النسخة المتماثلة غير متاحة لفترة طويلة وتخلّفت عن سجل النسخ المتماثل، فإنها تتحقق من البيانات الوصفية المحلية لديها بمقارنتها مع البيانات الوصفية الحالية في ZooKeeper، وتنقل الجداول الزائدة مع بياناتها إلى قاعدة بيانات منفصلة non-replicated (حتى لا يُحذف أي شيء إضافي عن طريق الخطأ)، وتُنشئ الجداول المفقودة، وتُحدِّث أسماء الجداول إذا كانت قد أُعيدت تسميتها. ويتم النسخ المتماثل للبيانات على مستوى `ReplicatedMergeTree`، أي إذا لم يكن الجدول replicated فلن يتم النسخ المتماثل للبيانات (إذ إن قاعدة البيانات مسؤولة فقط عن البيانات الوصفية).

استعلامات [`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) مسموح بها، لكنها لا تخضع للـ النسخ المتماثل. سيقوم محرك قاعدة البيانات فقط بإضافة/جلب/إزالة partition أو part على النسخة المتماثلة الحالية. ومع ذلك، إذا كان الجدول نفسه يستخدم Replicated table engine، فسيتم النسخ المتماثل للبيانات بعد استخدام `ATTACH`.

إذا كنت تحتاج فقط إلى تهيئة عنقود من دون الحفاظ على النسخ المتماثل للجداول، فارجع إلى ميزة [Cluster Discovery](../../operations/cluster-discovery.md).

<div id="usage-example">
  ## مثال على الاستخدام
</div>

إنشاء عنقود يضم ثلاثة مضيفين:

```sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

إنشاء قاعدة بيانات على مستوى العنقود باستخدام معلمات ضمنية:

```sql
CREATE DATABASE r ON CLUSTER default ENGINE=Replicated;
```

تشغيل استعلام DDL:

```sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

```text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

إظهار جدول النظام:

```sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

إنشاء جدول موزّع وإدراج البيانات:

```sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

إضافة نسخة متماثلة على مضيف آخر:

```sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

إضافة نسخة متماثلة على مضيف آخر إذا استُخدم الماكرو `{uuid}` في `zoo_path`:

```sql
node1 :) SELECT uuid FROM system.databases WHERE database='r';
node4 :) CREATE DATABASE r UUID '<uuid from previous query>' ENGINE=Replicated('some/path/{uuid}','other_shard','r2');
```

سيبدو تكوين العنقود كما يلي:

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

سيجلب الجدول الموزّع البيانات أيضًا من المضيف الجديد:

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

<div id="settings">
  ## الإعدادات
</div>

الإعدادات التالية مدعومة:

| الإعداد                                                                      | القيمة الافتراضية              | الوصف                                                                                                                                                                                                                                                                                                                                            |
| ---------------------------------------------------------------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `max_broken_tables_ratio`                                                    | 1                              | لا تُجرِ استعادة النسخة المتماثلة تلقائيًا إذا كانت نسبة الجداول القديمة إلى إجمالي الجداول أكبر من ذلك                                                                                                                                                                                                                                          |
| `max_replication_lag_to_enqueue`                                             | 50                             | ستُطلق النسخة المتماثلة استثناءً عند محاولة تنفيذ استعلام إذا كان تأخر النسخ المتماثل لديها أكبر من ذلك                                                                                                                                                                                                                                          |
| `wait_entry_commited_timeout_sec`                                            | 3600                           | ستحاول النسخ المتماثلة إلغاء الاستعلام إذا تم تجاوز المهلة، لكن المضيف المُبادِر لم يكن قد نفّذه بعد                                                                                                                                                                                                                                             |
| `collection_name`                                                            |                                | اسم مجموعة معرّفة في إعدادات الخادم، حيث تكون جميع معلومات مصادقة العنقود معرّفة                                                                                                                                                                                                                                                                 |
| `check_consistency`                                                          | true                           | تحقّق من اتساق البيانات الوصفية المحلية والبيانات الوصفية في Keeper، ونفّذ استعادة النسخة المتماثلة عند وجود عدم اتساق                                                                                                                                                                                                                           |
| `max_retries_before_automatic_recovery`                                      | 10                             | الحد الأقصى لعدد محاولات تنفيذ إدخال في قائمة الانتظار قبل اعتبار النسخة المتماثلة مفقودة واستعادتها من لقطة snapshot (0 تعني عددًا غير محدود)                                                                                                                                                                                                   |
| `allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views` | false                          | إذا كان مفعّلًا، فعند معالجة DDLs في قواعد بيانات Replicated، يتم تخطي إنشاء وتبادل DDLs الخاصة بالجداول المؤقتة التابعة للعروض المادية القابلة للتحديث إن أمكن                                                                                                                                                                                  |
| `logs_to_keep`                                                               | 1000                           | العدد الافتراضي من السجلات التي يجب الاحتفاظ بها في ZooKeeper لقاعدة بيانات Replicated.                                                                                                                                                                                                                                                          |
| `default_replica_path`                                                       | `/clickhouse/databases/{uuid}` | المسار إلى قاعدة البيانات في ZooKeeper. يُستخدم أثناء إنشاء قاعدة البيانات إذا تم حذف الوسيطات.                                                                                                                                                                                                                                                  |
| `default_replica_shard_name`                                                 | `{shard}`                      | اسم الشارد الخاص بالنسخة المتماثلة في قاعدة البيانات. يُستخدم أثناء إنشاء قاعدة البيانات إذا تم حذف الوسيطات.                                                                                                                                                                                                                                     |
| `default_replica_name`                                                       | `{replica}`                    | اسم النسخة المتماثلة في قاعدة البيانات. يُستخدم أثناء إنشاء قاعدة البيانات إذا تم حذف الوسيطات.                                                                                                                                                                                                                                                  |
| `internal_replication`                                                       | false                          | ما إذا كان جدول Distributed المُنشأ باستخدام عنقود قاعدة البيانات Replicated هذه سيرسل البيانات إلى إحدى النسخ المتماثلة (تعني internal replication أن نسخ العنقود تنفّذ النسخ المتماثل فيما بينها بنفسها) أو إلى جميع النسخ المتماثلة (عدم وجود internal replication يعني أن جدول Distributed سيرسل البيانات المُدرجة إلى جميع النسخ المتماثلة) |

يمكن تجاوز القيم الافتراضية في ملف الإعدادات

```xml
<clickhouse>
    <database_replicated>
        <max_broken_tables_ratio>0.75</max_broken_tables_ratio>
        <max_replication_lag_to_enqueue>100</max_replication_lag_to_enqueue>
        <wait_entry_commited_timeout_sec>1800</wait_entry_commited_timeout_sec>
        <collection_name>postgres1</collection_name>
        <check_consistency>false</check_consistency>
        <max_retries_before_automatic_recovery>5</max_retries_before_automatic_recovery>
        <default_replica_path>/clickhouse/databases/{uuid}</default_replica_path>
        <default_replica_shard_name>{shard}</default_replica_shard_name>
        <default_replica_name>{replica}</default_replica_name>
        <internal_replication>false</internal_replication>
    </database_replicated>
</clickhouse>
```