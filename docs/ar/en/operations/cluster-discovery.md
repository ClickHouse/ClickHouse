---
description: 'وثائق حول اكتشاف العنقود في ClickHouse'
sidebar_label: 'اكتشاف العنقود'
slug: /operations/cluster-discovery
title: 'اكتشاف العنقود'
doc_type: 'guide'
---

<div id="overview">
  ## نظرة عامة
</div>

تُبسّط ميزة اكتشاف العنقود في ClickHouse تهيئة العنقود، إذ تتيح للعُقد اكتشاف نفسها وتسجيلها تلقائيًا من دون الحاجة إلى تعريفها صراحةً في ملفات التهيئة. ويكون ذلك مفيدًا بصورة خاصة عندما يصبح التعريف اليدوي لكل عقدة أمرًا مرهقًا.

:::note

تُعد ميزة اكتشاف العنقود ميزة تجريبية، وقد تتغير أو تُزال في الإصدارات القادمة.
لتمكينها، أضِف الإعداد `allow_experimental_cluster_discovery` إلى ملف التهيئة:

```xml
<clickhouse>
    <!-- ... -->
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <!-- ... -->
</clickhouse>
```

:::

<div id="remote-servers-configuration">
  ## إعداد الخوادم البعيدة
</div>

<div id="traditional-manual-configuration">
  ### الإعداد اليدوي التقليدي
</div>

تقليديًا، في ClickHouse، كان يجب تحديد كل مقطع وreplica في الـعنقود يدويًا ضمن الإعداد:

```xml
<remote_servers>
    <cluster_name>
        <shard>
            <replica>
                <host>node1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>node3</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node4</host>
                <port>9000</port>
            </replica>
        </shard>
    </cluster_name>
</remote_servers>

```

<div id="using-cluster-discovery">
  ### استخدام اكتشاف المجموعة
</div>

مع اكتشاف المجموعة، بدلًا من تعريف كل عقدة على نحو صريح، يكفي تحديد مسار في ZooKeeper. وستُكتشَف تلقائيًا جميع العقد التي تُسجَّل تحت هذا المسار في ZooKeeper وتُضاف إلى العنقود.

```xml
<remote_servers>
    <cluster_name>
        <discovery>
            <path>/clickhouse/discovery/cluster_name</path>

            <!-- # Optional configuration parameters: -->

            <!-- ## Authentication credentials to access all other nodes in cluster: -->
            <!-- <user>user1</user> -->
            <!-- <password>pass123</password> -->
            <!-- ### Alternatively to password, interserver secret may be used: -->
            <!-- <secret>secret123</secret> -->

            <!-- ## Shard for current node (see below): -->
            <!-- <shard>1</shard> -->

            <!-- ## Observer mode (see below): -->
            <!-- <observer/> -->
        </discovery>
    </cluster_name>
</remote_servers>
```

إذا كنت تريد تحديد رقم المقطع لعقدة محددة، فيمكنك تضمين الوسم `<shard>` في قسم `<discovery>`:

بالنسبة إلى `node1` و`node2`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>1</shard>
</discovery>
```

بالنسبة لـ `node3` و`node4`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>2</shard>
</discovery>
```

<div id="observer-mode">
  ### وضع المراقِب
</div>

لن تسجّل العُقد المُعدّة في وضع المراقِب نفسها كنسخ متماثلة.
وسيقتصر دورها على مراقبة النسخ المتماثلة النشطة الأخرى في العنقود واكتشافها دون المشاركة الفعلية.
لتمكين وضع المراقِب، أدرِج الوسم `<observer/>` ضمن قسم `<discovery>`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <observer/>
</discovery>
```

<div id="discovery-of-clusters">
  ### اكتشاف العناقيد
</div>

قد تحتاج أحيانًا إلى إضافة وإزالة ليس فقط المضيفين داخل العناقيد، بل العناقيد نفسها أيضًا. يمكنك استخدام العقدة `<multicluster_root_path>` مع المسار الجذر لعدة عناقيد:

```xml
<remote_servers>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

في هذه الحالة، عندما يسجّل مضيف آخر نفسه عند المسار `/clickhouse/discovery/some_new_cluster`، سيُضاف عنقود باسم `some_new_cluster`.

يمكنك استخدام الطريقتين معًا في الوقت نفسه، إذ يمكن للمضيف أن يسجّل نفسه في العنقود `my_cluster` وأن يكتشف أي عناقيد أخرى:

```xml
<remote_servers>
    <my_cluster>
        <discovery>
            <path>/clickhouse/discovery/my_cluster</path>
        </discovery>
    </my_cluster>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

القيود:

* لا يمكنك استخدام كلٍّ من `<path>` و`<multicluster_root_path>` ضمن الشجرة الفرعية نفسها `remote_servers`.
* لا يمكن استخدام `<multicluster_root_path>` إلا مع `<observer/>`.
* يُستخدم الجزء الأخير من المسار في Keeper بوصفه اسم العنقود، بينما عند التسجيل يُؤخذ الاسم من وسم XML.

<div id="use-cases-and-limitations">
  ## حالات الاستخدام والقيود
</div>

عند إضافة عُقد إلى مسار ZooKeeper المحدد أو إزالتها منه، يتم اكتشافها تلقائيًا أو استبعادها من العنقود دون الحاجة إلى تغيير التهيئة أو إعادة تشغيل الخادم.

ومع ذلك، تؤثر هذه التغييرات في تهيئة العنقود فقط، ولا تؤثر في البيانات أو قواعد البيانات والجداول الموجودة.

انظر إلى المثال التالي لعنقود مكوّن من 3 عُقد:

```xml
<remote_servers>
    <default>
        <discovery>
            <path>/clickhouse/discovery/default_cluster</path>
        </discovery>
    </default>
</remote_servers>
```

```sql
SELECT * EXCEPT (default_database, errors_count, slowdowns_count, estimated_recovery_time, database_shard_name, database_replica_name)
FROM system.clusters WHERE cluster = 'default';

┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

```sql
CREATE TABLE event_table ON CLUSTER default (event_time DateTime, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/event_table', '{replica}')
ORDER BY event_time PARTITION BY toYYYYMM(event_time);

INSERT INTO event_table ...
```

بعد ذلك، نضيف عقدة جديدة إلى العنقود عبر تشغيل عقدة جديدة تتضمن الإدخال نفسه في قسم `remote_servers` داخل ملف الإعدادات:

```response
┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           4 │ b0df3669b81f │ 172.26.0.6   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

تشارك العقدة الرابعة في العنقود، لكن الجدول `event_table` لا يزال موجودًا فقط على العقد الثلاث الأولى:

```sql
SELECT hostname(), database, table FROM clusterAllReplicas(default, system.tables) WHERE table = 'event_table' FORMAT PrettyCompactMonoBlock

┌─hostname()───┬─database─┬─table───────┐
│ a6a68731c21b │ default  │ event_table │
│ 92d3c04025e8 │ default  │ event_table │
│ 8e62b9cb17a1 │ default  │ event_table │
└──────────────┴──────────┴─────────────┘
```

إذا كنت بحاجة إلى تكرار الجداول على جميع العُقد، فيمكنك استخدام محرك قاعدة البيانات [Replicated](../engines/database-engines/replicated.md) كبديل لاكتشاف العنقود.