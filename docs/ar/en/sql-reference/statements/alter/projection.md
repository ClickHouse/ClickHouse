---
description: 'توثيق لإدارة الإسقاطات'
sidebar_label: 'PROJECTION'
sidebar_position: 49
slug: /sql-reference/statements/alter/projection
title: 'الإسقاطات'
doc_type: 'reference'
---

تشرح هذه الصفحة ماهية الإسقاطات، وكيفية استخدامها، والخيارات المختلفة لإدارتها.

<div id="overview">
  ## نظرة عامة على الإسقاطات
</div>

تخزّن الإسقاطات البيانات بتنسيق يُحسّن query execution، وتكون هذه الميزة مفيدة في الحالات التالية:

* تشغيل queries على عمود ليس جزءًا من primary key
* إجراء تجميع مسبق للأعمدة، مما يقلّل كلاً من العمليات الحسابية وIO

يمكنك تعريف إسقاط واحد أو أكثر لجدول، وأثناء query analysis سيختار ClickHouse الإسقاط الذي يتطلب أقل قدر من البيانات لمسحها، من دون تعديل query الذي يقدّمه المستخدم.

:::note[استخدام القرص]
تنشئ الإسقاطات داخليًا hidden table جديدًا، وهذا يعني الحاجة إلى المزيد من IO ومساحة أكبر على disk.
على سبيل المثال، إذا كان الإسقاط يعرّف primary key مختلفًا، فستُكرَّر جميع بيانات الجدول الأصلي.
:::

يمكنك الاطلاع على مزيد من التفاصيل التقنية حول كيفية عمل الإسقاطات داخليًا في هذه [الصفحة](/ar/guides/best-practices/sparse-primary-indexes.md/#option-3-projections).

<div id="examples">
  ## استخدام الإسقاطات
</div>

<div id="example-filtering-without-using-primary-keys">
  ### مثال على التصفية دون استخدام المفاتيح الأساسية
</div>

إنشاء الجدول:

```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```

باستخدام `ALTER TABLE`، يمكننا إضافة الإسقاط إلى جدولٍ موجود:

```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
    SELECT *
    ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```

إدراج البيانات:

```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

سيتيح لنا الإسقاط التصفية حسب `user_name` بسرعة، حتى إذا لم يكن `user_name` معرّفًا في `table` الأصلي على أنه `PRIMARY_KEY`.
في وقت تنفيذ الاستعلام، يحدّد ClickHouse أنه ستُعالَج بيانات أقل إذا استُخدم الإسقاط، لأن البيانات مرتبة حسب `user_name`.

```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

للتحقق من أن الاستعلام يستخدم الإسقاط، يمكننا مراجعة جدول `system.query_log`. في الحقل `projections` يظهر اسم الإسقاط المستخدم، أو يكون فارغًا إذا لم يُستخدم أي إسقاط:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="example-pre-aggregation-query">
  ### مثال على استعلام التجميع المسبق
</div>

أنشئ الجدول باستخدام الإسقاط `projection_visits_by_user`:

```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```

أدرِج البيانات:

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```

نفّذ استعلامًا أولًا باستخدام `GROUP BY` مع الحقل `user_agent`.
لن يستخدم هذا الاستعلام الـ إسقاط المحدَّد لأن التجميع المسبق لا يتطابق.

```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

للاستفادة من الإسقاط، يمكنك تنفيذ استعلامات تختار بعض حقول التجميع المسبق و`GROUP BY` أو جميعها:

```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```

```sql
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

كما ذُكر سابقًا، يمكنك مراجعة جدول `system.query_log` للتحقق مما إذا كان قد استُخدم إسقاط.
يعرض الحقل `projections` اسم الإسقاط المستخدم.
وسيكون فارغًا إذا لم يُستخدم أي إسقاط:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="projection-indexes">
  ### إنشاء فهارس الإسقاط واستخدامها
</div>

إنشاء [فهرس إسقاط](../../../engines/table-engines/mergetree-family/mergetree.md#projection-index):

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id INDEX user_id TYPE basic
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

<details markdown="1">
  <summary>إنشاء إسقاط باستخدام الحقل الصريح `_part_offset`</summary>

  يمكن أيضًا إنشاء فهارس الإسقاط باستخدام الصياغة التالية (غير موصى بها):

  ```sql
  CREATE TABLE events
  (
      `event_time` DateTime,
      `event_id` UInt64,
      `user_id` UInt64,
      `huge_string` String,
      PROJECTION order_by_user_id
      (
          SELECT
              _part_offset
          ORDER BY user_id
      )
  )
  ENGINE = MergeTree()
  ORDER BY (event_id);
  ```
</details>

إدراج بعض البيانات النموذجية:

```sql
INSERT INTO events SELECT * FROM generateRandom() LIMIT 100000;
```

يحتفظ الحقل `_part_offset` بقيمته عبر عمليات الدمج وعمليات التعديل، مما يجعله مفيدًا للفهرسة الثانوية. ويمكننا الاستفادة من ذلك في الاستعلامات:

```sql
SELECT
    count()
FROM events
WHERE _part_starting_offset + _part_offset IN (
    SELECT _part_starting_offset + _part_offset
    FROM events
    WHERE user_id = 42
)
SETTINGS enable_shared_storage_snapshot_in_query = 1
```

<div id="example-projection-with-where">
  ### مثال على إسقاط مع عبارة `WHERE`
</div>

يمكن أن تتضمن الإسقاطات عبارة `WHERE` لتخزين مجموعة فرعية فقط من الصفوف. ويكون ذلك مفيدًا عندما تُرشِّح الاستعلامات بشكل متكرر استنادًا إلى شرط معروف — إذ لا يُطبَّق الإسقاط إلا على الصفوف المطابقة، مما يقلل مساحة التخزين ويحسّن أداء الاستعلامات.

إنشاء جدول وإضافة إسقاط مُرشَّح:

```sql
CREATE TABLE events
(
    `event_type` String,
    `time` DateTime,
    `message` String
)
ENGINE = MergeTree()
ORDER BY time;

ALTER TABLE events ADD PROJECTION proj_pageview (
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

ALTER TABLE events MATERIALIZE PROJECTION proj_pageview;
```

إدراج البيانات:

```sql
INSERT INTO events VALUES
    ('pageview', '2024-01-01', 'homepage'),
    ('click', '2024-01-02', 'button'),
    ('pageview', '2024-01-03', 'about');
```

عندما **تستلزم منطقيًا** عبارة `WHERE` الخاصة بالاستعلام عبارة `WHERE` الخاصة بالإسقاط (أي إن كل شرط في مرشّح الإسقاط موجود أيضًا في مرشّح الاستعلام)، يمكن للمُحسِّن استخدام الإسقاط تلقائيًا عندما يحدّد أن ذلك مفيد:

```sql
-- This query implies the projection's WHERE, so the projection may be used:
SELECT time, message FROM events WHERE event_type = 'pageview';

-- A stricter query also implies the projection's WHERE:
SELECT time, message FROM events WHERE event_type = 'pageview' AND time > '2024-01-01';

-- This query does NOT imply the projection, so the base table is scanned:
SELECT time, message FROM events WHERE event_type = 'click';
```

فحص الاستلزام متحفّظ — إذ يستخدم مطابقةً تامةً للاقترانات في الصيغة القياسية لـ expression. وقد يفوّت بعض فرص التحسين الصحيحة (مثل استلزامات range)، لكنه لن ينتج أبدًا نتائج غير صحيحة.

<div id="manipulating-projections">
  ## إدارة الإسقاطات
</div>

تتوفّر العمليات التالية الخاصة بـ [الإسقاطات](/ar/engines/table-engines/mergetree-family/mergetree.md/#projections):

<div id="add-projection">
  ### ADD PROJECTION
</div>

استخدم تعليمة SQL أدناه لإضافة تعريف إسقاط إلى البيانات الوصفية لجدول:

```sql
-- Normal projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]

-- Aggregate projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]
```

:::note
عندما يعرّف الإسقاط عبارة `WHERE`، فلا تُجسَّد ماديًا إلا الصفوف التي تطابق الشرط. ويمكن للمُحسِّن استخدام مثل هذا الإسقاط عندما يستلزم `WHERE` الخاص بالاستعلام منطقيًا `WHERE` الخاص بالإسقاط، ويكون الإسقاط مفيدًا لخطة الاستعلام. وينطبق ذلك على كلٍّ من الإسقاطات العادية والتجميعية.
:::

<div id="with-settings">
  #### عبارة `WITH SETTINGS`
</div>

تحدّد `WITH SETTINGS` **إعدادات على مستوى الإسقاط**، التي تخصّص كيفية تخزين البيانات في الإسقاط (على سبيل المثال، `index_granularity` أو `index_granularity_bytes`).
وتتوافق هذه الإعدادات مباشرةً مع **إعدادات جدول MergeTree**، لكنها تنطبق **على هذا الإسقاط فقط**.

مثال:

```sql
ALTER TABLE t
ADD PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 4096,
    index_granularity_bytes = 1048576
);
```

تُطبَّق إعدادات الإسقاط بدلًا من إعدادات الجدول الفعّالة لهذا الإسقاط، مع الخضوع لقواعد التحقق (على سبيل المثال، سيُرفض أي تجاوز غير صالح أو غير متوافق).

<div id="drop-projection">
  ### DROP PROJECTION
</div>

استخدم التعليمة أدناه لإزالة وصف الإسقاط من البيانات الوصفية للجدول وحذف ملفات الإسقاط من القرص.
ويُنفَّذ ذلك على هيئة [عملية تعديل](/ar/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
```

<div id="materialize-projection">
  ### MATERIALIZE PROJECTION
</div>

استخدم التعليمة أدناه لإعادة بناء الإسقاط `name` في التقسيم `partition_name`.
يُنفَّذ هذا على شكل [عملية تعديل](/ar/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

<div id="clear-projection">
  ### CLEAR PROJECTION
</div>

استخدم التعليمة التالية لحذف ملفات الإسقاط من القرص دون إزالة الوصف المرتبط بها.
يُنفَّذ ذلك على شكل [عملية تعديل](/ar/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

تُعد الأوامر `ADD` و`DROP` و`CLEAR` خفيفة الوزن، بمعنى أنها لا تغيّر سوى البيانات الوصفية أو تزيل الملفات.
إضافةً إلى ذلك، فهي تُنسخ متماثلًا، وتُزامَن البيانات الوصفية للإسقاطات عبر ClickHouse Keeper أو ZooKeeper.

:::note
لا يُدعَم التعامل مع الإسقاطات إلا في الجداول التي تستخدم محرك [`*MergeTree`](/ar/engines/table-engines/mergetree-family/mergetree.md) (بما في ذلك [البدائل المكرّرة](/ar/engines/table-engines/mergetree-family/replication.md)).
:::

<div id="control-projections-merges">
  ### التحكّم في سلوك دمج الإسقاطات
</div>

عند تنفيذ استعلام، يختار ClickHouse بين القراءة من الجدول الأصلي أو من أحد إسقاطاته.
ويُتخذ قرار القراءة من الجدول الأصلي أو من أحد إسقاطاته بشكل مستقل لكل جزء من الجدول.
ويهدف ClickHouse عمومًا إلى قراءة أقل قدر ممكن من البيانات، ويستخدم بعض الأساليب لتحديد أفضل جزء للقراءة منه، مثل أخذ عينات من المفتاح الأساسي لأحد الأجزاء.
وفي بعض الحالات، لا تكون لأجزاء الجدول المصدر أجزاء إسقاط مقابلة.
وقد يحدث هذا، على سبيل المثال، لأن إنشاء إسقاط لجدول في SQL يكون &quot;كسولًا&quot; افتراضيًا، أي إنه لا يؤثر إلا في البيانات المُدرجة حديثًا ويترك الأجزاء الحالية كما هي.

وبما أن أحد الإسقاطات يحتوي بالفعل على قيم التجميع المحسوبة مسبقًا، يحاول ClickHouse القراءة من أجزاء الإسقاط المقابلة لتجنّب إعادة التجميع أثناء تنفيذ الاستعلام. وإذا كان جزء معيّن يفتقر إلى جزء الإسقاط المقابل، يعود تنفيذ الاستعلام إلى الجزء الأصلي.

ولكن ماذا يحدث إذا تغيّرت الصفوف في الجدول الأصلي بصورة غير بسيطة نتيجة عمليات دمج أجزاء البيانات في الخلفية؟
على سبيل المثال، افترض أن الجدول مخزَّن باستخدام محرك الجدول `ReplacingMergeTree`.
إذا اكتُشف الصف نفسه في عدة أجزاء إدخال أثناء الدمج، فلن يُحتفظ إلا بأحدث إصدار من الصف (من الجزء الذي أُدرج مؤخرًا)، بينما ستُستبعَد جميع الإصدارات الأقدم.

وبالمثل، إذا كان الجدول مخزَّنًا باستخدام محرك الجدول `AggregatingMergeTree`، فقد تدمج عملية الدمج الصفوف نفسها في أجزاء الإدخال (استنادًا إلى قيم المفتاح الأساسي) في صف واحد لتحديث حالات التجميع الجزئية.

قبل ClickHouse v24.8، كانت أجزاء الإسقاط إما تخرج بصمت عن المزامنة مع البيانات الأساسية، أو أن بعض العمليات مثل التحديثات وعمليات الحذف لم يكن بالإمكان تنفيذها على الإطلاق، لأن قاعدة البيانات كانت تطرح استثناءً تلقائيًا إذا كان الجدول يحتوي على إسقاطات.

منذ v24.8، يتحكّم إعداد جديد على مستوى الجدول [`deduplicate_merge_projection_mode`](/ar/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode) في السلوك عند حدوث عمليات دمج خلفية غير بسيطة، كما ذُكر أعلاه، في أجزاء الجدول الأصلي.

وتُعد Delete mutations مثالًا آخر على عمليات دمج الأجزاء التي تُسقِط الصفوف من أجزاء الجدول الأصلي. ومنذ v24.7، يتوفر أيضًا إعداد للتحكّم في السلوك فيما يتعلق بـ delete mutations التي تُفعَّل بواسطة lightweight deletes: [`lightweight_mutation_projection_mode`](/ar/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode).

فيما يلي القيم الممكنة لكلٍّ من `deduplicate_merge_projection_mode` و`lightweight_mutation_projection_mode`:

* `throw` (الافتراضي): يُطرح استثناء، مما يمنع أجزاء الإسقاط من الخروج عن المزامنة.
* `drop`: تُسقَط أجزاء جدول الإسقاط المتأثرة. وستعود الاستعلامات إلى جزء الجدول الأصلي بالنسبة إلى أجزاء الإسقاط المتأثرة.
* `rebuild`: يُعاد بناء جزء الإسقاط المتأثر ليظل متسقًا مع البيانات الموجودة في جزء الجدول الأصلي.

<div id="limitations">
  ## القيود
</div>

لا يمكن استخدام عمود `ALIAS` في عبارة `ORDER BY` الخاصة بـ إسقاط. على سبيل المثال:

```sql
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 ALIAS a + 1,
--highlight-next-line
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;
-- Fails with UNKNOWN_IDENTIFIER
```

أعمدة `ALIAS` لا تُخزَّن فعليًا، بل تُحتسب ديناميكيًا وقت تنفيذ الاستعلام، لذا لا تكون متاحة أثناء مسار كتابة جزء الإسقاط عند تقييم تعبير الترتيب.

بدلًا من ذلك، استخدم أعمدة `MATERIALIZED` أو ضمّن التعبير مباشرةً:

```sql
-- using MATERIALIZED column
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 MATERIALIZED a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

-- using an inline expression
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    PROJECTION p (SELECT a ORDER BY a + 1)
)
ENGINE = MergeTree ORDER BY id;
```

<div id="see-also">
  ## انظر أيضًا
</div>

* [&quot;التحكم في الإسقاطات أثناء عمليات الدمج&quot; (منشور مدونة)](https://clickhouse.com/blog/clickhouse-release-24-08#control-of-projections-during-merges)
* [&quot;الإسقاطات&quot; (دليل)](/ar/data-modeling/projections#using-projections-to-speed-up-UK-price-paid)
* [&quot;العروض المادية مقابل الإسقاطات&quot;](https://clickhouse.com/docs/managing-data/materialized-views-versus-projections)