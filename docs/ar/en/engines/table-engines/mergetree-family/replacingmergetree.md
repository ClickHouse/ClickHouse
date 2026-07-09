---
description: 'يختلف عن MergeTree في أنه يزيل الإدخالات المكررة التي لها قيمة مفتاح الفرز نفسها
  (قسم `ORDER BY`، وليس `PRIMARY KEY`).'
sidebar_label: 'ReplacingMergeTree'
sidebar_position: 40
slug: /engines/table-engines/mergetree-family/replacingmergetree
title: 'محرك الجدول ReplacingMergeTree'
doc_type: 'reference'
---

يختلف هذا المحرك عن [MergeTree](/ar/engines/table-engines/mergetree-family/mergetree) في أنه يزيل الإدخالات المكررة التي لها قيمة [مفتاح الفرز](../../../engines/table-engines/mergetree-family/mergetree.md) نفسها (قسم `ORDER BY`، وليس `PRIMARY KEY`).

لا تحدث إزالة التكرار للبيانات إلا أثناء عملية دمج. وتتم عملية الدمج في الخلفية في وقت غير معروف، لذلك لا يمكنك التخطيط لها. وقد يبقى بعض البيانات غير مُعالج. وعلى الرغم من أنه يمكنك تشغيل عملية دمج غير مجدولة باستخدام استعلام `OPTIMIZE`، فلا تعتمد على ذلك، لأن استعلام `OPTIMIZE` سيقرأ ويكتب كمية كبيرة من البيانات.

لذلك، يُعد `ReplacingMergeTree` مناسبًا لإزالة البيانات المكررة في الخلفية لتوفير المساحة، لكنه لا يضمن عدم وجود تكرارات.

:::note
يتوفر [هنا](/ar/guides/replacing-merge-tree) دليل مفصل حول ReplacingMergeTree، بما في ذلك أفضل الممارسات وكيفية تحسين الأداء.
:::

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

للاطلاع على وصف مَعلمات الطلب، راجع [وصف التعليمة](../../../sql-reference/statements/create/table.md).

:::note
يُحدَّد تفرّد الصفوف من خلال قسم `ORDER BY`، وليس `PRIMARY KEY`.
:::

<div id="replacingmergetree-parameters">
  ## معلمات ReplacingMergeTree
</div>

<div id="ver">
  ### `ver`
</div>

`ver` — عمود يحتوي على رقم الإصدار. النوع `UInt*` أو `Date` أو `DateTime` أو `DateTime64`. مُعامِل اختياري.

عند الدمج، يُبقي `ReplacingMergeTree` على صف واحد فقط من بين جميع الصفوف التي لها مفتاح الفرز نفسه:

* الصف الأخير ضمن مجموعة الاختيار، إذا لم يتم تعيين `ver`. ومجموعة الاختيار هي مجموعة من الصفوف ضمن مجموعة من الأجزاء المشاركة في الدمج. ويكون الجزء الذي أُنشئ مؤخرًا (آخر عملية إدراج) هو الأخير في مجموعة الاختيار. لذلك، بعد إزالة التكرار، سيبقى آخر صف من أحدث عملية إدراج لكل مفتاح فرز فريد.
* الصف ذو أعلى إصدار، إذا تم تحديد `ver`. وإذا كانت قيمة `ver` متطابقة في عدة صفوف، فستُطبَّق عليها قاعدة &quot;إذا لم يتم تحديد `ver`&quot;، أي سيبقى الصف الذي أُدرج مؤخرًا.

مثال:

```sql
-- without ver - the last inserted 'wins'
CREATE TABLE myFirstReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO myFirstReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO myFirstReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM myFirstReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ second  │ 2020-01-01 00:00:00 │
└─────┴─────────┴─────────────────────┘


-- with ver - the row with the biggest ver 'wins'
CREATE TABLE mySecondReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO mySecondReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO mySecondReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM mySecondReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ first   │ 2020-01-01 01:01:01 │
└─────┴─────────┴─────────────────────┘
```

<div id="is_deleted">
  ### `is_deleted`
</div>

`is_deleted` — اسم عمود يُستخدم أثناء الدمج لتحديد ما إذا كانت البيانات في هذا الصف تمثل الحالة أم ينبغي حذفها؛ `1` هو صف &quot;محذوف&quot;، و`0` هو صف &quot;حالة&quot;.

نوع بيانات العمود — `UInt8`.

:::note
لا يمكن تمكين `is_deleted` إلا عند استخدام `ver`.

بغضّ النظر عن العملية المُجراة على البيانات، يجب زيادة رقم الإصدار. وإذا كان لصفّين مُدرجَين رقم الإصدار نفسه، فسيُحتفَظ بآخر صف تم إدراجه.

بشكل افتراضي، يحتفظ ClickHouse بآخر صف لمفتاح معيّن حتى إذا كان ذلك الصف صف حذف. والغاية من ذلك هي ضمان إمكانية
إدراج أي صفوف لاحقة ذات إصدارات أقل بأمان، مع استمرار تطبيق صف الحذف.

لحذف صفوف الحذف هذه نهائيًا، فعِّل إعداد الجدول `allow_experimental_replacing_merge_with_cleanup` ثم نفِّذ أحد الخيارين التاليين:

1. اضبط إعدادات الجدول `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge` و`min_age_to_force_merge_on_partition_only` و`min_age_to_force_merge_seconds`. إذا كانت جميع الأجزاء في partition أقدم من `min_age_to_force_merge_seconds`، فسيقوم ClickHouse بدمجها
   كلها في part واحد وإزالة أي صفوف حذف.

2. شغِّل يدويًا `OPTIMIZE TABLE table [PARTITION partition | PARTITION ID 'partition_id'] FINAL CLEANUP`.
   :::

مثال:

```sql
-- with ver and is_deleted
CREATE OR REPLACE TABLE myThirdReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime,
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(eventTime, is_deleted)
ORDER BY key
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 0);
INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 1);

select * from myThirdReplacingMT final;

0 rows in set. Elapsed: 0.003 sec.

-- delete rows with is_deleted
OPTIMIZE TABLE myThirdReplacingMT FINAL CLEANUP;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 00:00:00', 0);

select * from myThirdReplacingMT final;

┌─key─┬─someCol─┬───────────eventTime─┬─is_deleted─┐
│   1 │ first   │ 2020-01-01 00:00:00 │          0 │
└─────┴─────────┴─────────────────────┴────────────┘
```

<div id="query-clauses">
  ## عبارات الاستعلام
</div>

عند إنشاء جدول `ReplacingMergeTree`، يلزم استخدام [العبارات](../../../engines/table-engines/mergetree-family/mergetree.md) نفسها المطلوبة عند إنشاء جدول `MergeTree`.

<details markdown="1">
  <summary>طريقة قديمة لإنشاء جدول</summary>

  :::note
  لا تستخدم هذه الطريقة في المشاريع الجديدة، وحاوِل، إن أمكن، نقل المشاريع القديمة إلى الطريقة الموضحة أعلاه.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
  ```

  جميع المَعلمات، باستثناء `ver`، لها المعنى نفسه كما في `MergeTree`.

  * `ver` - العمود الذي يحتوي على الإصدار. مَعلمة اختيارية. للاطلاع على الوصف، راجع النص أعلاه.
</details>

<div id="query-time-de-duplication--final">
  ## إزالة التكرار وقت الاستعلام &amp; FINAL
</div>

عند وقت الدمج، يحدِّد ReplacingMergeTree الصفوف المكررة باستخدام قيم أعمدة `ORDER BY` (المستخدمة في إنشاء الجدول) باعتبارها معرّفًا فريدًا، ولا يُبقي إلا على أعلى إصدار. لكن هذا لا يوفّر سوى صحة نهائية فقط، ولا يضمن إزالة تكرار الصفوف، لذا لا ينبغي الاعتماد عليه. لذلك قد تُنتج الاستعلامات نتائج غير صحيحة بسبب احتساب صفوف التحديث والحذف ضمن الاستعلامات.

وللحصول على نتائج صحيحة، يحتاج المستخدمون إلى استكمال عمليات الدمج في الخلفية بإزالة التكرار وقت الاستعلام وإزالة الصفوف المحذوفة. ويمكن تحقيق ذلك باستخدام المعامل `FINAL`. على سبيل المثال، تأمل المثال التالي:

```sql
CREATE TABLE rmt_example
(
    `number` UInt16
)
ENGINE = ReplacingMergeTree
ORDER BY number

INSERT INTO rmt_example SELECT floor(randUniform(0, 100)) AS number
FROM numbers(1000000000)

0 rows in set. Elapsed: 19.958 sec. Processed 1.00 billion rows, 8.00 GB (50.11 million rows/s., 400.84 MB/s.)
```

يؤدي الاستعلام بدون `FINAL` إلى إظهار عدد غير صحيح (ستختلف النتيجة الدقيقة بحسب عمليات الدمج):

```sql
SELECT count()
FROM rmt_example

┌─count()─┐
│     200 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

تؤدي إضافة FINAL إلى نتيجة صحيحة:

```sql
SELECT count()
FROM rmt_example
FINAL

┌─count()─┐
│     100 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

لمزيد من التفاصيل حول `FINAL`، بما في ذلك كيفية تحسين أدائه، نوصي بقراءة [دليلنا التفصيلي حول ReplacingMergeTree](/ar/guides/replacing-merge-tree).