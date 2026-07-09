---
description: 'يستبدل جميع الصفوف التي لها المفتاح الأساسي نفسه (أو بدقة أكبر، التي
  لها [مفتاح الفرز](../../../engines/table-engines/mergetree-family/mergetree.md)
  نفسه) بصف واحد (ضمن جزء بيانات واحد) يخزّن مجموعة من حالات
  الدوال التجميعية.'
sidebar_label: 'AggregatingMergeTree'
sidebar_position: 60
slug: /engines/table-engines/mergetree-family/aggregatingmergetree
title: 'محرك الجدول AggregatingMergeTree'
doc_type: 'reference'
---

يرث هذا المحرك من [MergeTree](/ar/engines/table-engines/mergetree-family/mergetree)، مع تعديل منطق دمج أجزاء البيانات. يستبدل ClickHouse جميع الصفوف التي لها المفتاح الأساسي نفسه (أو بدقة أكبر، التي لها [مفتاح الفرز](../../../engines/table-engines/mergetree-family/mergetree.md) نفسه) بصف واحد (ضمن جزء بيانات واحد) يخزّن مجموعة من حالات الدوال التجميعية.

يمكنك استخدام جداول `AggregatingMergeTree` لإجراء التجميع التدريجي للبيانات، بما في ذلك العروض المادية المجمّعة.

يمكنك مشاهدة مثال على كيفية استخدام AggregatingMergeTree والدوال التجميعية في الفيديو أدناه:

<div class="vimeo-container">
  <iframe width="1030" height="579" src="https://www.youtube.com/embed/pryhI4F_zqQ" title="حالات التجميع في ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />
</div>

يعالج هذا المحرك جميع الأعمدة ذات الأنواع التالية:

* [`AggregateFunction`](../../../sql-reference/data-types/aggregatefunction.md)
* [`SimpleAggregateFunction`](../../../sql-reference/data-types/simpleaggregatefunction.md)

يكون استخدام `AggregatingMergeTree` مناسبًا إذا كان يقلّل عدد الصفوف بعدة مراتب.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

للاطلاع على وصف معلمات الطلب، راجع [وصف الطلب](../../../sql-reference/statements/create/table.md).

**بنود الاستعلام**

عند إنشاء جدول `AggregatingMergeTree`، تكون [البنود](../../../engines/table-engines/mergetree-family/mergetree.md) نفسها مطلوبة كما هو الحال عند إنشاء جدول `MergeTree`.

<details markdown="1">
  <summary>الطريقة المتقادمة لإنشاء جدول</summary>

  :::note
  لا تستخدم هذه الطريقة في المشاريع الجديدة، وإن أمكن، انقل المشاريع القديمة إلى الطريقة الموضحة أعلاه.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  تحمل جميع المعلمات المعنى نفسه كما في `MergeTree`.
</details>

<div id="select-and-insert">
  ## SELECT and INSERT
</div>

لإدراج البيانات، استخدم استعلام [INSERT SELECT](../../../sql-reference/statements/insert-into.md) مع دوال التجميع ذات اللاحقة `-State`.
عند تحديد البيانات من جدول `AggregatingMergeTree`، استخدم عبارة `GROUP BY` ودوال التجميع نفسها المستخدمة عند إدراج البيانات، ولكن مع اللاحقة `-Merge`.

في نتائج استعلام `SELECT`، يكون لقيم النوع `AggregateFunction` تمثيل ثنائي خاص بالتنفيذ في جميع تنسيقات الإخراج في ClickHouse. على سبيل المثال، إذا قمت بتفريغ البيانات بتنسيق `TabSeparated` باستخدام استعلام `SELECT`، فيمكن تحميل هذا التفريغ مرةً أخرى باستخدام استعلام `INSERT`.

<div id="example-of-an-aggregated-materialized-view">
  ## مثال على عرض مادي مُجمَّع
</div>

يفترض المثال التالي أن لديك قاعدة بيانات باسم `test`. أنشئها إذا لم تكن موجودة من قبل باستخدام الأمر أدناه:

```sql
CREATE DATABASE test;
```

الآن، أنشئ الجدول `test.visits` الذي يحتوي على البيانات الخام:

```sql
CREATE TABLE test.visits
 (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Sign Nullable(Int32),
    UserID Nullable(Int32)
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);
```

بعد ذلك، تحتاج إلى جدول `AggregatingMergeTree` لتخزين `AggregationFunction` التي تتتبّع العدد الإجمالي للزيارات وعدد المستخدمين الفريدين.

أنشئ عرضًا ماديًا من نوع `AggregatingMergeTree` يراقب جدول `test.visits` ويستخدم النوع [`AggregateFunction`](/ar/sql-reference/data-types/aggregatefunction):

```sql
CREATE TABLE test.agg_visits (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Visits AggregateFunction(sum, Nullable(Int32)),
    Users AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID);
```

أنشئ عرضًا ماديًا يعبّئ `test.agg_visits` من `test.visits`:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;
```

أدرِج البيانات في الجدول `test.visits`:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1667446031000, 1, 3, 4), (1667446031000, 1, 6, 3);
```

تُدرَج البيانات في كلٍّ من `test.visits` و`test.agg_visits`.

للحصول على البيانات المُجمَّعة، نفِّذ استعلامًا مثل `SELECT ... GROUP BY ...` على العرض المادي `test.visits_mv`:

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.visits_mv
GROUP BY StartDate
ORDER BY StartDate;
```

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │      9 │     2 │
└─────────────────────────┴────────┴───────┘
```

أضِف سجلين آخرين إلى `test.visits`، لكن هذه المرة جرّب استخدام طابع زمني مختلف لأحد السجلين:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1669446031000, 2, 5, 10), (1667446031000, 3, 7, 5);
```

نفِّذ استعلام `SELECT` مرة أخرى، وسيُرجع المخرجات التالية:

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │     16 │     3 │
│ 2022-11-26 07:00:31.000 │      5 │     1 │
└─────────────────────────┴────────┴───────┘
```

في بعض الحالات، قد ترغب في تجنّب التجميع المسبق للصفوف وقت الإدراج، وذلك لنقل تكلفة التجميع من وقت الإدراج
إلى وقت الدمج. في المعتاد، يجب تضمين الأعمدة التي لا تدخل ضمن التجميع في عبارة `GROUP BY`
ضمن تعريف العرض المادي لتجنّب حدوث خطأ. ومع ذلك، يمكنك الاستفادة من الدالة [`initializeAggregation`](/ar/sql-reference/functions/other-functions#initializeAggregation)
مع الإعداد `optimize_on_insert = 0` (وهو مفعّل افتراضيًا) لتحقيق ذلك. وفي هذه الحالة، لم يعد استخدام `GROUP BY`
مطلوبًا:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    initializeAggregation('sumState', Sign) AS Visits,
    initializeAggregation('uniqState', UserID) AS Users
FROM test.visits;
```

:::note
عند استخدام `initializeAggregation`، تُنشأ حالة تجميع لكل صف على حدة من دون تجميع.
وينتج عن كل صف مصدر صف واحد في العرض المادي، بينما يحدث التجميع الفعلي لاحقًا عندما
يُجري `AggregatingMergeTree` دمج الأجزاء. ولا ينطبق ذلك إلا إذا كانت قيمة `optimize_on_insert = 0`.
:::

<div id="tuple-element-aggregation">
  ## تجميع عناصر Tuple
</div>

عندما يكون الإعداد `allow_tuple_element_aggregation` مفعّلًا، تُبسَّط أعمدة `Tuple` تكراريًا بحيث يشارك كل عنصر طرفي في التجميع بشكل مستقل. وهذا يعني أن الأعمدة الفرعية `AggregateFunction` أو `SimpleAggregateFunction` داخل `Tuple` تُجمَّع وفقًا للدوال الخاصة بها، تمامًا كما لو كانت أعمدة على المستوى الأعلى.

تُستبعَد الأعمدة الفرعية التابعة لـ `Tuple` ضمن مفتاح الفرز من التجميع. أمّا الأعمدة الفرعية غير التجميعية فتُعامل باعتبارها أعمدة عادية (ويُحتفَظ بأول قيمة لها).

:::note
هذا الإعداد غير قابل للتغيير ويجب تحديده عند إنشاء الجدول.
:::

```sql
CREATE TABLE agg_tuples
(
    key UInt32,
    metrics Tuple(
        total_visits SimpleAggregateFunction(sum, UInt64),
        unique_users SimpleAggregateFunction(max, UInt64)
    )
) ENGINE = AggregatingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO agg_tuples VALUES (1, (100, 5));
INSERT INTO agg_tuples VALUES (1, (200, 8));
INSERT INTO agg_tuples VALUES (2, (50, 3));

OPTIMIZE TABLE agg_tuples FINAL;

SELECT key, metrics.total_visits, metrics.unique_users FROM agg_tuples ORDER BY key;
```

```text
┌─key─┬─metrics.total_visits─┬─metrics.unique_users─┐
│   1 │                  300 │                    8 │
│   2 │                   50 │                    3 │
└─────┴──────────────────────┴──────────────────────┘
```

يُجمَّع `total_visits` باستخدام `sum` ‏(100 + 200 = 300)، بينما يُجمَّع `unique_users` باستخدام `max` ‏(max(5, 8) = 8).

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [استخدام مُبدِّلات التجميع في ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)