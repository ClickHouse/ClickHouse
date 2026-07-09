---
description: 'يرث CoalescingMergeTree من محرك MergeTree. وتتمثل ميزته الأساسية
  في قدرته على تخزين أحدث قيمة غير NULL لكل عمود تلقائيًا أثناء عمليات دمج الأجزاء.'
sidebar_label: 'CoalescingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/coalescingmergetree
title: 'محرك الجدول CoalescingMergeTree'
keywords: ['CoalescingMergeTree']
show_related_blogs: true
doc_type: 'reference'
---

:::note متاح ابتداءً من الإصدار 25.6
يتوفر محرك الجدول هذا ابتداءً من الإصدار 25.6 وما بعده في كل من OSS وCloud.
:::

يرث هذا المحرك من [MergeTree](/ar/engines/table-engines/mergetree-family/mergetree). ويتمثل الفرق الأساسي في كيفية دمج أجزاء البيانات: ففي جداول `CoalescingMergeTree`، يستبدل ClickHouse جميع الصفوف التي لها المفتاح الأساسي نفسه (أو، على نحو أدق، [مفتاح الفرز](../../../engines/table-engines/mergetree-family/mergetree.md) نفسه) بصف واحد يحتوي على أحدث القيم غير NULL لكل عمود.

يتيح ذلك عمليات upsert على مستوى الأعمدة، ما يعني أنه يمكنك تحديث أعمدة محددة فقط بدلًا من صفوف كاملة.

صُمم `CoalescingMergeTree` للاستخدام مع الأنواع Nullable في الأعمدة غير المفتاحية. وإذا لم تكن الأعمدة من النوع Nullable، فسيكون السلوك مماثلًا لما هو عليه في [ReplacingMergeTree](/ar/engines/table-engines/mergetree-family/replacingmergetree).

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CoalescingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

للاطّلاع على وصف معلمات الطلب، راجع [وصف الطلب](../../../sql-reference/statements/create/table.md).

<div id="parameters-of-coalescingmergetree">
  ### معلمات CoalescingMergeTree
</div>

<div id="columns">
  #### الأعمدة
</div>

`columns` - اختياري. مجموعة مرتبة تحتوي على أسماء الأعمدة التي ستُوحَّد قيمها. يجب ألا تكون الأعمدة المحددة جزءًا من مفتاح التقسيم أو مفتاح الفرز. إذا لم يتم تحديد `columns`، فسيُوحِّد ClickHouse القيم في جميع الأعمدة غير الموجودة في مفتاح الفرز.

<div id="query-clauses">
  ### بنود الاستعلام
</div>

عند إنشاء جدول `CoalescingMergeTree`، تكون [البنود](../../../engines/table-engines/mergetree-family/mergetree.md) نفسها مطلوبة كما عند إنشاء جدول `MergeTree`.

<details markdown="1">
  <summary>الطريقة المهجورة لإنشاء جدول</summary>

  :::note
  لا تستخدم هذه الطريقة في المشاريع الجديدة، وإن أمكن، فحوّل المشاريع القديمة إلى الطريقة الموضحة أعلاه.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] CoalescingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  جميع المعلمات، باستثناء `columns`، لها المعنى نفسه كما في `MergeTree`.

  * `columns` — قيمة من النوع `Tuple` تحتوي على أسماء الأعمدة التي ستُجمع قيمها. هذه معلمة اختيارية. للاطلاع على الوصف، راجع النص أعلاه.
</details>

<div id="usage-example">
  ## مثال استخدام
</div>

لنفترض الجدول التالي:

```sql
CREATE TABLE test_table
(
    key UInt64,
    value_int Nullable(UInt32),
    value_string Nullable(String),
    value_date Nullable(Date)
)
ENGINE = CoalescingMergeTree()
ORDER BY key
```

أدرج البيانات فيه:

```sql
INSERT INTO test_table VALUES(1, NULL, NULL, '2025-01-01'), (2, 10, 'test', NULL);
INSERT INTO test_table VALUES(1, 42, 'win', '2025-02-01');
INSERT INTO test_table(key, value_date) VALUES(2, '2025-02-01');
```

ستكون النتيجة كما يلي:

```sql
SELECT * FROM test_table ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   1 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-01-01 │
│   2 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-02-01 │
│   2 │        10 │ test         │       ᴺᵁᴸᴸ │
└─────┴───────────┴──────────────┴────────────┘
```

الاستعلام الموصى به للحصول على النتيجة الصحيحة والنهائية:

```sql
SELECT * FROM test_table FINAL ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   2 │        10 │ test         │ 2025-02-01 │
└─────┴───────────┴──────────────┴────────────┘
```

يفرض استخدام المُعدِّل `FINAL` على ClickHouse تطبيق منطق الدمج وقت تنفيذ الاستعلام، مما يضمن حصولك على قيمة &quot;أحدث&quot; صحيحة وموحَّدة لكل عمود. وهذه هي الطريقة الأكثر أمانًا ودقة عند الاستعلام من جدول CoalescingMergeTree.

:::note

قد يُرجع نهج يعتمد على `GROUP BY` نتائج غير صحيحة إذا لم تكن الأجزاء الأساسية قد دُمجت بالكامل.

```sql
SELECT key, last_value(value_int), last_value(value_string), last_value(value_date)  FROM test_table GROUP BY key; -- Not recommended.
```

:::

<div id="tuple-element-aggregation">
  ## تجميع عناصر `Tuple`
</div>

عند تمكين الإعداد `allow_tuple_element_aggregation`، تُسطَّح أعمدة `Tuple` بصورةٍ تكرارية بحيث يشارك كل عنصر طرفي في الدمج بصورة مستقلة. يتيح لك ذلك تخزين عدة حقول في عمود `Tuple` واحد ودمجها عنصرًا بعنصر أثناء عمليات الدمج، بحيث يحتفظ كل عمود فرعي `Nullable` بأحدث قيمة غير `NULL` بشكل مستقل.

تنطبق القواعد نفسها على الأعمدة الفرعية المُسطَّحة كما تنطبق على الأعمدة العادية:

* تُستبعد الأعمدة الفرعية التابعة لـ `Tuple` في مفتاح الفرز أو مفتاح التقسيم من الدمج.
* إذا تم تحديد `columns`، فلن تُدمج إلا الأعمدة الفرعية التابعة لأعمدة `Tuple` المُدرجة.

:::note
هذا الإعداد غير قابل للتغيير، ويجب تحديده عند إنشاء الجدول.
:::

```sql
CREATE TABLE coalescing_tuples
(
    key UInt64,
    data Tuple(
        value_a Nullable(UInt64),
        value_b Nullable(String),
        nested Tuple(
            value_c Nullable(UInt64)
        )
    )
) ENGINE = CoalescingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO coalescing_tuples VALUES (1, (100, NULL, (NULL)));
INSERT INTO coalescing_tuples VALUES (1, (NULL, 'hello', (42)));

SELECT key, data.value_a, data.value_b, data.nested.value_c FROM coalescing_tuples FINAL;
```

```text
┌─key─┬─data.value_a─┬─data.value_b─┬─data.nested.value_c─┐
│   1 │          100 │ hello        │                  42 │
└─────┴──────────────┴──────────────┴─────────────────────┘
```