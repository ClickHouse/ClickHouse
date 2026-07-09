---
description: 'توثيق Optimize'
sidebar_label: 'OPTIMIZE'
sidebar_position: 47
slug: /sql-reference/statements/optimize
title: 'تعليمة OPTIMIZE'
doc_type: 'مرجع'
---

يحاول هذا الاستعلام بدء عملية دمج غير مجدولة لأجزاء البيانات في الجداول. لاحظ أننا نوصي عمومًا بعدم استخدام `OPTIMIZE TABLE ... FINAL` (راجع [هذه الوثائق](/ar/optimize/avoidoptimizefinal))، لأن الغرض منه إداري وليس للاستخدام اليومي.

:::note
لا يمكن لـ `OPTIMIZE` إصلاح الخطأ `Too many parts`.
:::

**البنية**

```sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL | FORCE] [DEDUPLICATE [BY expression]]
```

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

استعلام `OPTIMIZE` مدعوم لعائلة [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) (بما في ذلك [العروض المادية](/ar/sql-reference/statements/create/view#materialized-view)) ومحرك [Buffer](../../engines/table-engines/special/buffer.md). أما محركات الجداول الأخرى فليست مدعومة.

عند استخدام `OPTIMIZE` مع عائلة محركات الجداول [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md)، ينشئ ClickHouse مهمة دمج وينتظر تنفيذها على جميع النسخ المتماثلة (إذا كان الإعداد [alter&#95;sync](/ar/operations/settings/settings#alter_sync) مضبوطًا على `2`) أو على النسخة المتماثلة الحالية (إذا كان الإعداد [alter&#95;sync](/ar/operations/settings/settings#alter_sync) مضبوطًا على `1`).

* إذا لم يُجرِ `OPTIMIZE` عملية دمج لأي سبب، فلن يُخطِر العميل بذلك. لتمكين الإشعارات، استخدم الإعداد [optimize&#95;throw&#95;if&#95;noop](/ar/operations/settings/settings#optimize_throw_if_noop).
* إذا حددت `PARTITION`، فلن يُحسَّن إلا التقسيم المحدد. [كيفية تعيين تعبير التقسيم](alter/partition.md#how-to-set-partition-expression).
* إذا حددت `FINAL` أو `FORCE`، فستُنفَّذ عملية التحسين حتى عندما تكون جميع البيانات موجودة بالفعل في جزء واحد. يمكنك التحكم في هذا السلوك باستخدام [optimize&#95;skip&#95;merged&#95;partitions](/ar/operations/settings/settings#optimize_skip_merged_partitions). كذلك، يُفرَض الدمج حتى إذا كانت هناك عمليات دمج متزامنة قيد التنفيذ.
* إذا حددت `DEDUPLICATE`، فستُزال الصفوف المتطابقة تمامًا (ما لم يتم تحديد عبارة BY) (تُقارَن جميع الأعمدة)، وهذا مفيد فقط مع محرك MergeTree.

يمكنك تحديد المدة (بالثواني) التي يجب انتظارها حتى تنفذ النسخ المتماثلة غير النشطة استعلامات `OPTIMIZE` باستخدام الإعداد [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ar/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
إذا كان `alter_sync` مضبوطًا على `2` وكانت بعض النسخ المتماثلة غير نشطة لمدة تتجاوز الوقت المحدد في الإعداد `replication_wait_for_inactive_replica_timeout`، فسيُطرَح الاستثناء `UNFINISHED`.
:::

<div id="dry-run">
  ## DRY RUN
</div>

تحاكي العبارة `DRY RUN` دمج الأجزاء المحددة من دون اعتماد النتيجة. ويُكتب الجزء المدمج في موقع مؤقت، ثم يُجرى التحقق منه، وبعد ذلك يُتخلَّص منه. وتبقى الأجزاء الأصلية وبيانات الجدول دون تغيير.

ويكون هذا مفيدًا من أجل:

* اختبار صحة الدمج عبر إصدارات ClickHouse المختلفة.
* إعادة إنتاج العيوب البرمجية المرتبطة بالدمج بصورة حتمية.
* قياس أداء الدمج.

لا يدعم `DRY RUN` إلا جداول عائلة [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). وتكون الكلمة المفتاحية `PARTS` مع قائمة بأسماء الأجزاء مطلوبة. ويجب أن تكون جميع الأجزاء المحددة موجودة ونشطة وتنتمي إلى التقسيم نفسه.

لا يتوافق `DRY RUN` مع `FINAL` و`PARTITION`. ويمكن دمجه مع `DEDUPLICATE` (مع تحديد اختياري للأعمدة) و`CLEANUP` (لجداول `ReplacingMergeTree`).

**البنية**

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

بشكل افتراضي، يتم التحقق من الجزء المدمج الناتج بطريقة مشابهة للاستعلام [`CHECK TABLE`](/ar/sql-reference/statements/check-table). يتحكم إعداد [optimize&#95;dry&#95;run&#95;check&#95;part](/ar/operations/settings/settings#optimize_dry_run_check_part) في هذا السلوك (وهو مفعّل افتراضيًا). يؤدي تعطيله إلى تخطي التحقق، وقد يكون ذلك مفيدًا لاختبار أداء عملية الدمج نفسها.

**مثال**

```sql
CREATE TABLE dry_run_example (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO dry_run_example VALUES (1, 'a'), (2, 'b');
INSERT INTO dry_run_example VALUES (1, 'c'), (4, 'd');

-- Simulate merging using two parts
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0';

-- Simulate merging with deduplication
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0' DEDUPLICATE;

-- Parts and data remain unchanged after DRY RUN
SELECT name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 'dry_run_example' AND active
ORDER BY name;
```

```response
┌─name────────┬─rows─┐
│ all_1_1_0   │    2 │
│ all_2_2_0   │    2 │
└─────────────┴──────┘
```

<div id="by-expression">
  ## تعبير BY
</div>

إذا كنت تريد تنفيذ إزالة التكرار على مجموعة مخصّصة من الأعمدة بدلًا من جميع الأعمدة، فيمكنك تحديد قائمة الأعمدة صراحةً أو استخدام أي مزيج من تعبيرات [`*`](../../sql-reference/statements/select/index.md#asterisk) أو [`COLUMNS`](/ar/sql-reference/statements/select#select-clause) أو [`EXCEPT`](/ar/sql-reference/statements/select/except-modifier). يجب أن تتضمن قائمة الأعمدة المكتوبة صراحةً أو الموسَّعة ضمنيًا جميع الأعمدة المحددة في تعبير ترتيب الصفوف (أي كلًّا من المفتاح الأساسي ومفتاح الفرز) وتعبير التقسيم (مفتاح التقسيم).

:::note
لاحظ أن `*` يتصرف تمامًا كما في `SELECT`: لا تُستخدم أعمدة [MATERIALIZED](/ar/sql-reference/statements/create/view#materialized-view) و[ALIAS](../../sql-reference/statements/create/table.md#alias) في التوسيع.

كما يُعدّ تحديد قائمة أعمدة فارغة، أو كتابة تعبير ينتج عنه قائمة أعمدة فارغة، أو تنفيذ إزالة التكرار باستخدام عمود `ALIAS`، خطأً.
:::

**البنية**

```sql
OPTIMIZE TABLE table DEDUPLICATE; -- all columns
OPTIMIZE TABLE table DEDUPLICATE BY *; -- excludes MATERIALIZED and ALIAS columns
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**أمثلة**

لنفترض الجدول التالي:

```sql title="Query"
CREATE TABLE example (
    primary_key Int32,
    secondary_key Int32,
    value UInt32,
    partition_key UInt32,
    materialized_value UInt32 MATERIALIZED 12345,
    aliased_value UInt32 ALIAS 2,
    PRIMARY KEY primary_key
) ENGINE=MergeTree
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);
```

```sql title="Query"
INSERT INTO example (primary_key, secondary_key, value, partition_key)
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```

```sql title="Query"
SELECT * FROM example;
```

```sql title="Response"

┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

تُنفَّذ جميع الأمثلة التالية على هذه الحالة، التي تضم 5 صفوف.

<div id="deduplicate">
  #### `DEDUPLICATE`
</div>

عند عدم تحديد الأعمدة المستخدمة لإزالة التكرار، تُؤخذ جميعها في الاعتبار. ولا يُزال الصف إلا إذا كانت جميع القيم في كل الأعمدة مساويةً للقيم المناظرة في الصف السابق:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-">
  #### `DEDUPLICATE BY *`
</div>

عند تحديد الأعمدة بشكل ضمني، تُزال الصفوف المكررة من الجدول بالاستناد إلى جميع الأعمدة التي ليست `ALIAS` أو `MATERIALIZED`. وبالنظر إلى الجدول أعلاه، تكون هذه الأعمدة هي: `primary_key` و`secondary_key` و`value` و`partition_key`

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by--except">
  #### `DEDUPLICATE BY * EXCEPT`
</div>

أزل التكرار باستخدام جميع الأعمدة التي ليست `ALIAS` أو `MATERIALIZED`، مع استبعاد `value` صراحةً: أعمدة `primary_key` و`secondary_key` و`partition_key`.

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-list-of-columns">
  #### `DEDUPLICATE BY <list of columns>`
</div>

أزِل التكرار بشكل صريح بحسب الأعمدة `primary_key` و`secondary_key` و`partition_key`:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-columnsregex">
  #### `DEDUPLICATE BY COLUMNS(<regex>)`
</div>

إزالة التكرار بحسب جميع الأعمدة المطابقة لتعبير نمطي: الأعمدة `primary_key` و`secondary_key` و`partition_key`:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```