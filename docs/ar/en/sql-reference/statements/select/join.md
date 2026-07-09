---
description: 'توثيق لعبارة JOIN'
sidebar_label: 'JOIN'
slug: /sql-reference/statements/select/join
title: 'عبارة JOIN'
keywords: ['INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', 'RIGHT JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN', 'LEFT SEMI JOIN', 'RIGHT SEMI JOIN', 'LEFT ANTI JOIN', 'RIGHT ANTI JOIN', 'LEFT ANY JOIN', 'RIGHT ANY JOIN', 'INNER ANY JOIN', 'ASOF JOIN', 'LEFT ASOF JOIN', 'PASTE JOIN', 'NATURAL JOIN']
doc_type: 'مرجع'
---

تُنتج العبارة `JOIN` جدولًا جديدًا عبر دمج الأعمدة من جدول واحد أو عدة جداول باستخدام القيم المشتركة بينها. وهي عملية شائعة في قواعد البيانات التي تدعم SQL، وتقابل عملية join في [الجبر العلائقي](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators). وغالبًا ما تُعرف الحالة الخاصة المتمثلة في ربط جدول بنفسه باسم &quot;self-join&quot;.

**الصياغة**

```sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

تُسمّى التعبيرات الواردة في عبارة `ON` والأعمدة الواردة في عبارة `USING` &quot;مفاتيح الربط&quot;. وما لم يُذكر خلاف ذلك، فإن `JOIN` يُنتج [حاصل ضرب ديكارتي](https://en.wikipedia.org/wiki/Cartesian_product) للصفوف التي تتطابق فيها &quot;مفاتيح الربط&quot;، وقد يؤدي ذلك إلى نتائج تضم عددًا من الصفوف أكبر بكثير من الجداول الأصلية.

<div id="supported-types-of-join">
  ## أنواع JOIN المدعومة
</div>

جميع أنواع [SQL JOIN](https://en.wikipedia.org/wiki/Join_\(SQL\)) القياسية مدعومة:

| النوع              | الوصف                                                                                                                                                                                                                                                            |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `INNER JOIN`       | لا تُعاد إلا الصفوف المتطابقة.                                                                                                                                                                                                                                   |
| `LEFT OUTER JOIN`  | تُعاد الصفوف غير المتطابقة من الجدول الأيسر بالإضافة إلى الصفوف المتطابقة.                                                                                                                                                                                       |
| `RIGHT OUTER JOIN` | تُعاد الصفوف غير المتطابقة من الجدول الأيمن بالإضافة إلى الصفوف المتطابقة.                                                                                                                                                                                       |
| `FULL OUTER JOIN`  | تُعاد الصفوف غير المتطابقة من كلا الجدولين بالإضافة إلى الصفوف المتطابقة.                                                                                                                                                                                        |
| `CROSS JOIN`       | يُنتج الضرب الديكارتي للجدولين بالكامل، ولا يتم تحديد &quot;مفاتيح الربط&quot;.                                                                                                                                                                                  |
| `NATURAL JOIN`     | يُجري الربط تلقائيًا على جميع الأعمدة التي تحمل الاسم نفسه في كلا الجدولين؛ ويظهر كل عمود مشترك مرة واحدة في النتيجة. يدعم الصيغ `INNER` (الافتراضي) و`LEFT` و`RIGHT` و`FULL`. وهو مكافئ لـ `JOIN ... USING (col1, col2, ...)` حيث تُشتق قائمة الأعمدة تلقائيًا. |

* `JOIN` من دون تحديد نوع يُفهم على أنه `INNER`.
* يمكن حذف الكلمة المفتاحية `OUTER` بأمان.
* صياغة بديلة لـ `CROSS JOIN` هي تحديد عدة جداول في [`FROM` clause](../../../sql-reference/statements/select/from.md) مفصولة بفواصل.
* إذا لم تكن هناك أعمدة متطابقة لـ `NATURAL JOIN`، فإنه يعمل مثل `CROSS JOIN`.

أنواع الربط الإضافية المتاحة في ClickHouse هي:

| النوع                                               | الوصف                                                                                                                           |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`                 | قائمة سماح على &quot;مفاتيح الربط&quot;، من دون إنتاج ضرب ديكارتي.                                                              |
| `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`                 | قائمة حظر على &quot;مفاتيح الربط&quot;، من دون إنتاج ضرب ديكارتي.                                                               |
| `LEFT ANY JOIN`, `RIGHT ANY JOIN`, `INNER ANY JOIN` | يعطّل جزئيًا (للجانب المقابل من `LEFT` و`RIGHT`) أو كليًا (بالنسبة إلى `INNER` و`FULL`) الضرب الديكارتي لأنواع `JOIN` القياسية. |
| `ASOF JOIN`, `LEFT ASOF JOIN`                       | ربط التسلسلات مع تطابق غير تام. يرد أدناه شرح استخدام `ASOF JOIN`.                                                              |
| `PASTE JOIN`                                        | يُجري دمجًا أفقيًا لجدولين.                                                                                                     |

:::note
عندما يتم ضبط [join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm) على `partial_merge`، فإن `RIGHT JOIN` و`FULL JOIN` مدعومان فقط مع strictness من النوع `ALL` (أما `SEMI` و`ANTI` و`ANY` و`ASOF` فغير مدعومة).
:::

<div id="settings">
  ## الإعدادات
</div>

يمكن تجاوز نوع `JOIN` الافتراضي باستخدام الإعداد [`join_default_strictness`](../../../operations/settings/settings.md#join_default_strictness).

يعتمد سلوك خادم ClickHouse في عمليات `ANY JOIN` على الإعداد [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys).

**انظر أيضًا**

* [`join_algorithm`](../../../operations/settings/settings.md#join_algorithm)
* [`join_any_take_last_row`](../../../operations/settings/settings.md#join_any_take_last_row)
* [`join_use_nulls`](../../../operations/settings/settings.md#join_use_nulls)
* [`partial_merge_join_rows_in_right_blocks`](../../../operations/settings/settings.md#partial_merge_join_rows_in_right_blocks)
* [`join_on_disk_max_files_to_merge`](../../../operations/settings/settings.md#join_on_disk_max_files_to_merge)
* [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)

استخدم الإعداد `cross_to_inner_join_rewrite` لتحديد السلوك عند فشل ClickHouse في إعادة كتابة `CROSS JOIN` إلى `INNER JOIN`. القيمة الافتراضية هي `1`، ما يسمح بمتابعة عملية الربط، لكنها ستكون أبطأ. اضبط `cross_to_inner_join_rewrite` على `0` إذا كنت تريد ظهور خطأ، واضبطه على `2` لعدم تشغيل عمليات `CROSS JOIN`، وبدلًا من ذلك فرض إعادة كتابة جميع عمليات الربط بالفاصلة أو `CROSS JOIN`. إذا فشلت إعادة الكتابة عندما تكون القيمة `2`، فستتلقى رسالة خطأ نصها: &quot;Please, try to simplify `WHERE` section&quot;.

<div id="on-section-conditions">
  ## شروط قسم `ON`
</div>

يمكن أن يحتوي قسم `ON` على عدة شروط مدمجة باستخدام العاملين `AND` و`OR`. ويجب أن تستوفي الشروط التي تحدد مفاتيح الربط ما يلي:

* أن تشير إلى كلٍّ من الجدول الأيسر والجدول الأيمن
* أن تستخدم عامل المساواة

يمكن للشروط الأخرى استخدام عوامل منطقية أخرى، لكن يجب أن تشير إلى الجدول الأيسر أو الجدول الأيمن في الاستعلام.

تُربط الصفوف إذا تحقق الشرط المركب بالكامل. وإذا لم تتحقق الشروط، فقد تظل الصفوف ضمن النتيجة بحسب نوع `JOIN`. لاحظ أنه إذا وُضعت الشروط نفسها في قسم `WHERE` ولم تتحقق، فستُستبعَد الصفوف دائمًا من النتيجة.

يعمل العامل `OR` داخل عبارة `ON` باستخدام خوارزمية الربط بالتجزئة (hash join) — إذ يُنشأ جدول تجزئة (hash table) منفصل لكل وسيط `OR` يحتوي على مفاتيح ربط لـ `JOIN`، لذلك يزداد استهلاك الذاكرة ووقت تنفيذ الاستعلام خطيًا مع زيادة عدد تعبيرات `OR` في عبارة `ON`.

:::note
إذا كان الشرط يشير إلى أعمدة من جداول مختلفة، فلا يُدعَم حاليًا سوى عامل المساواة (`=`).
:::

**مثال**

افترض وجود `table_1` و`table_2`:

```response
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1 │ A    │     │  1 │ Text A         │     10 │
│  2 │ B    │     │  1 │ Another text A │     12 │
│  3 │ C    │     │  2 │ Text B         │     15 │
└────┴──────┘     └────┴────────────────┴────────┘
```

استعلام بشرط واحد لمفتاح الربط وشرط إضافي لـ `table_2`:

```sql title="Query"
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

لاحظ أن النتيجة تتضمن الصف الذي يحمل الاسم `C` وعمود النص الفارغ. وقد أُدرج هذا الصف ضمن النتيجة لأنه استُخدم نوع `OUTER` من الربط.

```response title="Response"
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

استعلام بربط من النوع `INNER` وبشروط متعددة:

```sql title="Query"
SELECT name, text, scores FROM table_1 INNER JOIN table_2
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

```sql title="Response"
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │     15 │
└──────┴────────┴────────┘
```

استعلام بنوع `INNER` من عملية `JOIN` وبشرط يتضمن `OR`:

```sql title="Query"
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

استعلام بنوع ربط `INNER` وبشروط تستخدم `OR` و`AND`:

:::note

بشكل افتراضي، تكون شروط عدم المساواة مدعومة ما دامت تستخدم أعمدة من الجدول نفسه.
على سبيل المثال، `t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c`، لأن `t1.b > 0` يستخدم أعمدة من `t1` فقط، و`t2.b > t2.c` يستخدم أعمدة من `t2` فقط.
ومع ذلك، يمكنك تجربة الدعم التجريبي لشروط مثل `t1.a = t2.key AND t1.b > t2.key`؛ راجع القسم أدناه لمزيد من التفاصيل.

:::

```sql title="Query"
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

<div id="join-with-inequality-conditions-for-columns-from-different-tables">
  ## JOIN مع شروط عدم المساواة لأعمدة من جداول مختلفة
</div>

يدعم ClickHouse حاليًا `ALL/ANY/SEMI/ANTI INNER/LEFT/RIGHT/FULL JOIN` مع شروط عدم المساواة بالإضافة إلى شروط المساواة. ولا تُدعَم شروط عدم المساواة إلا مع خوارزميتي JOIN `hash` و`grace_hash`. كما أن شروط عدم المساواة لا تُدعَم عند استخدام `join_use_nulls`.

**مثال**

الجدول `t1`:

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ a    │ 1 │ 1 │ 2 │
│ key1 │ b    │ 2 │ 3 │ 2 │
│ key1 │ c    │ 3 │ 2 │ 1 │
│ key1 │ d    │ 4 │ 7 │ 2 │
│ key1 │ e    │ 5 │ 5 │ 5 │
│ key2 │ a2   │ 1 │ 1 │ 1 │
│ key4 │ f    │ 2 │ 3 │ 4 │
└──────┴──────┴───┴───┴───┘
```

الجدول `t2`

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ A    │ 1 │ 2 │ 1 │
│ key1 │ B    │ 2 │ 1 │ 2 │
│ key1 │ C    │ 3 │ 4 │ 5 │
│ key1 │ D    │ 4 │ 1 │ 6 │
│ key3 │ a3   │ 1 │ 1 │ 1 │
│ key4 │ F    │ 1 │ 1 │ 1 │
└──────┴──────┴───┴───┴───┘
```

```sql
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.a < t2.a) ORDER BY (t1.key, t1.attr, t2.key, t2.attr);
```

```response
key1    a    1    1    2    key1    B    2    1    2
key1    a    1    1    2    key1    C    3    4    5
key1    a    1    1    2    key1    D    4    1    6
key1    b    2    3    2    key1    C    3    4    5
key1    b    2    3    2    key1    D    4    1    6
key1    c    3    2    1    key1    D    4    1    6
key1    d    4    7    2            0    0    \N
key1    e    5    5    5            0    0    \N
key2    a2    1    1    1            0    0    \N
key4    f    2    3    4            0    0    \N
```

<div id="null-values-in-join-keys">
  ## قيم `NULL` في مفاتيح `JOIN`
</div>

لا تساوي `NULL` أي قيمة، بما في ذلك نفسها. وهذا يعني أنه إذا كان مفتاح `JOIN` يحتوي على قيمة `NULL` في أحد الجدولين، فلن يطابق قيمة `NULL` في الجدول الآخر.

**مثال**

الجدول `A`:

```response
┌───id─┬─name────┐
│    1 │ Alice   │
│    2 │ Bob     │
│ ᴺᵁᴸᴸ │ Charlie │
└──────┴─────────┘
```

الجدول `B`:

```response
┌───id─┬─score─┐
│    1 │    90 │
│    3 │    85 │
│ ᴺᵁᴸᴸ │    88 │
└──────┴───────┘
```

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON A.id = B.id
```

```response
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │     0 │
└─────────┴───────┘
```

لاحظ أن الصف الذي يحتوي على `Charlie` من الجدول `A`، والصف الذي درجته 88 من الجدول `B`، غير موجودين في النتيجة بسبب القيمة `NULL` في مفتاح `JOIN`.

إذا كنت تريد مطابقة قيم `NULL`، فاستخدم الدالة `isNotDistinctFrom` لمقارنة مفاتيح `JOIN`.

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```markdown
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

<div id="asof-join-usage">
  ## استخدام ASOF JOIN
</div>

يكون `ASOF JOIN` مفيدًا عندما تحتاج إلى ربط سجلات لا يتوفر بينها تطابق تام.

تتطلب خوارزمية `JOIN` هذه عمودًا خاصًا في الجداول. ويجب أن يكون هذا العمود:

* أن يحتوي على تسلسل مرتب.
* من أحد الأنواع التالية: [Int, UInt](../../../sql-reference/data-types/int-uint.md)، [Float](../../../sql-reference/data-types/float.md)، [Date](../../../sql-reference/data-types/date.md)، [DateTime](../../../sql-reference/data-types/datetime.md)، [Decimal](../../../sql-reference/data-types/decimal.md).
* بالنسبة إلى خوارزمية الربط `hash`، لا يمكن أن يكون هو العمود الوحيد في عبارة `JOIN`.

الصياغة `ASOF JOIN ... ON`:

```sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

يمكنك استخدام أي عدد من شروط المساواة، وشرطًا واحدًا فقط لأقرب تطابق. على سبيل المثال، `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

الشروط المدعومة لأقرب تطابق: `>`, `>=`, `<`, `<=`.

الصيغة `ASOF JOIN ... USING`:

```sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

يستخدم `ASOF JOIN` العمود `equi_columnX` للربط على أساس المساواة، والعمود `asof_column` للربط بأقرب قيمة مطابقة وفق الشرط `table_1.asof_column >= table_2.asof_column`. ويكون العمود `asof_column` دائمًا الأخير في بند `USING`.

على سبيل المثال، انظر إلى الجداول التالية:

```text
         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...
```

يمكن لـ `ASOF JOIN` أخذ الطابع الزمني لحدث مستخدم من `table_1` والعثور على حدث في `table_2` يكون طابعه الزمني هو الأقرب إلى الطابع الزمني للحدث من `table_1` وفقًا لشرط أقرب تطابق. وتُعد قيم الطابع الزمني المتساوية الأقرب إذا كانت متاحة. هنا، يمكن استخدام العمود `user_id` للربط على أساس المساواة، ويمكن استخدام العمود `ev_time` للربط على أساس أقرب تطابق. في مثالنا، يمكن ربط `event_1_1` مع `event_2_1`، ويمكن ربط `event_1_2` مع `event_2_3`، لكن لا يمكن ربط `event_2_2`.

:::note
لا يدعم `ASOF JOIN` إلا خوارزميتي JOIN ‏`hash` و`full_sorting_merge`.
كما أنه **غير** مدعوم في محرك الجداول [Join](../../../engines/table-engines/special/join.md).
:::

<div id="paste-join-usage">
  ## استخدام `PASTE JOIN`
</div>

نتيجة `PASTE JOIN` هي جدول يضم جميع الأعمدة من الاستعلام الفرعي الأيسر، تليها جميع الأعمدة من الاستعلام الفرعي الأيمن.
تُطابَق الصفوف استنادًا إلى مواضعها في الجداول الأصلية (يجب أن يكون ترتيب الصفوف محددًا).
إذا أعادت الاستعلامات الفرعية عددًا مختلفًا من الصفوف، فستُحذَف الصفوف الزائدة.

مثال:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers(2)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(2)
    ORDER BY a DESC
) AS t2

┌─a─┬─t2.a─┐
│ 0 │    1 │
│ 1 │    0 │
└───┴──────┘
```

ملاحظة: في هذه الحالة، قد تكون النتيجة غير حتمية إذا جرت القراءة بالتوازي. على سبيل المثال:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers_mt(5)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(10)
    ORDER BY a DESC
) AS t2
SETTINGS max_block_size = 2;

┌─a─┬─t2.a─┐
│ 2 │    9 │
│ 3 │    8 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 0 │    7 │
│ 1 │    6 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 4 │    5 │
└───┴──────┘
```

<div id="distributed-join">
  ## JOIN الموزّع
</div>

توجد طريقتان لتنفيذ `JOIN` يتضمن جداول موزعة:

* عند استخدام `JOIN` عادي، يُرسَل الاستعلام إلى الخوادم البعيدة. وتُنفَّذ الاستعلامات الفرعية على كل خادم منها لتكوين الجدول الأيمن، ثم تُجرى عملية الربط باستخدام هذا الجدول. وبعبارة أخرى، يُنشأ الجدول الأيمن بشكل مستقل على كل خادم.
* عند استخدام `GLOBAL ... JOIN`، ينفّذ الخادم المُرسِل للطلب أولًا استعلامًا فرعيًا لحساب أحد جانبَي الربط، ثم يجمع النتيجة في جدول مؤقت. بعد ذلك، يُمرَّر هذا الجدول المؤقت إلى كل خادم بعيد، وتُنفَّذ الاستعلامات عليها باستخدام البيانات المؤقتة المنقولة. في حالتي `LEFT` و`INNER` JOIN، يُحتسَب الجدول الأيمن من خلال الاستعلام الفرعي. أما في `RIGHT` JOIN، فيُحتسَب الجدول الأيسر بدلًا من ذلك، لأن الجدول الأيمن هو الذي يجري الاحتفاظ به ويجب قراءته من الأجزاء الموزعة.

توخَّ الحذر عند استخدام `GLOBAL`. لمزيد من المعلومات، راجع قسم [الاستعلامات الفرعية الموزعة](/ar/sql-reference/operators/in#distributed-subqueries).

<div id="implicit-type-conversion">
  ## التحويل الضمني للنوع
</div>

تدعم استعلامات `INNER JOIN` و`LEFT JOIN` و`RIGHT JOIN` و`FULL JOIN` التحويل الضمني للنوع في &quot;مفاتيح الربط&quot;. ومع ذلك، لا يمكن تنفيذ الاستعلام إذا تعذر تحويل مفاتيح الربط من الجدولين الأيسر والأيمن إلى نوع واحد (على سبيل المثال، لا يوجد نوع بيانات يمكنه استيعاب جميع القيم من كلٍّ من `UInt64` و`Int64`، أو `String` و`Int32`).

**مثال**

لنأخذ الجدول `t_1`:

```response
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```

والجدول `t_2`:

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int16         │ Nullable(Int64) │
│  1 │   -1 │ Int16         │ Nullable(Int64) │
│  1 │    1 │ Int16         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

الاستعلام

```sql
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b);
```

تعيد المجموعة:

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

<div id="usage-recommendations">
  ## توصيات للاستخدام
</div>

<div id="processing-of-empty-or-null-cells">
  ### معالجة الخلايا الفارغة أو NULL
</div>

أثناء ضم الجداول، قد تظهر خلايا فارغة. يحدّد الإعداد [join&#95;use&#95;nulls](../../../operations/settings/settings.md#join_use_nulls) كيفية ملء ClickHouse لهذه الخلايا.

إذا كانت مفاتيح `JOIN` حقولًا من النوع [Nullable](../../../sql-reference/data-types/nullable.md)، فلن تُضم الصفوف التي تكون قيمة مفتاح واحد على الأقل فيها [NULL](/ar/sql-reference/syntax#null).

<div id="syntax">
  ### الصياغة
</div>

يجب أن تحمل الأعمدة المحددة في `USING` الأسماء نفسها في كلا الاستعلامين الفرعيين، بينما يجب أن تختلف أسماء الأعمدة الأخرى. يمكنك استخدام الأسماء المستعارة لتغيير أسماء الأعمدة في الاستعلامات الفرعية.

تحدّد عبارة `USING` عمودًا واحدًا أو أكثر للربط، ما يعني مساواة هذه الأعمدة. تُحدَّد قائمة الأعمدة من دون أقواس. شروط الربط الأكثر تعقيدًا غير مدعومة.

<div id="syntax-limitations">
  ### قيود الصياغة
</div>

بالنسبة إلى عبارات `JOIN` المتعددة ضمن استعلام `SELECT` واحد:

* لا يكون جلب جميع الأعمدة باستخدام `*` متاحًا إلا عند الربط بين جداول، وليس بين استعلامات فرعية.
* العبارة `PREWHERE` غير متاحة.
* العبارة `USING` غير متاحة.

بالنسبة إلى العبارات `ON` و`WHERE` و`GROUP BY`:

* لا يمكن استخدام تعبيرات عشوائية في العبارات `ON` و`WHERE` و`GROUP BY`، لكن يمكنك تعريف تعبير في عبارة `SELECT` ثم استخدامه في هذه العبارات عبر اسم مستعار.

<div id="performance">
  ### الأداء
</div>

عند تنفيذ `JOIN`، لا يوجد أي تحسين لترتيب التنفيذ بالنسبة إلى المراحل الأخرى من الاستعلام. إذ يُنفَّذ الربط (أي البحث في الجدول الأيمن) قبل التصفية في `WHERE` وقبل التجميع.

في كل مرة يُنفَّذ فيها استعلام باستخدام `JOIN` نفسه، يُعاد تنفيذ الاستعلام الفرعي لأن النتيجة غير مخزنة مؤقتًا. لتجنّب ذلك، استخدم محرك الجداول الخاص [Join](../../../engines/table-engines/special/join.md)، وهو مصفوفة مُعدّة مسبقًا لعمليات الربط وتبقى دائمًا في RAM.

في بعض الحالات، يكون استخدام [IN](../../../sql-reference/operators/in.md) أكثر كفاءة من `JOIN`.

إذا كنت بحاجة إلى `JOIN` للربط مع جداول الأبعاد (وهي جداول صغيرة نسبيًا تحتوي على خصائص الأبعاد، مثل أسماء الحملات الإعلانية)، فقد لا يكون `JOIN` الخيار الأنسب، لأن الجدول الأيمن يُعاد الوصول إليه مع كل استعلام. في مثل هذه الحالات، تتوفر ميزة &quot;Dictionaries&quot; التي ينبغي استخدامها بدلًا من `JOIN`. لمزيد من المعلومات، راجع قسم [Dictionaries](/ar/sql-reference/statements/create/dictionary/overview.md).

<div id="memory-limitations">
  ### قيود الذاكرة
</div>

يستخدم ClickHouse افتراضيًا خوارزمية [الربط بالتجزئة](https://en.wikipedia.org/wiki/Hash_join). يأخذ ClickHouse الجدول `right_table` وينشئ له hash table في RAM. إذا كان `join_algorithm = 'auto'` مُمكّنًا، فبعد تجاوز حدّ معيّن من استهلاك الذاكرة، ينتقل ClickHouse إلى خوارزمية [merge](https://en.wikipedia.org/wiki/Sort-merge_join) join. للاطلاع على وصف خوارزميات `JOIN`، راجع إعداد [join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm).

إذا كنت بحاجة إلى تقييد استهلاك الذاكرة لعملية `JOIN`، فاستخدم الإعدادات التالية:

* [max&#95;rows&#95;in&#95;join](/ar/operations/settings/settings#max_rows_in_join) — يحدّ من عدد الصفوف في hash table.
* [max&#95;bytes&#95;in&#95;join](/ar/operations/settings/settings#max_bytes_in_join) — يحدّ من حجم hash table.

عند بلوغ أيّ من هذه الحدود، يتصرف ClickHouse وفقًا لما يحدده إعداد [join&#95;overflow&#95;mode](/ar/operations/settings/settings#join_overflow_mode).

<div id="examples">
  ## أمثلة
</div>

مثال:

```sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

```text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [ClickHouse: نظام إدارة قواعد بيانات فائق السرعة مع دعم كامل لعمليات JOIN في SQL - الجزء 1](https://clickhouse.com/blog/clickhouse-fully-supports-joins)
* مدونة: [ClickHouse: نظام إدارة قواعد بيانات فائق السرعة مع دعم كامل لعمليات JOIN في SQL - من الداخل - الجزء 2](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)
* مدونة: [ClickHouse: نظام إدارة قواعد بيانات فائق السرعة مع دعم كامل لعمليات JOIN في SQL - من الداخل - الجزء 3](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3)
* مدونة: [ClickHouse: نظام إدارة قواعد بيانات فائق السرعة مع دعم كامل لعمليات JOIN في SQL - من الداخل - الجزء 4](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)