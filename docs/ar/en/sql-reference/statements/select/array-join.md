---
description: 'توثيق لعبارة ARRAY JOIN'
sidebar_label: 'ARRAY JOIN'
slug: /sql-reference/statements/select/array-join
title: 'عبارة ARRAY JOIN'
doc_type: 'مرجع'
---

من العمليات الشائعة في الجداول التي تحتوي على عمود مصفوفة إنشاء جدول جديد يضم صفًا لكل عنصر على حدة من عناصر المصفوفة في ذلك العمود الأصلي، مع تكرار قيم الأعمدة الأخرى. وهذه هي الحالة الأساسية لما تفعله العبارة `ARRAY JOIN`.

وتأتي هذه التسمية من أنه يمكن النظر إليها على أنها تنفيذ `JOIN` مع مصفوفة أو بنية بيانات متداخلة. والغرض منها مشابه للدالة [arrayJoin](/ar/sql-reference/functions/array-join)، لكن إمكانات العبارة أوسع.

الصياغة:

```sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

الأنواع المدعومة من `ARRAY JOIN` مُدرجة أدناه:

* `ARRAY JOIN` - في الحالة الأساسية، لا تُضمَّن المصفوفات الفارغة في نتيجة `JOIN`.
* `LEFT ARRAY JOIN` - تحتوي نتيجة `JOIN` على صفوف ذات مصفوفات فارغة. وتُعيَّن قيمة المصفوفة الفارغة إلى القيمة الافتراضية لنوع عنصر المصفوفة (عادةً 0 أو سلسلة فارغة أو NULL).

<div id="basic-array-join-examples">
  ## أمثلة أساسية على ARRAY JOIN
</div>

<div id="array-join-left-array-join-examples">
  ### ARRAY JOIN و LEFT ARRAY JOIN
</div>

توضح الأمثلة أدناه كيفية استخدام البندين `ARRAY JOIN` و `LEFT ARRAY JOIN`. فلننشئ جدولًا يحتوي على عمود من النوع [Array](../../../sql-reference/data-types/array.md) ثم نُدرج فيه قيمًا:

```sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

```response
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

يستخدم المثال أدناه عبارة `ARRAY JOIN`:

```sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

```response
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

يستخدم المثال التالي عبارة `LEFT ARRAY JOIN`:

```sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

```response
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

<div id="array-join-arrayEnumerate">
  ### ARRAY JOIN ودالة arrayEnumerate
</div>

تُستخدم هذه الدالة عادةً مع `ARRAY JOIN`. وتتيح احتساب شيءٍ معيّن مرة واحدة فقط لكل مصفوفة بعد تطبيق `ARRAY JOIN`. مثال:

```sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

في هذا المثال، يمثّل Reaches عدد التحويلات (السلاسل الناتجة بعد تطبيق `ARRAY JOIN`)، ويمثّل Hits عدد مشاهدات الصفحة (السلاسل قبل `ARRAY JOIN`). وفي هذه الحالة تحديدًا، يمكنك الحصول على النتيجة نفسها بطريقة أسهل:

```sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

<div id="array_join_arrayEnumerateUniq">
  ### ARRAY JOIN و arrayEnumerateUniq
</div>

تكون هذه الدالة مفيدة عند استخدام `ARRAY JOIN` وتجميع عناصر المصفوفة.

في هذا المثال، يتضمن كل معرّف هدف احتساب عدد التحويلات (إذ إن كل عنصر في بنية البيانات المتداخلة Goals هو هدف تم تحقيقه، ونشير إليه على أنه تحويل) وعدد الجلسات. ومن دون `ARRAY JOIN`، كنّا سنحسب عدد الجلسات على أنه sum(Sign). ولكن في هذه الحالة تحديدًا، تضاعفت الصفوف بسبب البنية المتداخلة Goals، لذلك، وحتى نحتسب كل جلسة مرة واحدة فقط بعد ذلك، نطبّق شرطًا على قيمة الدالة `arrayEnumerateUniq(Goals.ID)`.

```sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

```text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

<div id="using-aliases">
  ## استخدام الأسماء المستعارة
</div>

يمكن تعيين اسم مستعار لمصفوفة ضمن عبارة `ARRAY JOIN`. وفي هذه الحالة، يمكن الوصول إلى عنصر من عناصر المصفوفة عبر هذا الاسم المستعار، بينما يُستخدَم الاسم الأصلي للوصول إلى المصفوفة نفسها. مثال:

```sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

```response
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

باستخدام الأسماء المستعارة، يمكنك تنفيذ `ARRAY JOIN` بمصفوفة خارجية. على سبيل المثال:

```sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

```response
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

يمكن أن تُفصل عدة مصفوفات بفواصل في عبارة `ARRAY JOIN`. في هذه الحالة، يُجرى `JOIN` عليها جميعًا في الوقت نفسه (المجموع المباشر، وليس الجداء الديكارتي). لاحظ أن جميع المصفوفات يجب أن تكون بالحجم نفسه افتراضيًا. مثال:

```sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

يستخدم المثال التالي الدالة [arrayEnumerate](/ar/sql-reference/functions/array-functions#arrayEnumerate):

```sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

يمكن ضمّ عدة مصفوفات بأحجام مختلفة باستخدام: `SETTINGS enable_unaligned_array_join = 1`. مثال:

```sql
SELECT s, arr, a, b
FROM arrays_test ARRAY JOIN arr AS a, [['a','b'],['c']] AS b
SETTINGS enable_unaligned_array_join = 1;
```

```response
┌─s───────┬─arr─────┬─a─┬─b─────────┐
│ Hello   │ [1,2]   │ 1 │ ['a','b'] │
│ Hello   │ [1,2]   │ 2 │ ['c']     │
│ World   │ [3,4,5] │ 3 │ ['a','b'] │
│ World   │ [3,4,5] │ 4 │ ['c']     │
│ World   │ [3,4,5] │ 5 │ []        │
│ Goodbye │ []      │ 0 │ ['a','b'] │
│ Goodbye │ []      │ 0 │ ['c']     │
└─────────┴─────────┴───┴───────────┘
```

<div id="array-join-with-nested-data-structure">
  ## ARRAY JOIN مع بنية بيانات متداخلة
</div>

يعمل `ARRAY JOIN` أيضًا مع [بُنى البيانات المتداخلة](../../../sql-reference/data-types/nested-data-structures/index.md):

```sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

```response
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

عند تحديد أسماء بُنى البيانات المتداخلة في `ARRAY JOIN`، يكون المعنى مماثلًا لتنفيذ `ARRAY JOIN` على جميع عناصر المصفوفة التي تتألف منها. ترد الأمثلة أدناه:

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

وهذا الشكل منطقي أيضًا:

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

يمكن استخدام اسم مستعار لبنية بيانات متداخلة لاختيار نتيجة `JOIN` أو المصفوفة الأصلية. مثال:

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

مثال لاستخدام الدالة [arrayEnumerate](/ar/sql-reference/functions/array-functions#arrayEnumerate):

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

يُحسَّن ترتيب تنفيذ الاستعلام عند استخدام `ARRAY JOIN`. وعلى الرغم من أنه يجب دائمًا تحديد `ARRAY JOIN` قبل عبارة [WHERE](../../../sql-reference/statements/select/where.md)/[PREWHERE](../../../sql-reference/statements/select/prewhere.md) في الاستعلام، فمن الناحية التقنية يمكن تنفيذهما بأي ترتيب، ما لم تُستخدم نتيجة `ARRAY JOIN` في التصفية. ويتحكم مُحسِّن الاستعلامات في ترتيب المعالجة.

<div id="incompatibility-with-short-circuit-function-evaluation">
  ### عدم التوافق مع التقييم المختصر للدوال
</div>

تُعد [آلية التقييم المختصر للدوال](/ar/operations/settings/settings#short_circuit_function_evaluation) ميزةً تُحسِّن تنفيذ التعبيرات المعقدة في دوال محددة مثل `if` و`multiIf` و`and` و`or`. وهي تمنع حدوث استثناءات محتملة، مثل القسمة على صفر، أثناء تنفيذ هذه الدوال.

يُنفَّذ `arrayJoin` دائمًا، ولا يدعم التقييم المختصر للدوال. ويرجع ذلك إلى أنه دالة فريدة تُعالَج بشكل منفصل عن جميع الدوال الأخرى أثناء تحليل الاستعلام وتنفيذه، كما أنها تتطلب منطقًا إضافيًا لا يتوافق مع التقييم المختصر للدوال. والسبب في ذلك هو أن عدد الصفوف في النتيجة يعتمد على ناتج `arrayJoin`، كما أن تنفيذ `arrayJoin` بأسلوب lazy معقد جدًا ومكلف.

<div id="related-content">
  ## محتوى ذي صلة
</div>

* المدونة: [العمل مع بيانات السلاسل الزمنية في ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)