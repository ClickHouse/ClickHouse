---
description: 'توثيق مُركِّبات الدوال التجميعية'
sidebar_label: 'المُركِّبات'
sidebar_position: 37
slug: /sql-reference/aggregate-functions/combinators
title: 'مُركِّبات الدوال التجميعية'
doc_type: 'reference'
---

يمكن إلحاق لاحقة باسم دالة تجميعية، مما يغيّر طريقة عمل الدالة التجميعية.

<div id="-if">
  ## -If
</div>

يمكن إلحاق اللاحقة -If باسم أي دالة تجميعية. في هذه الحالة، تقبل الدالة التجميعية وسيطًا إضافيًا، وهو شرط (من النوع Uint8). ولا تعالج الدالة التجميعية إلا الصفوف التي يتحقق فيها الشرط. وإذا لم يتحقق الشرط ولو مرة واحدة، فإنها تُرجع قيمة افتراضية (تكون عادةً أصفارًا أو سلاسل فارغة).

أمثلة: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` وما إلى ذلك.

باستخدام الدوال التجميعية الشرطية، يمكنك حساب التجميعات لعدة شروط في وقت واحد، من دون استخدام استعلامات فرعية وعمليات `JOIN`. على سبيل المثال، يمكن استخدام الدوال التجميعية الشرطية لتنفيذ ميزة مقارنة الشرائح.

<div id="-array">
  ## -Array
</div>

يمكن إلحاق اللاحقة -Array بأي دالة تجميعية. في هذه الحالة، تأخذ دالة تجميعية معاملات من النوع &#39;Array(T)&#39; (مصفوفات) بدلًا من معاملات من النوع &#39;T&#39;. وإذا كانت دالة تجميعية تقبل عدة معاملات، فيجب أن تكون كلها مصفوفات ذات أطوال متساوية. وعند معالجة المصفوفات، تعمل دالة تجميعية كما تعمل دالة تجميعية الأصلية على جميع عناصر المصفوفات.

مثال 1: `sumArray(arr)` - يحسب مجموع جميع عناصر كل مصفوفات &#39;arr&#39;. في هذا المثال، كان يمكن كتابة ذلك بصورة أبسط: `sum(arraySum(arr))`.

مثال 2: `uniqArray(arr)` – يحسب عدد العناصر الفريدة في جميع مصفوفات &#39;arr&#39;. ويمكن تنفيذ ذلك بطريقة أسهل: `uniq(arrayJoin(arr))`، لكن لا يكون من الممكن دائمًا إضافة &#39;arrayJoin&#39; إلى استعلام.

يمكن دمج ‎-If و‎ -Array. ومع ذلك، يجب أن تأتي &#39;Array&#39; أولًا ثم &#39;If&#39;. أمثلة: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. وبسبب هذا الترتيب، لن يكون المعامل &#39;cond&#39; مصفوفة.

<div id="-map">
  ## -Map
</div>

يمكن إلحاق اللاحقة ‎-Map‎ بأي دالة تجميعية. ويؤدي ذلك إلى إنشاء دالة تجميعية تأخذ Map type كـ argument، وتُجمِّع values لكل key في الـ map على حدة باستخدام دالة تجميعية المحددة. وتكون النتيجة أيضًا من نوع Map type.

**مثال**

```sql
CREATE TABLE map_map(
    date Date,
    timeslot DateTime,
    status Map(String, UInt64)
) ENGINE = MergeTree
ORDER BY ();

INSERT INTO map_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', (['a', 'b', 'c'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', (['c', 'd', 'e'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['d', 'e', 'f'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['f', 'g', 'g'], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(status),
    avgMap(status),
    minMap(status)
FROM map_map
GROUP BY timeslot;

┌────────────timeslot─┬─sumMap(status)───────────────────────┬─avgMap(status)───────────────────────┬─minMap(status)───────────────────────┐
│ 2000-01-01 00:00:00 │ {'a':10,'b':10,'c':20,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │
│ 2000-01-01 00:01:00 │ {'d':10,'e':10,'f':20,'g':20}        │ {'d':10,'e':10,'f':10,'g':10}        │ {'d':10,'e':10,'f':10,'g':10}        │
└─────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┘
```

<div id="-simplestate">
  ## -SimpleState
</div>

إذا طبّقت هذا المُركِّب، فستُرجِع الدالة التجميعية القيمة نفسها ولكن بنوع مختلف. ويكون هذا على هيئة [SimpleAggregateFunction(...)](../../sql-reference/data-types/simpleaggregatefunction.md) يمكن تخزينها في جدول للعمل مع جداول [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).

**البنية**

```sql
<aggFunction>SimpleState(x)
```

**الوسائط**

* `x` — معلمات الدالة التجميعية.

**القيم المُعادة**

قيمة دالة تجميعية من النوع `SimpleAggregateFunction(...)`.

**مثال**

```sql title="Query"
WITH anySimpleState(number) AS c SELECT toTypeName(c), c FROM numbers(1);
```

```text title="Response"
┌─toTypeName(c)────────────────────────┬─c─┐
│ SimpleAggregateFunction(any, UInt64) │ 0 │
└──────────────────────────────────────┴───┘
```

<div id="-state">
  ## -State
</div>

إذا طبّقت هذا المُركِّب، فلن تُرجِع الدالة التجميعية القيمة النهائية (مثل عدد القيم الفريدة للدالة [uniq](/ar/sql-reference/aggregate-functions/reference/uniq))، بل حالةً وسيطة من حالات التجميع (بالنسبة إلى `uniq`، تكون هذه الحالة هي جدول hash المستخدم لحساب عدد القيم الفريدة). ويكون ذلك من النوع `AggregateFunction(...)`، ويمكن استخدامه لمزيد من المعالجة أو تخزينه في جدول لإكمال التجميع لاحقًا.

:::note
يرجى ملاحظة أن -MapState ليس ثابتًا للبيانات نفسها، لأن ترتيب البيانات في الحالة الوسيطة قد يتغيّر، رغم أن ذلك لا يؤثر في إدخال هذه البيانات.
:::

للعمل مع هذه الحالات، استخدم:

* محرك الجدول [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).
* الدالة [finalizeAggregation](/ar/sql-reference/functions/other-functions#finalizeAggregation).
* الدالة [runningAccumulate](../../sql-reference/functions/other-functions.md#runningAccumulate).
* المُركِّب [-Merge](#-merge).
* المُركِّب [-MergeState](#-mergestate).

<div id="-merge">
  ## -Merge
</div>

إذا طبّقت هذا المُركِّب، فإن دالة تجميعية تأخذ حالة التجميع الوسيطة وسيطةً، وتدمج الحالات لإتمام التجميع، وتُرجِع القيمة الناتجة.

<div id="-mergestate">
  ## -MergeState
</div>

يدمج حالات التجميع الوسيطة بالطريقة نفسها التي يعمل بها المُركِّب -Merge. لكنه لا يعيد القيمة الناتجة، بل يعيد حالة تجميع وسيطة، على غرار المُركِّب -State.

<div id="-foreach">
  ## -ForEach
</div>

يحوّل دالةً تجميعية للجداول إلى دالة تجميعية للمصفوفات، بحيث تجمع العناصر المتناظرة في المصفوفات وتُرجع مصفوفة من النتائج. على سبيل المثال، تُرجع `sumForEach` للمصفوفات `[1, 2]`، `[3, 4, 5]` و`[6, 7]` النتيجة `[10, 13, 5]` بعد جمع العناصر المتناظرة معًا.

<div id="-tuple">
  ## -Tuple
</div>

يمكن إلحاق اللاحقة `-Tuple` بأي دالة تجميع. تأخذ الدالة الناتجة وسيطًا واحدًا من النوع `Tuple` مقابل كل وسيط في دالة التجميع الأساسية؛ ويجب أن تحتوي جميع قيم `Tuple` على العدد نفسه من العناصر. ويُطبَّق التجميع بشكل مستقل على كل موضع من مواضع العناصر، بحيث يستقبل العنصر المقابل من كل `Tuple` ويُرجع `Tuple` من النتائج.

إذا كانت قيمة `Tuple` الأولى المُدخلة تحتوي على أسماء عناصر صريحة، فستُحفَظ هذه الأسماء في النتيجة.

دوال التجميع التي تتعامل مع قيم `NULL` بنفسها (`anyRespectNulls` و`anyLastRespectNulls` والمُعدِّل `RESPECT NULLS`) لا تدعم النوع `Nullable(Tuple(...))` كوسيط؛ استخدم عناصر `Nullable` بدلًا من ذلك.

**البنية**

```sql
<aggFunction>Tuple(tuple1[, tuple2, ...])
```

**الوسائط**

* `tuple1[, tuple2, ...]` — أعمدة من النوع `Tuple`، بعمود واحد لكل وسيطة من وسيطات دالة التجميع الأساسية، على أن يكون لها جميعًا العدد نفسه من العناصر. يجب أن يكون كل عنصر من نوع تدعمه دالة التجميع الأساسية في موضع تلك الوسيطة.

**القيم المُعادة**

* `Tuple` يحتوي على نتيجة تطبيق دالة التجميع على كل عنصر على حدة.

النوع: `Tuple(aggFunction(element1), aggFunction(element2), ...)`.

**مثال**

الاستعلام:

```sql
SELECT sumTuple(t) FROM
(
    SELECT tuple(toInt64(1), toFloat64(2.5)) AS t
    UNION ALL
    SELECT tuple(toInt64(3), toFloat64(4.5))
    UNION ALL
    SELECT tuple(toInt64(5), toFloat64(6.5))
);
```

النتيجة:

```text
┌─sumTuple(t)─┐
│ (9,13.5)    │
└─────────────┘
```

عند الاستخدام مع `GROUP BY`:

```sql
SELECT
    k,
    avgTuple(t)
FROM
(
    SELECT
        number % 2 AS k,
        tuple(toInt64(number), toFloat64(number) * 1.5) AS t
    FROM numbers(6)
)
GROUP BY k
ORDER BY k;
```

```text
┌─k─┬─avgTuple(t)─┐
│ 0 │ (2,3)       │
│ 1 │ (3,4.5)     │
└───┴─────────────┘
```

عند الاستخدام مع دالة تجميع متعددة الوسائط: يوفّر كل وسيط `Tuple` وسيطًا واحدًا للدالة الأساسية، وتُطابَق العناصر بحسب مواضعها:

```text
corrTuple((a1, a2), (b1, b2)) = (corr(a1, b1), corr(a2, b2))
```

```sql
SELECT corrTuple((a1, a2), (b1, b2))
FROM
(
    SELECT
        toFloat64(number) AS a1,
        toFloat64(number * 2) AS a2,
        toFloat64(100 - number) AS b1,
        toFloat64(number * 3) AS b2
    FROM numbers(10)
);
```

```text
┌─corrTuple((a1, a2), (b1, b2))─┐
│ (-1,1)                        │
└───────────────────────────────┘
```

`a1` و `b1` مرتبطان ارتباطًا عكسيًا، بينما `a2` و `b2` يتناسبان طرديًا، لذا تكون النتيجة `(-1, 1)`.

يمكن دمج `-Tuple` مع مُركِّبات أخرى مثل `-If`. مثال: `sumTupleIf(tuple_column, cond)`.

<div id="-distinct">
  ## -Distinct
</div>

لن يُحتسب كل تركيب فريد من المعاملات ضمن التجميع إلا مرة واحدة فقط. تُتجاهل القيم المتكررة.
أمثلة: `sum(DISTINCT x)` (أو `sumDistinct(x)`)، و`groupArray(DISTINCT x)` (أو `groupArrayDistinct(x)`)، و`corrStable(DISTINCT x, y)` (أو `corrStableDistinct(x, y)`) وما إلى ذلك.

<div id="-ordefault">
  ## -OrDefault
</div>

يُغيّر سلوك الدالة التجميعية.

إذا لم تكن للدالة التجميعية قيم إدخال، فإن هذا المُركِّب يجعلها تُرجع القيمة الافتراضية لنوع بيانات الإرجاع الخاص بها. وينطبق ذلك على الدوال التجميعية التي يمكنها قبول بيانات إدخال فارغة.

يمكن استخدام `-OrDefault` مع مُركِّبات أخرى.

**البنية**

```sql
<aggFunction>OrDefault(x)
```

**الوسائط**

* `x` — معلمات الدالة التجميعية.

**القيم المعادة**

تعيد القيمة الافتراضية لنوع الإرجاع الخاص بالدالة التجميعية إذا لم يكن هناك ما يمكن تجميعه.

يعتمد النوع على الدالة التجميعية المستخدمة.

**مثال**

```sql title="Query"
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

```text title="Response"
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

يمكن أيضًا استخدام `-OrDefault` مع مُركِّبات أخرى. ويفيد ذلك عندما لا تقبل دالة التجميع إدخالًا فارغًا.

```sql title="Query"
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

<div id="-ornull">
  ## -OrNull
</div>

يغيّر سلوك دالة تجميعية.

يحوّل هذا المُركِّب ناتج الدالة التجميعية إلى نوع البيانات [Nullable](../../sql-reference/data-types/nullable.md). وإذا لم تكن هناك قيم لتجميعها، فإن الدالة تُرجع [NULL](/ar/operations/settings/formats#input_format_null_as_default).

يمكن استخدام `-OrNull` مع مُركِّبات أخرى.

**الصيغة**

```sql
<aggFunction>OrNull(x)
```

**الوسيطات**

* `x` — معلمات الدالة التجميعية.

**القيم المُعادة**

* نتيجة الدالة التجميعية، محوّلة إلى نوع البيانات `Nullable`.
* `NULL`، إذا لم توجد قيم لتجميعها.

النوع: `Nullable(aggregate function return type)`.

**مثال**

أضف `-orNull` إلى نهاية اسم الدالة التجميعية.

```sql title="Query"
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

```text title="Response"
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

يمكن أيضًا استخدام `-OrNull` مع مُركِّبات أخرى. ويكون ذلك مفيدًا عندما لا تقبل الدالة التجميعية إدخالًا فارغًا.

```sql title="Query"
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

<div id="-resample">
  ## -Resample
</div>

يتيح لك تقسيم البيانات إلى مجموعات، ثم تجميع البيانات في كل مجموعة على حدة. تُنشأ هذه المجموعات بتقسيم قيم أحد الأعمدة إلى فترات.

```sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**الوسائط**

* `start` — قيمة بداية النطاق الزمني المطلوب بالكامل لقيم `resampling_key`.
* `stop` — قيمة نهاية النطاق الزمني المطلوب بالكامل لقيم `resampling_key`. لا يتضمن النطاق الكامل قيمة `stop` ‏`[start, stop)`.
* `step` — الخطوة المستخدمة لتقسيم النطاق الكامل إلى نطاقات فرعية. يُنفَّذ `aggFunction` على كلٍّ من هذه النطاقات الفرعية بشكل مستقل.
* `resampling_key` — العمود الذي تُستخدَم قيمه لتقسيم البيانات إلى نطاقات.
* `aggFunction_params` — معاملات `aggFunction`.

**القيم المُعادة**

* مصفوفة من نتائج `aggFunction` لكل نطاق فرعي.

**مثال**

لنفترض وجود الجدول `people` بالبيانات التالية:

```text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

دعونا نستخرج أسماء الأشخاص الذين تقع أعمارهم ضمن النطاقين `[30,60)` و`[60,75)`. وبما أننا نستخدم تمثيل العمر بالأعداد الصحيحة، فستكون الأعمار ضمن النطاقين `[30, 59]` و`[60,74]`.

لتجميع الأسماء في مصفوفة، نستخدم الدالة التجميعية [groupArray](/ar/sql-reference/aggregate-functions/reference/grouparray). وهي تأخذ وسيطة واحدة. في حالتنا، هذه هي العمود `name`. ويجب أن تستخدم الدالة `groupArrayResample` العمود `age` لتجميع الأسماء بحسب العمر. ولتحديد النطاقات المطلوبة، نمرّر الوسائط `30, 75, 30` إلى الدالة `groupArrayResample`.

```sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

```text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

انظر إلى النتائج.

استُبعد `John` من العينة لأنه صغير السن جدًا. أمّا الأشخاص الآخرون فتوزّعوا وفقًا للفئات العمرية المحددة.

والآن لنحسب العدد الإجمالي للأشخاص ومتوسط أجورهم ضمن الفئات العمرية المحددة.

```sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

```text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

<div id="-argmin">
  ## -ArgMin
</div>

يمكن إلحاق اللاحقة -ArgMin باسم أي دالة تجميعية. في هذه الحالة، تقبل الدالة التجميعية وسيطًا إضافيًا، ويجب أن يكون هذا الوسيط تعبيرًا قابلاً للمقارنة. ولا تعالج الدالة التجميعية إلا الصفوف التي لها أدنى قيمة للتعبير الإضافي المحدد.

أمثلة: `sumArgMin(column, expr)` و`countArgMin(expr)` و`avgArgMin(x, expr)` وما إلى ذلك.

<div id="-argmax">
  ## -ArgMax
</div>

مشابه لللاحقة -ArgMin، لكنه يعالج فقط الصفوف ذات القيمة القصوى للتعبير الإضافي المحدد.

<div id="related-content">
  ## محتوى ذي صلة
</div>

* مدونة: [استخدام مُركِّبات الدوال التجميعية في ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)