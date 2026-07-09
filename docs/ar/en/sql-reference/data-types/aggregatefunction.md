---
description: 'توثيق نوع البيانات AggregateFunction في ClickHouse، الذي
يخزّن الحالات الوسيطة للدوال التجميعية'
keywords: ['AggregateFunction', 'النوع']
sidebar_label: 'AggregateFunction'
sidebar_position: 46
slug: /sql-reference/data-types/aggregatefunction
title: 'نوع AggregateFunction'
doc_type: 'reference'
---

<div id="description">
  ## الوصف
</div>

تحتوي جميع [الدوال التجميعية](/ar/sql-reference/aggregate-functions) في ClickHouse على
حالة وسيطة خاصة بالتنفيذ يمكن إجراء تسلسل لها إلى
نوع البيانات `AggregateFunction` وتخزينها في جدول. ويُجرى ذلك عادةً
باستخدام [materialized view](../../sql-reference/statements/create/view.md).

هناك مُركِّبان من [combinators](/ar/sql-reference/aggregate-functions/combinators)
يُستخدمان شائعًا مع النوع `AggregateFunction`:

* مُركِّب الدالة التجميعية [`-State`](/ar/sql-reference/aggregate-functions/combinators#-state)، والذي عند إلحاقه باسم دالة تجميعية
  يُنتج حالات وسيطة من `AggregateFunction`.
* مُركِّب الدالة التجميعية [`-Merge`](/ar/sql-reference/aggregate-functions/combinators#-merge)،
  والذي يُستخدم للحصول على النتيجة النهائية لعملية التجميع
  من الحالات الوسيطة.

<div id="syntax">
  ## الصيغة
</div>

```sql
AggregateFunction(aggregate_function_name, types_of_arguments...)
```

**المعلمات**

* `aggregate_function_name` - اسم دالة التجميع. إذا كانت الدالة
  تقبل معلمات، فيجب تحديد هذه المعلمات أيضًا.
* `types_of_arguments` - أنواع وسيطات دالة التجميع.

على سبيل المثال:

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

<div id="usage">
  ## الاستخدام
</div>

<div id="data-insertion">
  ### إدراج البيانات
</div>

لإدراج البيانات في جدول يحتوي على أعمدة من النوع `AggregateFunction`، يمكنك
استخدام `INSERT SELECT` مع الدوال التجميعية ومُركِّب الدوال التجميعية
[`-State`](/ar/sql-reference/aggregate-functions/combinators#-state).

على سبيل المثال، للإدراج في أعمدة من النوع `AggregateFunction(uniq, UInt64)` و
`AggregateFunction(quantiles(0.5, 0.9), UInt64)`، استخدم الدوال التجميعية التالية
مع المُركِّبات.

```sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

على عكس الدالتين `uniq` و`quantiles`، فإن `uniqState` و`quantilesState`
(مع إضافة المُركِّب `-State`) تُرجعان الحالة بدلًا من القيمة النهائية.
وبعبارة أخرى، فهما تُرجعان قيمة من النوع `AggregateFunction`.

في نتائج استعلام `SELECT`، تكون للقيم من النوع `AggregateFunction`
تمثيلات ثنائية خاصة بالتنفيذ في جميع تنسيقات الإخراج في ClickHouse.

يوجد إعداد خاص على مستوى Session باسم `aggregate_function_input_format` يتيح إنشاء الحالة من قيم الإدخال.
وهو يدعم التنسيقات التالية:

* `state` - سلسلة ثنائية تحتوي على الحالة المُسلسلة (الافتراضي).
  إذا قمت بتفريغ البيانات، على سبيل المثال، إلى التنسيق `TabSeparated` باستخدام استعلام `SELECT`،
  فيمكن بعد ذلك إعادة تحميل هذا التفريغ باستخدام استعلام `INSERT`.
* `value` - سيتوقع التنسيق قيمة واحدة لوسيط الدالة التجميعية، أو في حال وجود عدة وسائط، Tuple منها؛ وسيُفك تسلسلها لتكوين الحالة المطلوبة
* `array` - سيتوقع التنسيق Array من القيم، كما هو موضح في خيار `value` أعلاه؛ وستُجمَّع جميع عناصر المصفوفة لتكوين الحالة

<div id="data-selection">
  ### اختيار البيانات
</div>

عند اختيار البيانات من جدول `AggregatingMergeTree`، استخدم عبارة `GROUP BY`
ودوال التجميع نفسها التي استخدمتها عند إدخال البيانات، ولكن مع
المُركِّب [`-Merge`](/ar/sql-reference/aggregate-functions/combinators#-merge).

تأخذ دالة التجميع المُلحَق بها المُركِّب `-Merge` مجموعة من
حالات التجميع، وتدمجها، ثم تُرجع نتيجة التجميع الكاملة للبيانات.

على سبيل المثال، يُرجع الاستعلامان التاليان النتيجة نفسها:

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

<div id="usage-example">
  ## مثال للاستخدام
</div>

راجع وصف محرك [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).

<div id="related-content">
  ## محتوى ذي صلة
</div>

* مدونة: [استخدام مُعدِّلات التجميع في ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* مُركِّب [MergeState](/ar/sql-reference/aggregate-functions/combinators#-mergestate).
* مُركِّب [State](/ar/sql-reference/aggregate-functions/combinators#-state).