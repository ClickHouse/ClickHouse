---
description: 'توثيق لنوع البيانات SimpleAggregateFunction'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'النوع SimpleAggregateFunction'
doc_type: 'reference'
---

<div id="description">
  ## الوصف
</div>

يخزّن نوع البيانات `SimpleAggregateFunction` الحالة الوسيطة لـ
دالة تجميع، لكنه لا يخزّن حالتها الكاملة كما يفعل النوع [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md).

يمكن تطبيق هذا التحسين على الدوال التي تتحقق فيها الخاصية
التالية:

> يمكن الحصول على نتيجة تطبيق الدالة `f` على مجموعة الصفوف `S1 UNION ALL S2`
> عبر تطبيق `f` على أجزاء مجموعة الصفوف كلٌّ على حدة، ثم
> تطبيق `f` مرة أخرى على النتائج: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`.

تضمن هذه الخاصية أن نتائج التجميع الجزئية تكفي لحساب
النتيجة المجمعة، لذا لا نحتاج إلى تخزين أي بيانات إضافية أو معالجتها. على
سبيل المثال، لا تتطلب نتيجتا الدالتين `min` و`max` أي خطوات إضافية
لحساب النتيجة النهائية انطلاقًا من الخطوات الوسيطة، بينما تتطلب الدالة `avg`
الاحتفاظ بمجموع وعدّاد، ثم قسمة الأول على الثاني للحصول على
المتوسط في خطوة `Merge` نهائية تجمع الحالات الوسيطة.

غالبًا ما تُنتَج قيم دوال التجميع عبر استدعاء دالة تجميع
مع إلحاق المُركِّب [`-SimpleState`](/ar/sql-reference/aggregate-functions/combinators#-simplestate) باسم الدالة.

<div id="syntax">
  ## الصيغة
</div>

```sql
SimpleAggregateFunction(aggregate_function_name, types_of_arguments...)
```

**المعلمات**

* `aggregate_function_name` - اسم دالة تجميع.
* `Type` - أنواع وسيطات دالة التجميع.

<div id="supported-functions">
  ## الدوال المدعومة
</div>

دوال التجميع التالية مدعومة:

* [`any`](/ar/sql-reference/aggregate-functions/reference/any.md)
* [`any_respect_nulls`](/ar/sql-reference/aggregate-functions/reference/any.md)
* [`anyLast`](/ar/sql-reference/aggregate-functions/reference/anyLast.md)
* [`anyLast_respect_nulls`](/ar/sql-reference/aggregate-functions/reference/anyLast.md)
* [`min`](/ar/sql-reference/aggregate-functions/reference/min.md)
* [`max`](/ar/sql-reference/aggregate-functions/reference/max.md)
* [`sum`](/ar/sql-reference/aggregate-functions/reference/sum.md)
* [`sumWithOverflow`](/ar/sql-reference/aggregate-functions/reference/sumWithOverflow.md)
* [`groupBitAnd`](/ar/sql-reference/aggregate-functions/reference/groupBitAnd.md)
* [`groupBitOr`](/ar/sql-reference/aggregate-functions/reference/groupBitOr.md)
* [`groupBitXor`](/ar/sql-reference/aggregate-functions/reference/groupBitXor.md)
* [`groupArrayArray`](/ar/sql-reference/aggregate-functions/reference/groupArrayArray.md)
* [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupUniqArray.md)
* [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
* [`sumMap` (`sumMappedArrays`)](/ar/sql-reference/aggregate-functions/reference/sumMappedArrays.md)
* [`minMap` (`minMappedArrays`)](/ar/sql-reference/aggregate-functions/reference/minMappedArrays.md)
* [`maxMap` (`maxMappedArrays`)](/ar/sql-reference/aggregate-functions/reference/maxMappedArrays.md)

:::note
تكون قيم `SimpleAggregateFunction(func, Type)` من `Type` نفسه،
لذلك، بخلاف النوع `AggregateFunction`، لا حاجة إلى تطبيق
المُركِّبات `-Merge`/`-State`.

يوفر النوع `SimpleAggregateFunction` أداءً أفضل من `AggregateFunction`
لدوال التجميع نفسها.
:::

<div id="example">
  ## مثال
</div>

```sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [استخدام مُركِّبات التجميع في ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)    - مدونة: [استخدام مُركِّبات التجميع في ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* النوع [AggregateFunction](/ar/sql-reference/data-types/aggregatefunction).