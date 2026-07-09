---
description: 'توثيق الدالة flipCoordinates'
sidebar_label: 'تبديل الإحداثيات'
sidebar_position: 63
slug: /sql-reference/functions/geo/flipCoordinates
title: 'عكس الإحداثيات'
doc_type: 'reference'
---

<div id="flipcoordinates">
  ## flipCoordinates
</div>

تبدّل الدالة `flipCoordinates` إحداثيات Point أو Ring أو Polygon أو مضلع متعدد. ويكون ذلك مفيدًا، على سبيل المثال، عند التحويل بين أنظمة الإحداثيات التي يختلف فيها ترتيب خط العرض وخط الطول.

```sql
flipCoordinates(coordinates)
```

<div id="input-parameters">
  ### معلمات الإدخال
</div>

* `coordinates` — قيمة `Tuple` تمثل نقطة `(x, y)`، أو مصفوفة من قيم `Tuple` من هذا النوع تمثل حلقة أو مضلعًا أو `Multipolygon`. تشمل أنواع الإدخال المدعومة ما يلي:
  * [**نقطة**](../../data-types/geo.md#point): قيمة `Tuple` بالشكل `(x, y)`، حيث تكون `x` و`y` قيمتين من نوع [Float64](../../data-types/float.md).
  * [**حلقة**](../../data-types/geo.md#ring): مصفوفة من النقاط `[(x1, y1), (x2, y2), ...]`.
  * [**مضلع**](../../data-types/geo.md#polygon): مصفوفة من الحلقات `[ring1, ring2, ...]`، حيث تكون كل حلقة مصفوفة من النقاط.
  * [**Multipolygon**](../../data-types/geo.md#multipolygon): مصفوفة من المضلعات `[polygon1, polygon2, ...]`.

<div id="returned-value">
  ### القيمة المُعادة
</div>

تعيد الدالة المُدخلات بعد قلب الإحداثيات. على سبيل المثال:

* تتحول النقطة `(x, y)` إلى `(y, x)`.
* تتحول الحلقة `[(x1, y1), (x2, y2)]` إلى `[(y1, x1), (y2, x2)]`.
* تُعالَج البُنى المتداخلة مثل المضلعات والمضلعات المتعددة بشكل递اعي.

<div id="examples">
  ### أمثلة
</div>

<div id="example-1">
  #### المثال 1: عكس إحداثيات نقطة واحدة
</div>

```sql
SELECT flipCoordinates((10, 20)) AS flipped_point
```

```text
┌─flipped_point─┐
│ (20,10)       │
└───────────────┘
```

<div id="example-2">
  #### مثال 2: عكس مصفوفة من النقاط (حلقة)
</div>

```sql
SELECT flipCoordinates([(10, 20), (30, 40)]) AS flipped_ring
```

```text
┌─flipped_ring──────────────┐
│ [(20,10),(40,30)]         │
└───────────────────────────┘
```

<div id="example-3">
  #### مثال 3: عكس إحداثيات مضلع
</div>

```sql
SELECT flipCoordinates([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]) AS flipped_polygon
```

```text
┌─flipped_polygon──────────────────────────────┐
│ [[(20,10),(40,30)],[(60,50),(80,70)]]        │
└──────────────────────────────────────────────┘
```

<div id="example-4">
  #### مثال 4: عكس إحداثيات Multipolygon
</div>

```sql
SELECT flipCoordinates([[[10, 20], [30, 40]], [[50, 60], [70, 80]]]) AS flipped_multipolygon
```

```text
┌─flipped_multipolygon──────────────────────────────┐
│ [[[20,10],[40,30]],[[60,50],[80,70]]]             │
└───────────────────────────────────────────────────┘
```