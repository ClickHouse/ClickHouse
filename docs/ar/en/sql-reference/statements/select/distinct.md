---
description: 'توثيق عبارة DISTINCT'
sidebar_label: 'DISTINCT'
slug: /sql-reference/statements/select/distinct
title: 'عبارة DISTINCT'
doc_type: 'reference'
---

إذا استُخدم `SELECT DISTINCT`، فلن تبقى في نتيجة الاستعلام إلا الصفوف المميزة. وبالتالي، من بين كل مجموعة من الصفوف المتطابقة تمامًا في النتيجة، لن يبقى سوى صف واحد.

يمكنك تحديد قائمة الأعمدة التي يجب أن تكون قيمها فريدة: `SELECT DISTINCT ON (column1, column2,...)`. وإذا لم تُحدَّد الأعمدة، فستُؤخذ جميعها في الاعتبار.

لننظر إلى الجدول:

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

استخدام `DISTINCT` دون تحديد الأعمدة:

```sql
SELECT DISTINCT * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

استخدام `DISTINCT` مع أعمدة محددة:

```sql
SELECT DISTINCT ON (a,b) * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

<div id="distinct-and-order-by">
  ## DISTINCT و ORDER BY
</div>

يدعم ClickHouse استخدام عبارتي `DISTINCT` و `ORDER BY` على أعمدة مختلفة ضمن استعلام واحد. وتُنفَّذ عبارة `DISTINCT` قبل عبارة `ORDER BY`.

انظر إلى الجدول التالي:

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

استعلام البيانات:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

اختيار البيانات وفق اتجاهات فرز مختلفة:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

تم استبعاد الصف `2, 4` قبل الفرز.

خذ هذه الخصوصية في التنفيذ في الحسبان عند برمجة الاستعلامات.

<div id="null-processing">
  ## معالجة NULL
</div>

يعمل `DISTINCT` مع [NULL](/ar/sql-reference/syntax#null) كما لو كانت `NULL` قيمةً محددة، وكأن `NULL==NULL`. وبعبارة أخرى، في نتائج `DISTINCT`، لا تظهر التوليفات المختلفة التي تتضمن `NULL` إلا مرة واحدة فقط. ويختلف ذلك عن معالجة `NULL` في معظم السياقات الأخرى.

<div id="alternatives">
  ## البدائل
</div>

يمكن الحصول على النتيجة نفسها بتطبيق [GROUP BY](/ar/sql-reference/statements/select/group-by) على نفس مجموعة القيم المحددة في عبارة `SELECT`، من دون استخدام أي دوال تجميع. ولكن توجد بعض الاختلافات مقارنةً بأسلوب `GROUP BY`:

* يمكن استخدام `DISTINCT` مع `GROUP BY` في الوقت نفسه.
* عند حذف [ORDER BY](../../../sql-reference/statements/select/order-by.md) وتحديد [LIMIT](../../../sql-reference/statements/select/limit.md)، يتوقف الاستعلام عن التنفيذ فورًا بعد قراءة العدد المطلوب من الصفوف المختلفة.
* تُخرَج كتل البيانات أثناء معالجتها، من دون انتظار اكتمال تنفيذ الاستعلام بالكامل.