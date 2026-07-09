---
description: 'توثيق عبارة LIMIT'
sidebar_label: 'LIMIT'
slug: /sql-reference/statements/select/limit
title: 'عبارة LIMIT'
doc_type: 'reference'
---

تتحكم العبارة `LIMIT` في عدد الصفوف التي تُعاد في نتيجة الاستعلام.

<div id="basic-syntax">
  ## الصياغة الأساسية
</div>

**تحديد الصفوف الأولى:**

```sql
LIMIT m
```

يعيد أول `m` صفوف من النتيجة، أو جميع السجلات إذا كان عددها أقل من `m`.

**صياغة TOP البديلة (متوافقة مع MS SQL Server):**

```sql
-- SELECT TOP number|percent column_name(s) FROM table_name
SELECT TOP 10 * FROM numbers(100);
SELECT TOP 0.1 * FROM numbers(100);
```

هذا يعادل `LIMIT m`، ويمكن استخدامه للتوافق مع استعلامات Microsoft SQL Server.

**SELECT مع OFFSET:**

```sql
LIMIT m OFFSET n
-- or equivalently:
LIMIT n, m
```

يتخطى أول `n` صفوف، ثم يعيد الصفوف `m` التالية.

في كلتا الصيغتين، يجب أن يكون `n` و`m` عددين صحيحين غير سالبين.

<div id="negative-limits">
  ## الحدود السالبة
</div>

اختر صفوفًا من *نهاية* مجموعة النتائج باستخدام قيم سالبة:

| الصياغة             | النتيجة                            |
| -------------------- | ---------------------------------- |
| `LIMIT -m`           | آخر `m` صفوف                       |
| `LIMIT -m OFFSET -n` | آخر `m` صفوف بعد تخطي آخر `n` صفوف |
| `LIMIT m OFFSET -n`  | أول `m` صفوف بعد تخطي آخر `n` صفوف |
| `LIMIT -m OFFSET n`  | آخر `m` صفوف بعد تخطي أول `n` صفوف |

الصياغة `LIMIT -n, -m` مكافئة للصياغة `LIMIT -m OFFSET -n`.

<div id="fractional-limits">
  ## الحدود الكسرية
</div>

استخدم قيماً عشرية بين 0 و1 لاختيار نسبة مئوية من الصفوف:

| الصياغة                 | النتيجة                                       |
| ----------------------- | --------------------------------------------- |
| `LIMIT 0.1`             | أول 10% من الصفوف                             |
| `LIMIT 1 OFFSET 0.5`    | الصف الأوسط                                   |
| `LIMIT 0.25 OFFSET 0.5` | الربع الثالث (25% من الصفوف بعد تخطي أول 50%) |

:::note

* يجب أن تكون الكسور قيماً من نوع [Float64](../../data-types/float.md) أكبر من 0 وأقل من 1.
* يُقرَّب عدد الصفوف الكسري إلى العدد الصحيح التالي.
  :::

<div id="combining-limit-types">
  ## الجمع بين أنواع LIMIT
</div>

يمكنك الجمع بين الأعداد الصحيحة العادية وقيم الإزاحة الكسرية أو السالبة:

```sql
LIMIT 10 OFFSET 0.5    -- 10 rows starting from the halfway point
LIMIT 10 OFFSET -20    -- 10 rows after skipping the last 20
```

<div id="limit--with-ties-modifier">
  ## LIMIT ... WITH TIES
</div>

يُضمِّن المُعدِّل `WITH TIES` صفوفًا إضافية لها قيم `ORDER BY` نفسها التي يحملها الصف الأخير ضمن الحد.

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

باستخدام `WITH TIES`، تُضمَّن جميع الصفوف المطابقة لآخر قيمة:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5 WITH TIES
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

يتم تضمين الصف 6 لأنه يحمل القيمة نفسها (`2`) كالصف 5.

وينطبق الأمر نفسه عند تحديد الإزاحة باستخدام الكلمة المفتاحية `OFFSET`:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 3 OFFSET 2 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

إن تخطّي أول صفَّين وأخذ 3 صفوف سيُرجِع عادةً `1, 1, 2`، لكن أُدرِجت الـ `2` الثانية لأنها تتساوى مع الصف الأخير.

تعمل `WITH TIES` أيضًا مع الحدود السالبة والإزاحات. وهي تُدرِج صفوفًا إضافية لها قيم `ORDER BY` نفسها الخاصة بأول صف مُحدَّد:

```sql
SELECT number % 3 AS n FROM numbers(15)
ORDER BY n LIMIT -4 OFFSET -3 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

من دون `WITH TIES`، ستكون النتيجة `1, 1, 2, 2`. ومع `WITH TIES`، تُدرَج ثلاثة صفوف إضافية بالقيمة `1` لأنها مساوية لأول صف مُختار.

يمكن دمج هذا المُعدِّل مع المُعدِّل [`ORDER BY ... WITH FILL`](/ar/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier).

<div id="considerations">
  ## اعتبارات
</div>

**نتائج غير حتمية:** من دون عبارة [`ORDER BY`](../../../sql-reference/statements/select/order-by.md)، قد تكون الصفوف المُعادة اعتباطية وقد تختلف من تنفيذ استعلام إلى آخر.

**حد على مستوى الخادم:** يمكن أيضًا أن يتأثر عدد الصفوف المُعادة بإعداد [الحد](../../../operations/settings/settings.md#limit).

<div id="see-also">
  ## انظر أيضًا
</div>

* [LIMIT BY](/ar/sql-reference/statements/select/limit-by) — يقيّد عدد الصفوف لكل مجموعة من القيم، وهو مفيد للحصول على أعلى N من النتائج ضمن كل فئة.