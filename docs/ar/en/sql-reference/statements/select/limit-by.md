---
description: 'توثيق البند LIMIT BY'
sidebar_label: 'LIMIT BY'
slug: /sql-reference/statements/select/limit-by
title: 'البند LIMIT BY'
doc_type: 'reference'
---

يختار الاستعلام الذي يحتوي على البند `LIMIT n BY expressions` أول `n` صفوف لكل قيمة مميزة من `expressions`. ويمكن أن يحتوي مفتاح `LIMIT BY` على أي عدد من [expressions](/ar/sql-reference/syntax#expressions).

يدعم ClickHouse صيغ البنية التالية:

* `LIMIT [offset_value, ]n BY expressions`
* `LIMIT n OFFSET offset_value BY expressions`

أثناء معالجة الاستعلام، يختار ClickHouse البيانات المرتبة وفقًا لـ مفتاح الفرز. ويُحدَّد مفتاح الفرز صراحةً باستخدام البند [ORDER BY](/ar/sql-reference/statements/select/order-by)، أو ضمنيًا كخاصية من خصائص محرك الجدول (لا يكون ترتيب الصفوف مضمونًا إلا عند استخدام [ORDER BY](/ar/sql-reference/statements/select/order-by)، وإلا فلن تكون كتل الصفوف مرتبة بسبب تعدد الخيوط). بعد ذلك، يطبّق ClickHouse ‎`LIMIT n BY expressions`‎ ويُرجع أول `n` صفوف لكل تركيبة مميزة من `expressions`. وإذا تم تحديد `OFFSET`، فإن ClickHouse يتجاوز، لكل كتلة بيانات تنتمي إلى تركيبة مميزة من `expressions`، عددًا من الصفوف يساوي `offset_value` من بداية الكتلة، ثم يُرجع بحد أقصى `n` صفوف. وإذا كانت قيمة `offset_value` أكبر من عدد الصفوف في كتلة البيانات، فلن يُرجع ClickHouse أي صفوف من تلك الكتلة.

:::note
لا يرتبط `LIMIT BY` بـ [LIMIT](../../../sql-reference/statements/select/limit.md). ويمكن استخدامهما معًا في الاستعلام نفسه.
:::

إذا أردت استخدام أرقام الأعمدة بدلًا من أسماء الأعمدة في البند `LIMIT BY`، ففعِّل الإعداد [enable&#95;positional&#95;arguments](/ar/operations/settings/settings#enable_positional_arguments).

<div id="examples">
  ## أمثلة
</div>

جدول مثال:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

الاستعلامات:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

يعيد الاستعلام `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` النتيجة نفسها.

يعيد الاستعلام التالي أفضل 5 مصادر إحالة لكل زوج من `domain, device_type`، على ألا يتجاوز العدد الإجمالي 100 صف (`LIMIT n BY + LIMIT`).

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100;
```

يعمل `LIMIT BY` أيضًا مع الحدود السالبة والإزاحات السالبة. وعلى غرار [عبارة LIMIT السالبة](/ar/sql-reference/statements/select/limit#negative-limits)، يمكنك استخدام قيم سالبة مع `LIMIT BY` لاستخراج الصفوف من *نهاية* كل مجموعة.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

تعيد آخر صفَّين لكل `id`. في حالة `id = 1`، نحصل على الصفَّين `11` و`12`؛ أما في حالة `id = 2`، فتُعاد كلا الصفَّين لأن المجموعة تضم صفَّين فقط.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -1 OFFSET -1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  2 │  20 │
└────┴─────┘
```

يُرجِع الصفَّ قبل الأخير لكل `id`: إذ يُسقِط `OFFSET -1` في النهاية الصفَّ الأخير من كل مجموعة، ثم يُبقي `-1` في البداية الصفَّ الأخير مما يتبقى.

يمكن أيضًا مزج `LIMIT` و`OFFSET` بإشارتين مختلفتين. على سبيل المثال، لإسقاط الصف الأول من كل مجموعة ثم الإبقاء على آخر صفَّين مما يتبقى:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 OFFSET 1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

بالنسبة إلى `id = 1`، يُتخطّى الصف الأول (`10`)؛ وتُعاد القيمتان الأخيرتان `11, 12` كلتاهما. وبالنسبة إلى `id = 2`، يُتخطّى الصف الأول (`20`)، فلا يبقى سوى `21`.

<div id="limit-by-all">
  ## LIMIT BY ALL
</div>

يعادل `LIMIT BY ALL` إدراج جميع التعبيرات المحددة في `SELECT` التي ليست دوالًا تجميعية.

على سبيل المثال:

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY ALL;
```

هو نفسه

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY col1, col2, col3;
```

في حالة خاصة، إذا كانت هناك دالة تتضمن في وسيطاتها كلاً من دوال التجميع وحقولاً أخرى، فستتضمن مفاتيح `LIMIT BY` أكبر عدد ممكن من الحقول غير المجمّعة التي يمكن استخراجها منها.

على سبيل المثال:

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY ALL;
```

هو نفسه

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY substring(a, 4, 2), substring(a, 1, 2);
```

<div id="examples">
  ## أمثلة
</div>

جدول نموذجي:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

الاستعلامات:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

يعطي الاستعلام `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` النتيجة نفسها.

باستخدام `LIMIT BY ALL`:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY ALL;
```

هذا يعادل:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY id, val;
```