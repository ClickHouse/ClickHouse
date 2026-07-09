---
description: 'توثيق عبارة UNION'
sidebar_label: 'UNION'
slug: /sql-reference/statements/select/union
title: 'عبارة UNION'
doc_type: 'reference'
---

يمكنك استخدام `UNION` مع تحديد `UNION ALL` أو `UNION DISTINCT` بشكل صريح.

إذا لم تحدد `ALL` أو `DISTINCT`، فسيعتمد ذلك على الإعداد `union_default_mode`. والفرق بين `UNION ALL` و`UNION DISTINCT` هو أن `UNION DISTINCT` يزيل القيم المكررة من نتيجة الاتحاد، وهو ما يعادل `SELECT DISTINCT` من استعلام فرعي يحتوي على `UNION ALL`.

يمكنك استخدام `UNION` لدمج أي عدد من استعلامات `SELECT` من خلال ضم نتائجها. مثال:

```sql title="Query"
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

تُطابَق أعمدة النتيجة بحسب موضعها (الترتيب داخل `SELECT`). وإذا لم تتطابق أسماء الأعمدة، فتُؤخذ أسماء النتيجة النهائية من الاستعلام الأول.

يُجرى تحويل الأنواع في حالات `UNION`. على سبيل المثال، إذا كان هناك استعلامان يجري دمجهما ويحتويان على الحقل نفسه بنوعين متوافقين أحدهما `Nullable` والآخر غير `Nullable`، فإن `UNION` الناتج يحتوي على حقل من النوع `Nullable`.

يمكن إحاطة الاستعلامات التي تكون أجزاءً من `UNION` بـ `()`. ويُطبَّق [ORDER BY](../../../sql-reference/statements/select/order-by.md) و [LIMIT](../../../sql-reference/statements/select/limit.md) على كل استعلام على حدة، وليس على النتيجة النهائية. وإذا كنت بحاجة إلى تطبيق تحويل على النتيجة النهائية، فيمكنك وضع جميع الاستعلامات التي تستخدم `UNION` في استعلام فرعي داخل عبارة [FROM](../../../sql-reference/statements/select/from.md).

إذا استخدمت `UNION` من دون تحديد `UNION ALL` أو `UNION DISTINCT` صراحةً، فيمكنك تحديد وضع union باستخدام الإعداد [union&#95;default&#95;mode](/ar/operations/settings/settings#union_default_mode). يمكن أن تكون قيم الإعداد `ALL` أو `DISTINCT` أو سلسلة فارغة. ومع ذلك، إذا استخدمت `UNION` مع ضبط `union_default_mode` على سلسلة فارغة، فسيؤدي ذلك إلى طرح استثناء. توضّح الأمثلة التالية نتائج الاستعلامات عند استخدام قيم مختلفة لهذا الإعداد.

```sql title="Query"
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

```sql title="Query"
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

يمكن تنفيذ الاستعلامات التي تشكّل أجزاءً من `UNION/UNION ALL/UNION DISTINCT` في الوقت نفسه، ويمكن دمج نتائجها معًا.

**انظر أيضًا**

* إعداد [insert&#95;null&#95;as&#95;default](../../../operations/settings/settings.md#insert_null_as_default).
* إعداد [union&#95;default&#95;mode](/ar/operations/settings/settings#union_default_mode).