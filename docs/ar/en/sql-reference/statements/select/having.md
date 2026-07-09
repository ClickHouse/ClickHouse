---
description: 'توثيق لعبارة HAVING'
sidebar_label: 'HAVING'
slug: /sql-reference/statements/select/having
title: 'عبارة HAVING'
doc_type: 'reference'
---

تتيح تصفية نتائج التجميع الناتجة عن [GROUP BY](/ar/sql-reference/statements/select/group-by). وهي تشبه عبارة [WHERE](../../../sql-reference/statements/select/where.md)، إلا أن الفرق هو أن `WHERE` تُنفَّذ قبل التجميع، في حين تُنفَّذ `HAVING` بعده.

يمكن الإشارة إلى نتائج التجميع من عبارة `SELECT` داخل عبارة `HAVING` باستخدام الاسم المستعار لها. كما يمكن لعبارة `HAVING` أيضًا التصفية استنادًا إلى نتائج عمليات تجميع إضافية لا تُعاد ضمن نتيجة الاستعلام.

<div id="example">
  ## مثال
</div>

إذا كان لديك جدول `sales` على النحو التالي:

```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

يمكنك الاستعلام عنه على النحو التالي:

```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```

سيعرض هذا مندوبي المبيعات الذين يتجاوز إجمالي مبيعاتهم 10,000 في منطقتهم.

<div id="limitations">
  ## القيود
</div>

لا يمكن استخدام `HAVING` إذا لم يُنفَّذ التجميع. استخدم `WHERE` بدلًا منه.