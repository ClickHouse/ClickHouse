---
description: 'توثيق تعديل تعبيرات المفتاح'
sidebar_label: 'ORDER BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/order-by
title: 'تعديل تعبيرات المفتاح'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

يغيّر الأمر [مفتاح الفرز](../../../engines/table-engines/mergetree-family/mergetree.md) للجدول إلى `new_expression` (تعبير أو مجموعة مرتبة من التعبيرات). ويظل المفتاح الأساسي كما هو.

ويُعد هذا الأمر خفيفًا لأنه يغيّر البيانات الوصفية فقط. وللحفاظ على خاصية ترتيب صفوف أجزاء البيانات وفق تعبير مفتاح الفرز، لا يمكنك إضافة تعبيرات تتضمن أعمدة موجودة بالفعل إلى مفتاح الفرز (يُسمح فقط بالأعمدة التي أُضيفت بواسطة الأمر `ADD COLUMN` ضمن استعلام `ALTER` نفسه، ومن دون قيمة افتراضية للعمود).

:::note
لا يعمل هذا إلا مع الجداول ضمن عائلة [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (بما في ذلك الجداول [المكررة](../../../engines/table-engines/mergetree-family/replication.md)).
:::