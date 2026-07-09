---
description: 'توثيق لتعليمات ALTER TABLE ... UPDATE'
sidebar_label: 'UPDATE'
sidebar_position: 40
slug: /sql-reference/statements/alter/update
title: 'تعليمات ALTER TABLE ... UPDATE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

يُعدِّل البيانات المطابقة لتعبير التصفية المحدد. ويُنفَّذ على هيئة [mutation](/ar/sql-reference/statements/alter/index.md#mutations).

:::note
تجعل البادئة `ALTER TABLE` هذه الصياغة مختلفة عن معظم الأنظمة الأخرى التي تدعم SQL. والمقصود من ذلك الإشارة إلى أنه، بخلاف الاستعلامات المشابهة في قواعد بيانات OLTP، فهذه عملية مكلفة غير مصممة للاستخدام المتكرر.
:::

يجب أن يكون `filter_expr` من النوع `UInt8`. يحدِّث هذا الاستعلام قيم الأعمدة المحددة إلى قيم التعبيرات المقابلة في الصفوف التي تأخذ فيها `filter_expr` قيمة غير صفرية. تُحوَّل القيم إلى نوع العمود باستخدام المعامل `CAST`. إن تحديث الأعمدة المستخدمة في حساب المفتاح الأساسي أو مفتاح التقسيم غير مدعوم.

يمكن أن يحتوي الاستعلام الواحد على عدة أوامر مفصولة بفواصل.

يُحدَّد تزامن معالجة الاستعلام بواسطة الإعداد [mutations&#95;sync](/ar/operations/settings/settings.md/#mutations_sync). ويكون افتراضيًا غير متزامن.

**انظر أيضًا**

* [Mutations](/ar/sql-reference/statements/alter/index.md#mutations)
* [تزامن استعلامات ALTER](/ar/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* الإعداد [mutations&#95;sync](/ar/operations/settings/settings.md/#mutations_sync)
* [Lightweight `UPDATE`](/ar/sql-reference/statements/update) - تحديث خفيف بديل يستخدم أجزاء التصحيح
* [`APPLY PATCHES`](/ar/sql-reference/statements/alter/apply-patches) - طبّق التصحيحات يدويًا من التحديثات الخفيفة

<div id="related-content">
  ## محتوى ذي صلة
</div>

* مدونة: [التعامل مع التحديثات وعمليات الحذف في ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)