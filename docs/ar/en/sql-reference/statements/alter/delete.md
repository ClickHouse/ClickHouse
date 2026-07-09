---
description: 'توثيق لعبارة ALTER TABLE ... DELETE'
sidebar_label: 'DELETE'
sidebar_position: 39
slug: /sql-reference/statements/alter/delete
title: 'عبارة ALTER TABLE ... DELETE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

يحذف البيانات المطابقة لتعبير التصفية المحدد. ويُنَفَّذ على هيئة [mutation](/ar/sql-reference/statements/alter/index.md#mutations).

:::note
تجعل البادئة `ALTER TABLE` هذه الصياغة مختلفة عن معظم الأنظمة الأخرى التي تدعم SQL. والمقصود منها الإشارة إلى أنه، بخلاف الاستعلامات المشابهة في قواعد بيانات OLTP، فهذه عملية ثقيلة غير مصممة للاستخدام المتكرر. تُعد `ALTER TABLE` عملية ثقيلة تتطلب دمج البيانات الأساسية قبل حذفها. بالنسبة إلى جداول MergeTree، يُنصح باستخدام استعلام [`DELETE FROM`](/ar/sql-reference/statements/delete.md)، الذي ينفذ حذفًا خفيفًا وقد يكون أسرع بكثير.
:::

يجب أن يكون `filter_expr` من النوع `UInt8`. ويحذف الاستعلام الصفوف في الجدول التي تكون فيها قيمة هذا التعبير غير صفرية.

يمكن أن يحتوي الاستعلام الواحد على عدة أوامر مفصولة بفواصل.

يُحدَّد تزامن معالجة الاستعلام بواسطة الإعداد [mutations&#95;sync](/ar/operations/settings/settings.md/#mutations_sync). وهو غير متزامن افتراضيًا.

**انظر أيضًا**

* [Mutations](/ar/sql-reference/statements/alter/index.md#mutations)
* [تزامن استعلامات ALTER](/ar/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* إعداد [mutations&#95;sync](/ar/operations/settings/settings.md/#mutations_sync)

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [التعامل مع التحديثات وعمليات الحذف في ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)