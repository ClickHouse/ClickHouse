---
description: 'توثيق لنوع البيانات الخاص Nothing'
sidebar_label: 'Nothing'
sidebar_position: 60
slug: /sql-reference/data-types/special-data-types/nothing
title: 'Nothing'
doc_type: 'مرجع'
---

الغرض الوحيد من نوع البيانات هذا هو تمثيل الحالات التي لا يُتوقَّع فيها وجود قيمة. لذلك لا يمكنك إنشاء قيمة من النوع `Nothing`.

على سبيل المثال، القيمة الحرفية [NULL](/ar/sql-reference/syntax#null) يكون نوعها `Nullable(Nothing)`. اطّلع على مزيد من المعلومات حول [Nullable](../../../sql-reference/data-types/nullable.md).

يمكن أيضًا استخدام النوع `Nothing` للدلالة على المصفوفات الفارغة:

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```