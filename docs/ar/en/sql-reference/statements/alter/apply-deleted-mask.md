---
description: 'توثيق APPLY DELETED MASK للصفوف المحذوفة'
sidebar_label: 'APPLY DELETED MASK'
sidebar_position: 46
slug: /sql-reference/statements/alter/apply-deleted-mask
title: 'APPLY DELETED MASK للصفوف المحذوفة'
doc_type: 'مرجع'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

يطبّق هذا الأمر القناع الذي أنشأه [الحذف الخفيف](/ar/sql-reference/statements/delete)، ويزيل قسرًا الصفوف المعلَّمة على أنها محذوفة من القرص. هذا الأمر عبارة عن mutation ثقيلة، وهو من حيث المعنى مكافئ للاستعلام `ALTER TABLE [db].name DELETE WHERE _row_exists = 0`.

:::note
لا يعمل إلا مع الجداول ضمن عائلة [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (بما في ذلك الجداول [replicated](../../../engines/table-engines/mergetree-family/replication.md)).
:::

**انظر أيضًا**

* [عمليات الحذف الخفيف](/ar/sql-reference/statements/delete)
* [عمليات الحذف الثقيلة](/ar/sql-reference/statements/alter/delete.md)