---
description: 'توثيق لتعديل تعبير SAMPLE BY'
sidebar_label: 'SAMPLE BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/sample-by
title: 'تعديل تعبيرات مفتاح أخذ العينات'
doc_type: 'reference'
---

العمليات التالية متاحة:

<div id="modify">
  ## MODIFY
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

يغيّر الأمر [مفتاح أخذ العينات](../../../engines/table-engines/mergetree-family/mergetree.md) للجدول إلى `new_expression` (تعبيرًا أو مجموعةً مرتبةً من التعبيرات). يجب أن يتضمن المفتاح الأساسي مفتاح أخذ العينات الجديد.

<div id="remove">
  ## إزالة
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

يزيل هذا الأمر [مفتاح أخذ العينات](../../../engines/table-engines/mergetree-family/mergetree.md) من الجدول.

الأمران `MODIFY` و`REMOVE` خفيفان بمعنى أنهما لا يغيّران سوى البيانات الوصفية أو يزيلان الملفات.

:::note
لا يعمل هذا إلا مع الجداول من عائلة [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) (بما في ذلك الجداول [المكررة](../../../engines/table-engines/mergetree-family/replication.md)).
:::