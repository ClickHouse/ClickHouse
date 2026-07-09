---
alias: []
description: 'توثيق تنسيق LineAsString'
input_format: true
keywords: ['LineAsString']
output_format: true
slug: /interfaces/formats/LineAsString
title: 'LineAsString'
doc_type: 'reference'
---

| الإدخال | الإخراج | اسم بديل |
| ------- | ------- | -------- |
| ✔       | ✔       |          |

<div id="description">
  ## الوصف
</div>

يتعامل التنسيق `LineAsString` مع كل سطر من بيانات الإدخال باعتباره قيمة نصية واحدة.
ولا يمكن تحليل هذا التنسيق إلا لجدول يحتوي على حقل واحد من النوع [String](/ar/sql-reference/data-types/string.md).
ويجب تعيين الأعمدة المتبقية إلى [`DEFAULT`](/ar/sql-reference/statements/create/table.md/#default) أو [`MATERIALIZED`](/ar/sql-reference/statements/create/view#materialized-view)، أو حذفها.

<div id="example-usage">
  ## مثال للاستخدام
</div>

```sql title="Query"
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

```text title="Response"
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
