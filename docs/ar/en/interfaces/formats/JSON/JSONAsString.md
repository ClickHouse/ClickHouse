---
alias: []
description: 'توثيق تنسيق JSONAsString'
input_format: true
keywords: ['JSONAsString']
output_format: false
slug: /interfaces/formats/JSONAsString
title: 'JSONAsString'
doc_type: 'مرجع'
---

| إدخال | إخراج | اسم بديل |
| ----- | ----- | -------- |
| ✔     | ✗     |          |

<div id="description">
  ## الوصف
</div>

في هذا التنسيق، يُفسَّر كائن JSON واحد باعتباره قيمة واحدة.
إذا كان الإدخال يحتوي على عدة كائنات JSON (مفصولة بفواصل)، فستُفسَّر على أنها صفوف منفصلة.
إذا كانت بيانات الإدخال محاطة بـ `[]`، فستُفسَّر على أنها مصفوفة من كائنات JSON.

:::note
لا يمكن تحليل هذا التنسيق إلا لجدول يحتوي على حقل واحد من النوع [String](/ar/sql-reference/data-types/string.md).
ويجب ضبط الأعمدة المتبقية على [`DEFAULT`](/ar/sql-reference/statements/create/table.md/#default) أو [`MATERIALIZED`](/ar/sql-reference/statements/create/view#materialized-view)،
أو إهمالها.
:::

بعد تحويل كائن JSON بالكامل إلى String، يمكنك استخدام [دوال JSON](/ar/sql-reference/functions/json-functions.md) لمعالجته.

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="basic-example">
  ### مثال بسيط
</div>

```sql title="Query"
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

```response title="Response"
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

<div id="an-array-of-json-objects">
  ### مصفوفة من كائنات JSON
</div>

```sql title="Query"
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

```response title="Response"
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
