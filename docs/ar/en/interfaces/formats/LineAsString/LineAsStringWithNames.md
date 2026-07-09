---
alias: []
description: 'توثيق لتنسيق LineAsStringWithNames'
input_format: false
keywords: ['LineAsStringWithNames']
output_format: true
slug: /interfaces/formats/LineAsStringWithNames
title: 'LineAsStringWithNames'
doc_type: 'مرجع'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✗       | ✔       |                |

<div id="description">
  ## الوصف
</div>

تنسيق `LineAsStringWithNames` مشابه للتنسيق [`LineAsString`](./LineAsString.md)، لكنه يطبع صف العناوين الذي يتضمن أسماء الأعمدة.

<div id="example-usage">
  ## مثال للاستخدام
</div>

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNames;
```

```response title="Response"
name    value
John    30
Jane    25
Peter    35
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
