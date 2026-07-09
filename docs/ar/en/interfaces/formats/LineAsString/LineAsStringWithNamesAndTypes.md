---
alias: []
description: 'توثيق لتنسيق LineAsStringWithNamesAndTypes'
input_format: false
keywords: ['LineAsStringWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/LineAsStringWithNamesAndTypes
title: 'LineAsStringWithNamesAndTypes'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✗       | ✔       |                |

<div id="description">
  ## الوصف
</div>

تنسيق `LineAsStringWithNames` مشابه لتنسيق [`LineAsString`](./LineAsString.md)
لكنه يعرض صفَّي ترويسة: أحدهما يحتوي على أسماء الأعمدة، والآخر على الأنواع.

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

SELECT * FROM example FORMAT LineAsStringWithNamesAndTypes;
```

```response title="Response"
name    value
String    Int32
John    30
Jane    25
Peter    35
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
