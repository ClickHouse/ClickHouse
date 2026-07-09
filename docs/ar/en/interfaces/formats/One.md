---
alias: []
description: 'توثيق تنسيق One'
input_format: true
keywords: ['One']
output_format: false
slug: /interfaces/formats/One
title: 'One'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم البديل |
| ------- | ------- | ------------ |
| ✔       | ✗       |              |

<div id="description">
  ## الوصف
</div>

يُعدّ التنسيق `One` تنسيق إدخال خاصًا لا يقرأ أي بيانات من الملف، ويُرجع صفًا واحدًا فقط يتضمن عمودًا من النوع [`UInt8`](../../sql-reference/data-types/int-uint.md) باسم `dummy` وقيمته `0` (مثل الجدول `system.one`).
يمكن استخدامه مع الأعمدة الافتراضية `_file/_path` لسرد جميع الملفات دون قراءة البيانات الفعلية.

<div id="example-usage">
  ## مثال للاستخدام
</div>

مثال:

```sql title="Query"
SELECT _file FROM file('path/to/files/data*', One);
```

```text title="Response"
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
