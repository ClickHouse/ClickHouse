---
alias: []
description: 'وثائق تنسيق Vertical'
input_format: false
keywords: ['Vertical']
output_format: true
slug: /interfaces/formats/Vertical
title: 'Vertical'
doc_type: 'reference'
---

| الإدخال | المخرجات | الاسم البديل |
| ------- | -------- | ------------ |
| ✗       | ✔        |              |

<div id="description">
  ## الوصف
</div>

يَطبع هذا التنسيق كل قيمة في سطر منفصل مع إظهار اسم العمود المحدد. ويكون مناسبًا لطباعة صف واحد فقط أو بضعة صفوف عندما يتكوّن كل صف من عدد كبير من الأعمدة.

لاحظ أن [`NULL`](/ar/sql-reference/syntax.md) يُطبع على هيئة `ᴺᵁᴸᴸ` لتسهيل التمييز بين القيمة النصية `NULL` وغياب القيمة. أما أعمدة JSON فستُطبع بتنسيق جميل، ويُطبع `NULL` فيها على هيئة `null` لأنه قيمة JSON صالحة ويسهل تمييزها عن `"null"`.

<div id="example-usage">
  ## مثال على الاستخدام
</div>

مثال:

```sql
SELECT * FROM t_null FORMAT Vertical
```

```response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

لا تُطبَّق عملية الإفلات على الصفوف في تنسيق Vertical:

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

```response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

هذا التنسيق مناسب فقط لإخراج نتيجة استعلام، وليس لتحليل البيانات (استرجاعها لإدراجها في جدول).

<div id="format-settings">
  ## إعدادات التنسيق
</div>
