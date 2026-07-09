---
alias: []
description: 'وثائق تنسيق RowBinaryWithDefaults'
input_format: true
keywords: ['RowBinaryWithDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithDefaults
title: 'RowBinaryWithDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✗       |                |

<div id="description">
  ## الوصف
</div>

يشبه تنسيق [`RowBinary`](./RowBinary.md)، لكن مع بايت إضافي قبل كل عمود يحدد ما إذا كان ينبغي استخدام القيمة الافتراضية.

<div id="example-usage">
  ## مثال للاستخدام
</div>

أمثلة:

```sql title="Query"
SELECT * FROM FORMAT('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')
```

```response title="Response"
┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

* بالنسبة للعمود `x`، يوجد بايت واحد فقط هو `01`، ويشير إلى أنه يجب استخدام القيمة الافتراضية، ولا تَرِد أي بيانات أخرى بعد هذا البايت.
* بالنسبة للعمود `y`، تبدأ البيانات بالبايت `00`، ويشير ذلك إلى أن العمود يحتوي على قيمة فعلية يجب قراءتها من البيانات اللاحقة `01000000`.

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<RowBinaryFormatSettings />