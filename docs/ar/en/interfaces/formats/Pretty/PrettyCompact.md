---
alias: []
description: 'وثائق تنسيق PrettyCompact'
input_format: false
keywords: ['PrettyCompact']
output_format: true
slug: /interfaces/formats/PrettyCompact
title: 'PrettyCompact'
doc_type: 'مرجع'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | الإخراج | الاسم البديل |
| ------- | ------- | ------------ |
| ✗       | ✔       |              |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`Pretty`](./Pretty.md) في أن الجدول يُعرض مع خطوط شبكية بين الصفوف.
ولذلك تكون النتيجة أكثر إحكامًا.

:::note
يُستخدم هذا التنسيق افتراضيًا في command-line client في interactive mode.
:::

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />