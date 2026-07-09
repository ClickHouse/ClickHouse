---
alias: []
description: 'توثيق تنسيق PrettyMonoBlock'
input_format: false
keywords: ['PrettyMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyMonoBlock
title: 'PrettyMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | المخرجات | اسم مستعار |
| ------- | -------- | ---------- |
| ✗       | ✔        |            |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`Pretty`](/ar/interfaces/formats/Pretty) في أنه يُخزَّن مؤقتًا ما يصل إلى `10,000` صف،
ثم يُعرَض على هيئة جدول واحد، لا على شكل [كتل](/ar/development/architecture#block).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />