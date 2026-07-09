---
alias: []
description: 'وثائق تنسيق PrettyCompactNoEscapesMonoBlock'
input_format: false
keywords: ['PrettyCompactNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapesMonoBlock
title: 'PrettyCompactNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | المخرجات | اسم مستعار |
| ------- | -------- | ---------- |
| ✗       | ✔        |            |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`PrettyCompactNoEscapes`](./PrettyCompactNoEscapes.md) في أنه تُخزَّن مؤقتًا حتى `10,000` صفوف،
ثم تُخرَج على هيئة جدول واحد، وليس على شكل [كتل](/ar/development/architecture#block).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />