---
alias: []
description: 'توثيق تنسيق PrettySpaceNoEscapesMonoBlock'
input_format: false
keywords: ['PrettySpaceNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapesMonoBlock
title: 'PrettySpaceNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | الإخراج | اسم بديل |
| ------- | ------- | -------- |
| ✗       | ✔       |          |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`PrettySpaceNoEscapes`](./PrettySpaceNoEscapes.md) في أنه تُخزَّن مؤقتًا حتى `10,000` صف،
ثم تُعرَض كجدول واحد، وليس على شكل [كتل](/ar/development/architecture#block).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />