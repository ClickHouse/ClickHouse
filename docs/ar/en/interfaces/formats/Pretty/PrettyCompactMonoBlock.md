---
alias: []
description: 'وثائق تنسيق PrettyCompactMonoBlock'
input_format: false
keywords: ['PrettyCompactMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactMonoBlock
title: 'PrettyCompactMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | الإخراج | الاسم البديل |
| ------- | ------- | ------------ |
| ✗       | ✔       |              |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`PrettyCompact`](./PrettyCompact.md) في أنه تُخزَّن مؤقتًا حتى `10,000` صف،
ثم تُعرَض كجدول واحد، وليس على شكل [كتل](/ar/development/architecture#block).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />