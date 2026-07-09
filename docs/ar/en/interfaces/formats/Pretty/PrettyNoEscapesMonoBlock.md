---
alias: []
description: 'توثيق تنسيق PrettyNoEscapesMonoBlock'
input_format: false
keywords: ['PrettyNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyNoEscapesMonoBlock
title: 'PrettyNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✗       | ✔       |                |

<div id="description">
  ## الوصف
</div>

يختلف عن التنسيق [`PrettyNoEscapes`](./PrettyNoEscapes.md) في أنه تُخزَّن مؤقتًا حتى `10,000` صفوف،
ثم تُعرَض كجدول واحد، وليس على شكل blocks.

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="format-settings">
  ## إعدادات الصيغة
</div>

<PrettyFormatSettings />