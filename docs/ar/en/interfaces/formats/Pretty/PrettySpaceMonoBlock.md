---
alias: []
description: 'وثائق تنسيق PrettySpaceMonoBlock'
input_format: false
keywords: ['PrettySpaceMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceMonoBlock
title: 'PrettySpaceMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✗       | ✔       |                |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`PrettySpace`](./PrettySpace.md) في أنه يُخزَّن مؤقتًا ما يصل إلى `10,000` صف،
ثم يُخرَج على هيئة جدول واحد، وليس على شكل [كتل](/ar/development/architecture#block).

<div id="example-usage">
  ## مثال على الاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />