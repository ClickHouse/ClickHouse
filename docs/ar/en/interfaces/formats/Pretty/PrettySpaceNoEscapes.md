---
alias: []
description: 'وثائق تنسيق PrettySpaceNoEscapes'
input_format: false
keywords: ['PrettySpaceNoEscapes']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapes
title: 'PrettySpaceNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✗       | ✔       |                |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`PrettySpace`](./PrettySpace.md) في أنه لا يستخدم [تسلسلات الهروب ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code).
وهذا ضروري لعرض هذا التنسيق في المتصفح، وكذلك لاستخدام أداة سطر الأوامر &#39;watch&#39;.

<div id="example-usage">
  ## مثال على الاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />