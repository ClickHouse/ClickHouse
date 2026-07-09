---
alias: []
description: 'توثيق التنسيق PrettyCompactNoEscapes'
input_format: false
keywords: ['PrettyCompactNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapes
title: 'PrettyCompactNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | الإخراج | اسم بديل |
| ------- | ------- | -------- |
| ✗       | ✔       |          |

<div id="description">
  ## الوصف
</div>

يختلف عن التنسيق [`PrettyCompact`](./PrettyCompact.md) في أنه لا يستخدم [تسلسلات هروب ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code).
وهذا ضروري لعرض هذا التنسيق في المتصفح، وكذلك لاستخدام الأداة `watch` من سطر الأوامر.

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />