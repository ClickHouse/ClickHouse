---
alias: []
description: 'توثيق تنسيق PrettyNoEscapes'
input_format: false
keywords: ['PrettyNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyNoEscapes
title: 'PrettyNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | الإخراج | اسم بديل |
| ------- | ------- | -------- |
| ✗       | ✔       |          |

<div id="description">
  ## الوصف
</div>

يختلف عن [Pretty](/ar/interfaces/formats/Pretty) في أنه لا يستخدم [تسلسلات الهروب ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code).
وهذا ضروري لعرض هذا التنسيق في المتصفح، وكذلك لاستخدام الأداة watch من سطر الأوامر.

<div id="example-usage">
  ## مثال للاستخدام
</div>

مثال:

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
يمكن استخدام [واجهة HTTP](/ar/interfaces/http) لعرض هذا التنسيق في المتصفح.
:::

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />