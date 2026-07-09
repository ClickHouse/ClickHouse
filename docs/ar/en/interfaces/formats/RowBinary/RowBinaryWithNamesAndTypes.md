---
alias: []
description: 'توثيق تنسيق RowBinaryWithNamesAndTypes'
input_format: true
keywords: ['RowBinaryWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/RowBinaryWithNamesAndTypes
title: 'RowBinaryWithNamesAndTypes'
doc_type: 'مرجع'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| الإدخال | الإخراج | اسم بديل |
| ------- | ------- | -------- |
| ✔       | ✔       |          |

<div id="description">
  ## الوصف
</div>

مشابه لتنسيق [RowBinary](./RowBinary.md)، ولكن مع ترويسة إضافية:

* عدد الأعمدة (N) المرمّز باستخدام [`LEB128`](https://en.wikipedia.org/wiki/LEB128).
* `String` عددها N لتحديد أسماء الأعمدة.
* `String` عددها N لتحديد أنواع الأعمدة.

<div id="example-usage">
  ## مثال على الاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<RowBinaryFormatSettings />

:::note
إذا كانت قيمة الإعداد [`input_format_with_names_use_header`](/ar/operations/settings/settings-formats.md/#input_format_with_names_use_header) مضبوطة على `1`،
فستُطابَق الأعمدة في بيانات الإدخال مع أعمدة الجدول بحسب أسمائها، وسيتم تخطي الأعمدة ذات الأسماء غير المعروفة إذا كانت قيمة الإعداد [input&#95;format&#95;skip&#95;unknown&#95;fields](/ar/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) مضبوطة على `1`.
وإلا، فسيتم تخطي الصف الأول.
إذا كانت قيمة الإعداد [`input_format_with_types_use_header`](/ar/operations/settings/settings-formats.md/#input_format_with_types_use_header) مضبوطة على `1`،
فستُقارَن الأنواع في بيانات الإدخال بأنواع الأعمدة المقابلة في الجدول. وإلا، فسيتم تخطي الصف الثاني.
:::