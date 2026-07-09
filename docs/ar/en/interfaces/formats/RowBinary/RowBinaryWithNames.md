---
description: 'توثيق تنسيق RowBinaryWithNames'
input_format: true
keywords: ['RowBinaryWithNames']
output_format: true
slug: /interfaces/formats/RowBinaryWithNames
title: 'RowBinaryWithNames'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| الإدخال | الإخراج | اسم بديل |
| ------- | ------- | -------- |
| ✔       | ✔       |          |

<div id="description">
  ## الوصف
</div>

يشبه تنسيق [`RowBinary`](./RowBinary.md)، ولكن مع ترويسة إضافية:

* عدد الأعمدة (N) مُرمَّز بترميز [`LEB128`](https://en.wikipedia.org/wiki/LEB128).
* عدد N من القيم `String` التي تحدد أسماء الأعمدة.

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<RowBinaryFormatSettings />

:::note

* إذا كان الإعداد [`input_format_with_names_use_header`](/ar/operations/settings/settings-formats.md/#input_format_with_names_use_header) مضبوطًا على `1`، فستُطابَق الأعمدة الواردة في بيانات الإدخال مع أعمدة الجدول بحسب أسمائها، وسيتم تخطي الأعمدة ذات الأسماء غير المعروفة.
* إذا كان الإعداد [`input_format_skip_unknown_fields`](/ar/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) مضبوطًا على `1`، فسيتم تخطي الحقول غير المعروفة. وإلا، فسيتم تخطي الصف الأول.
  :::