---
alias: []
description: 'توثيق تنسيق JSONEachRowWithProgress'
input_format: false
keywords: ['JSONEachRowWithProgress']
output_format: true
slug: /interfaces/formats/JSONEachRowWithProgress
title: 'JSONEachRowWithProgress'
doc_type: 'مرجع'
---

| إدخال | إخراج | الاسم البديل |
| ----- | ----- | ------------ |
| ✗     | ✔     |              |

<div id="description">
  ## الوصف
</div>

يختلف عن [`JSONEachRow`](./JSONEachRow.md)/[`JSONStringsEachRow`](./JSONStringsEachRow.md) في أن ClickHouse يُرجع أيضًا معلومات التقدّم على هيئة قيم JSON.

<div id="example-usage">
  ## مثال للاستخدام
</div>

```json
{"row":{"num":42,"str":"hello","arr":[0,1]}}
{"row":{"num":43,"str":"hello","arr":[0,1,2]}}
{"row":{"num":44,"str":"hello","arr":[0,1,2,3]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
