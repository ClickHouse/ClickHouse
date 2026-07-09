---
description: 'يُحلِّل البيانات من الوسائط وفق تنسيق الإدخال المحدد. وإذا لم يتم تحديد وسيط البنية، فسيتم استخراجها من البيانات.'
slug: /sql-reference/table-functions/format
sidebar_position: 65
sidebar_label: 'format'
title: 'format'
doc_type: 'reference'
---

يُحلِّل البيانات من الوسائط وفق تنسيق الإدخال المحدد. وإذا لم يتم تحديد وسيط البنية، فسيتم استخراجها من البيانات.

<div id="syntax">
  ## البنية
</div>

```sql
format(format_name, [structure], data)
```

<div id="arguments">
  ## الوسائط
</div>

* `format_name` — [تنسيق](/ar/sql-reference/formats) البيانات.
* `structure` - بنية الجدول. اختياري. التنسيق: &#39;column1&#95;name column1&#95;type, column2&#95;name column2&#95;type, ...&#39;.
* `data` — قيمة حرفية من النوع `String` أو تعبير ثابت يُرجع سلسلة تحتوي على بيانات بالتنسيق المحدد

<div id="returned_value">
  ## القيمة المعادة
</div>

جدول يتضمن بيانات جرى تحليلها من الوسيط `data` وفقًا للتنسيق المحدد والبنية المحددة أو المستخرجة.

<div id="examples">
  ## أمثلة
</div>

من دون وسيط `structure`:

```sql title="Query"
SELECT * FROM format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

```response title="Response"
┌───b─┬─a─────┐
│ 111 │ Hello │
│ 123 │ World │
│ 112 │ Hello │
│ 124 │ World │
└─────┴───────┘
```

```sql title="Query"
DESC format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

```response title="Response"
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ b    │ Nullable(Float64) │              │                    │         │                  │                │
│ a    │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

باستخدام الوسيط `structure`:

```sql title="Query"
SELECT * FROM format(JSONEachRow, 'a String, b UInt32',
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

```response title="Response"
┌─a─────┬───b─┐
│ Hello │ 111 │
│ World │ 123 │
│ Hello │ 112 │
│ World │ 124 │
└───────┴─────┘
```

<div id="related">
  ## مواضيع ذات صلة
</div>

* [التنسيقات](../../interfaces/formats.md)