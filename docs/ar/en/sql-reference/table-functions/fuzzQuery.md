---
description: 'يُجري تغييرات عشوائية على سلسلة الاستعلام المحددة.'
sidebar_label: 'fuzzQuery'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzQuery
title: 'fuzzQuery'
doc_type: 'reference'
---

يُجري تغييرات عشوائية على سلسلة الاستعلام المحددة.

<div id="syntax">
  ## الصيغة
</div>

```sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

<div id="arguments">
  ## الوسائط
</div>

| الوسيط             | الوصف                                                                           |
| ------------------ | ------------------------------------------------------------------------------- |
| `query`            | ‏(String) - الاستعلام الذي ستُجرى عليه عملية التشويش.                           |
| `max_query_length` | ‏(UInt64) - الحد الأقصى للطول الذي يمكن أن يبلغه الاستعلام أثناء عملية التشويش. |
| `random_seed`      | ‏(UInt64) - بذرة عشوائية لإنتاج نتائج ثابتة.                                    |

<div id="returned_value">
  ## القيمة المُعادة
</div>

كائن جدول يحتوي على عمود واحد يتضمن سلاسل استعلام مُشوَّشة.

<div id="usage-example">
  ## مثال على الاستخدام
</div>

```sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```response
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```