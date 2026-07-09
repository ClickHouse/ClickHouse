---
description: 'جدول نظامي يسرد الفهارس الافتراضية (what-if) المعرَّفة في الجلسة الحالية'
keywords: ['جدول نظامي', 'hypothetical_indexes', 'what-if']
sidebar_label: 'hypothetical_indexes'
sidebar_position: 81
slug: /operations/system-tables/hypothetical_indexes
title: 'system.hypothetical_indexes'
doc_type: 'reference'
---

<div id="system-hypothetical-indexes">
  # system.hypothetical_indexes
</div>

يسرد كل skip فهرس افتراضي (what-if) مُعرَّف في الجلسة الحالية. راجع [`CREATE HYPOTHETICAL INDEX`](/ar/sql-reference/statements/hypothetical-index#create-hypothetical-index) و[`EXPLAIN WHATIF`](/ar/sql-reference/statements/explain#explain-whatif).

محتويات الجدول محصورة ضمن نطاق الجلسة: فلا يرى كل اتصال إلا الفهارس الافتراضية الخاصة به، ويكون الجدول فارغًا إذا لم يتم إنشاء أي فهارس في الجلسة الحالية.

تُحدَّد قيمتا `(database, table)` الحاليتان باستخدام معرّف UUID وقت تنفيذ الاستعلام، لذا فإنهما تعكسان `RENAME TABLE`، كما تُخفى تلقائيًا الإدخالات الخاصة بالجداول التي حُذفت.

<div id="columns">
  ## الأعمدة
</div>

| العمود        | النوع    | الوصف                                                            |
| ------------- | -------- | ---------------------------------------------------------------- |
| `database`    | `String` | قاعدة البيانات المستهدفة.                                        |
| `table`       | `String` | الجدول المستهدف.                                                 |
| `name`        | `String` | اسم الفهرس.                                                      |
| `type`        | `String` | نوع الفهرس (`minmax` و`set` و`bloom_filter` وما إلى ذلك).        |
| `type_full`   | `String` | تعبير نوع الفهرس، بما في ذلك الوسيطات، مثل `bloom_filter(0.01)`. |
| `expression`  | `String` | تعبير الفهرس كما هو مكتوب في `CREATE HYPOTHETICAL INDEX`.        |
| `granularity` | `UInt64` | عدد حبيبات البيانات لكل حبيبة فهرس.                              |

<div id="example">
  ## مثال
</div>

```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` هو اسم النوع الأساسي، بينما يتضمن `type_full` المعاملات، لكي يتمكن المستخدمون من التمييز بين المتغيرات ذات المعاملات مثل `bloom_filter(0.01)` و`bloom_filter(0.001)`.

<div id="see-also">
  ## راجع أيضًا
</div>

* [`CREATE HYPOTHETICAL INDEX`](/ar/sql-reference/statements/hypothetical-index#create-hypothetical-index)
* [`EXPLAIN WHATIF`](/ar/sql-reference/statements/explain#explain-whatif)