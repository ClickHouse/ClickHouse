---
description: 'يحوّل استعلامًا فرعيًا إلى جدول. تطبّق هذه الدالة طرق العرض.'
sidebar_label: 'view'
sidebar_position: 210
slug: /sql-reference/table-functions/view
title: 'view'
doc_type: 'مرجع'
---

يحوّل استعلامًا فرعيًا إلى جدول. تطبّق هذه الدالة طرق العرض (راجع [CREATE VIEW](/ar/sql-reference/statements/create/view)). لا يخزّن الجدول الناتج البيانات، بل يخزّن فقط استعلام `SELECT` المحدد. وعند القراءة من الجدول، ينفّذ ClickHouse الاستعلام ويحذف جميع الأعمدة غير الضرورية من النتيجة.

<div id="syntax">
  ## الصياغة
</div>

```sql
view(subquery)
```

<div id="arguments">
  ## الوسائط
</div>

* `subquery` — استعلام `SELECT`.

<div id="returned_value">
  ## القيمة المعادة
</div>

* جدول.

<div id="examples">
  ## أمثلة
</div>

جدول الإدخال:

```text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

```sql title="Query"
SELECT * FROM view(SELECT name FROM months);
```

```text title="Response"
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

يمكنك استخدام الدالة `view` كمعامل في دالتي الجدول [remote](/ar/sql-reference/table-functions/remote) و[cluster](/ar/sql-reference/table-functions/cluster):

```sql title="Query"
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

```sql title="Query"
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

<div id="related">
  ## ذات صلة
</div>

* [محرك جدول view](/ar/engines/table-engines/special/view/)