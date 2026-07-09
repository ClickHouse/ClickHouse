---
description: 'يتيح تنفيذ استعلامات على البيانات المخزنة في قاعدة بيانات SQLite.'
sidebar_label: 'sqlite'
sidebar_position: 185
slug: /sql-reference/table-functions/sqlite
title: 'sqlite'
doc_type: 'reference'
---

يتيح تنفيذ استعلامات على البيانات المخزنة في قاعدة بيانات [SQLite](../../engines/database-engines/sqlite.md).

<div id="syntax">
  ## الصيغة
</div>

```sql
sqlite('db_path', 'table_name')
```

<div id="arguments">
  ## المعاملات
</div>

* `db_path` — مسار ملف يحتوي على قاعدة بيانات SQLite. [String](../../sql-reference/data-types/string.md).
* `table_name` — اسم جدول في قاعدة بيانات SQLite، أو استعلام يُمرَّر إلى SQLite كما هو (راجع [تمرير استعلام بدلًا من اسم جدول](#passing-a-query)). [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## القيمة المُعادة
</div>

* كائن جدول يحتوي على الأعمدة نفسها الموجودة في جدول `SQLite` الأصلي.

<div id="passing-a-query">
  ## تمرير استعلام بدلًا من اسم جدول
</div>

بدلًا من اسم جدول، يمكن أن تكون الوسيطة الثانية استعلام `SELECT` يُمرَّر إلى SQLite كما هو. وتُستنتج بنية الجدول الناتج تلقائيًا من نتيجة الاستعلام. ويمكن كتابة الاستعلام إما كاستعلام فرعي أو بتغليفه داخل الدالة `query`:

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

هذا الجدول للقراءة فقط: لا يُسمح بإجراء `INSERT` فيه. كما يدعم محرك الجدول [`SQLite`](/ar/engines/table-engines/integrations/sqlite) الصيغة نفسها.

:::note
يحلّل ClickHouse صيغة الاستعلام الفرعي `(SELECT ...)` ثم يعيد تسلسلها قبل إرسالها إلى SQLite. لذلك يجب أن تكون صالحة في ClickHouse SQL. ولتمرير صيغة خاصة بـ SQLite لا يحلّلها ClickHouse، استخدم الصيغة `query('...')`، إذ يُرسل نصها إلى SQLite كما هو.

أي `WHERE` أو `LIMIT` خارجي، أو aggregation، وما إلى ذلك من استعلام ClickHouse المحيط **لا** يُدفَع إلى داخل الاستعلام الممرَّر، بل يُطبَّق في ClickHouse بعد جلب نتيجة الاستعلام كاملةً. ولتقييد البيانات المقروءة من SQLite، ضع عامل التصفية داخل الاستعلام الممرَّر. ومع [`external_table_strict_query = 1`](/ar/operations/settings/settings#external_table_strict_query)، يُرفَض عامل التصفية الخارجي الذي لا يمكن دفعه إلى المصدر مع استثناء بدلًا من تطبيقه محليًا.
:::

<div id="example">
  ## مثال
</div>

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

<div id="related">
  ## روابط ذات صلة
</div>

* [SQLite](../../engines/table-engines/integrations/sqlite.md) محرك الجدول
* [محرك قاعدة البيانات SQLite](../../engines/database-engines/sqlite.md) — قسم دعم أنواع البيانات