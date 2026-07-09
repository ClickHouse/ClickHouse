---
description: 'يتيح الاتصال بقاعدة بيانات SQLite وإجراء استعلامات `INSERT` و`SELECT`
  لتبادل البيانات بين ClickHouse وSQLite.'
sidebar_label: 'SQLite'
sidebar_position: 55
slug: /engines/database-engines/sqlite
title: 'SQLite'
doc_type: 'reference'
---

يتيح الاتصال بقاعدة بيانات [SQLite](https://www.sqlite.org/index.html) وإجراء استعلامات `INSERT` و`SELECT` لتبادل البيانات بين ClickHouse وSQLite.

<div id="creating-a-database">
  ## إنشاء قاعدة بيانات
</div>

```sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**معلمات المحرك**

* `db_path` — مسار ملف يحتوي على قاعدة بيانات SQLite.

<div id="data_types-support">
  ## دعم أنواع البيانات
</div>

يوضح الجدول أدناه تعيين الأنواع الافتراضي عندما يستنتج ClickHouse المخطط تلقائيًا من SQLite:

| SQLite  | ClickHouse                                          |
| ------- | --------------------------------------------------- |
| INTEGER | [Int32](../../sql-reference/data-types/int-uint.md) |
| REAL    | [Float32](../../sql-reference/data-types/float.md)  |
| TEXT    | [String](../../sql-reference/data-types/string.md)  |
| TEXT    | [UUID](../../sql-reference/data-types/uuid.md)      |
| BLOB    | [String](../../sql-reference/data-types/string.md)  |

عند تعريف جدول صراحةً باستخدام أنواع ClickHouse محددة عبر [SQLite table engine](../../engines/table-engines/integrations/sqlite.md)، يمكن تحليل أنواع ClickHouse التالية من أعمدة TEXT في SQLite:

* [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md)
* [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md)
* [UUID](../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../sql-reference/data-types/decimal.md)
* [FixedString](../../sql-reference/data-types/fixedstring.md)
* جميع أنواع الأعداد الصحيحة ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../sql-reference/data-types/float.md)

يعتمد SQLite نظام أنواع ديناميكيًا، وتُجري دوال الوصول بحسب النوع فيه تحويلًا تلقائيًا للأنواع. على سبيل المثال، ستُرجع قراءة عمود TEXT على أنه عدد صحيح القيمة 0 إذا تعذر تحليل النص كرقم. وهذا يعني أنه إذا جرى تعريف جدول ClickHouse بنوع يختلف عن نوع عمود SQLite الأساسي، فقد تُحوَّل القيم ضمنيًا بدلًا من أن تتسبب في حدوث خطأ.

<div id="specifics-and-recommendations">
  ## تفاصيل وتوصيات
</div>

تُخزّن SQLite قاعدة البيانات بالكامل (التعريفات والجداول والفهارس والبيانات نفسها) في ملف واحد متعدد المنصات على الجهاز المضيف. وأثناء الكتابة، تقفل SQLite ملف قاعدة البيانات بالكامل، لذلك تُنفَّذ عمليات الكتابة بشكل تسلسلي، بينما يمكن تنفيذ عمليات القراءة بالتوازي.
لا تتطلب SQLite إدارة خدمة (مثل برامج نصية لبدء التشغيل) أو التحكم في الوصول المستند إلى `GRANT` وكلمات المرور. ويُدار التحكم في الوصول من خلال أذونات نظام الملفات الممنوحة لملف قاعدة البيانات نفسه.

<div id="usage-example">
  ## مثال على الاستخدام
</div>

قاعدة بيانات في ClickHouse متصلة بـ SQLite:

```sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

```text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

يعرض الجداول:

```sql
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

إدراج البيانات في جدول SQLite انطلاقًا من جدول ClickHouse:

```sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```