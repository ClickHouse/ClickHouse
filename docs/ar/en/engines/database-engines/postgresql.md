---
description: 'يتيح الاتصال بقواعد البيانات الموجودة على خادم PostgreSQL بعيد.'
sidebar_label: 'PostgreSQL'
sidebar_position: 40
slug: /engines/database-engines/postgresql
title: 'PostgreSQL'
doc_type: 'guide'
---

يتيح الاتصال بقواعد البيانات الموجودة على خادم [PostgreSQL](https://www.postgresql.org) بعيد. ويدعم عمليات القراءة والكتابة (استعلامات `SELECT` و`INSERT`) لتبادل البيانات بين ClickHouse وPostgreSQL.

يوفّر وصولًا آنيًا إلى قائمة الجداول وبنية الجداول على خادم PostgreSQL البعيد باستخدام استعلامي `SHOW TABLES` و`DESCRIBE TABLE`.

ويدعم تعديل بنية الجداول (`ALTER TABLE ... ADD|DROP COLUMN`). إذا ضُبطت المعلمة `use_table_cache` (انظر معلمة المحرك أدناه) على `1`، فستُخزَّن بنية الجدول مؤقتًا ولن يتم التحقق من تعديلها، لكن يمكن تحديثها باستخدام استعلامي `DETACH` و`ATTACH`.

<div id="creating-a-database">
  ## إنشاء قاعدة بيانات
</div>

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**معلمات المحرك**

* `host:port` — عنوان خادم PostgreSQL.
* `database` — اسم قاعدة البيانات البعيدة.
* `user` — مستخدم PostgreSQL.
* `password` — كلمة مرور المستخدم.
* `schema` — مخطط PostgreSQL.
* `use_table_cache` — يحدّد ما إذا كان هيكل جدول قاعدة البيانات مخزّنًا مؤقتًا أم لا. اختياري. القيمة الافتراضية: `0`.

<div id="data_types-support">
  ## دعم أنواع البيانات
</div>

| PostgreSQL       | ClickHouse                                                                 |
| ---------------- | -------------------------------------------------------------------------- |
| DATE             | [Date](../../sql-reference/data-types/date.md)                             |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)                     |
| REAL             | [Float32](../../sql-reference/data-types/float.md)                         |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)                         |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md) (انظر الملاحظة أدناه) |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)                        |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)                        |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)                        |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)                       |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)                       |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)                         |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))              |
| ARRAY            | [Array](../../sql-reference/data-types/array.md)                           |

:::note
يُحوَّل PostgreSQL `numeric(p, 0)` ذي قيمة precision `p` الأكبر من 76 (وهو الحد الأقصى الذي يدعمه `Decimal256`) — على سبيل المثال `numeric(78, 0)`، والذي يُستخدم عادةً لتخزين أعداد صحيحة بطول 256 بت — إلى [`Int256`](../../sql-reference/data-types/int-uint.md) بدلًا من `Decimal`. وتُرفض القيم التي لا تقع ضمن نطاق `Int256` مع ظهور error.
:::

<div id="examples-of-use">
  ## أمثلة على الاستخدام
</div>

قاعدة بيانات في ClickHouse تتبادل البيانات مع خادم PostgreSQL:

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'schema_name',1);
```

```sql
SHOW DATABASES;
```

```text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

```sql
SHOW TABLES FROM test_database;
```

```text
┌─name───────┐
│ test_table │
└────────────┘
```

قراءة البيانات من جدول PostgreSQL:

```sql
SELECT * FROM test_database.test_table;
```

```text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

كتابة البيانات إلى جدول PostgreSQL:

```sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

لنفترض أنه تم تعديل بنية الجدول في PostgreSQL:

```sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

نظرًا إلى أن المعلَمة `use_table_cache` ضُبطت على `1` عند إنشاء قاعدة البيانات، فقد كانت بنية الجدول في ClickHouse مخزّنة مؤقتًا، ولذلك لم تُعدَّل:

```sql
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

بعد فصل الجدول ثم إرفاقه من جديد، تم تحديث البنية:

```sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [ClickHouse و PostgreSQL - انسجام مثالي في عالم البيانات - الجزء 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* مدونة: [ClickHouse و PostgreSQL - انسجام مثالي في عالم البيانات - الجزء 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)