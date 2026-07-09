---
description: 'يتيح لـ ClickHouse الاتصال بقواعد بيانات خارجية عبر JDBC.'
sidebar_label: 'JDBC'
sidebar_position: 100
slug: /engines/table-engines/integrations/jdbc
title: 'محرك جدول JDBC'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="jdbc-table-engine">
  # محرك جدول JDBC
</div>

<CloudNotSupportedBadge />

:::note
يحتوي clickhouse-jdbc-bridge على شيفرة تجريبية ولم يعد مدعومًا. وقد يتضمن مشكلات في الاعتمادية وثغرات أمنية. استخدمه على مسؤوليتك الخاصة.
توصي ClickHouse باستخدام دوال الجداول المضمنة في ClickHouse، إذ توفّر بديلًا أفضل لسيناريوهات الاستعلامات المخصصة (Postgres وMySQL وMongoDB وما إلى ذلك).
:::

يتيح لـ ClickHouse الاتصال بقواعد بيانات خارجية عبر [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

لتنفيذ اتصال JDBC، تستخدم ClickHouse البرنامج المنفصل [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge)، والذي ينبغي تشغيله كخدمة في الخلفية.

يدعم هذا المحرك نوع البيانات [Nullable](../../../sql-reference/data-types/nullable.md).

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource, external_database, external_table)
```

**معلمات المحرك**

* `datasource` — معرّف URI أو اسم لنظام إدارة قواعد بيانات خارجي.

  تنسيق URI: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
  مثال لـ MySQL: `jdbc:mysql://localhost:3306/?user=root&password=root`.

* `external_database` — اسم قاعدة بيانات في نظام إدارة قواعد بيانات خارجي، أو بدلًا من ذلك مخطط جدول محدد صراحةً (راجع الأمثلة).

* `external_table` — اسم الجدول في قاعدة بيانات خارجية، أو استعلام `select` مثل `select * from table1 where column1=1`.

* يمكن أيضًا تمرير هذه المعلمات باستخدام [المجموعات المسماة](/ar/operations/named-collections.md).

<div id="usage-example">
  ## مثال على الاستخدام
</div>

إنشاء جدول في خادم MySQL بالاتصال به مباشرةً باستخدام عميل سطر الأوامر الخاص به:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

إنشاء جدول في ClickHouse server والاستعلام عن البيانات منه:

```sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

```sql
SELECT *
FROM jdbc_table
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

```sql
INSERT INTO jdbc_table(`int_id`, `float`)
SELECT toInt32(number), toFloat32(number * 1.0)
FROM system.numbers
```

<div id="see-also">
  ## راجع أيضًا
</div>

* [دالة الجدول JDBC](../../../sql-reference/table-functions/jdbc.md).