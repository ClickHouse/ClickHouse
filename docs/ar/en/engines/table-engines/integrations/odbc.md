---
description: 'يتيح لـ ClickHouse الاتصال بقواعد البيانات الخارجية عبر ODBC.'
sidebar_label: 'ODBC'
sidebar_position: 150
slug: /engines/table-engines/integrations/odbc
title: 'محرك الجدول ODBC'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="odbc-table-engine">
  # محرك الجدول ODBC
</div>

<CloudNotSupportedBadge />

يتيح لـ ClickHouse الاتصال بقواعد بيانات خارجية عبر [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

لتنفيذ اتصالات ODBC بأمان، يستخدم ClickHouse برنامجًا منفصلًا هو `clickhouse-odbc-bridge`. وإذا جرى تحميل ODBC Driver مباشرةً من `clickhouse-server`، فقد تؤدي مشكلات برنامج التشغيل إلى تعطل خادم ClickHouse. ويشغّل ClickHouse تلقائيًا `clickhouse-odbc-bridge` عند الحاجة إليه. ويُثبَّت برنامج ODBC bridge من الحزمة نفسها التي تتضمن `clickhouse-server`.

يدعم هذا المحرك نوع البيانات [Nullable](../../../sql-reference/data-types/nullable.md).

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(datasource, external_database, external_table)
```

اطّلع على وصفٍ مفصل لاستعلام [CREATE TABLE](/ar/sql-reference/statements/create/table).

يمكن أن تختلف بنية الجدول عن بنية الجدول المصدر:

* يجب أن تتطابق أسماء الأعمدة مع الأسماء الموجودة في الجدول المصدر، ولكن يمكنك استخدام بعض هذه الأعمدة فقط وبأي ترتيب.
* قد تختلف أنواع الأعمدة عن تلك الموجودة في الجدول المصدر. ويحاول ClickHouse [تحويل](/ar/sql-reference/functions/type-conversion-functions#CAST) القيم إلى أنواع بيانات ClickHouse.
* يحدّد الإعداد [external&#95;table&#95;functions&#95;use&#95;nulls](/ar/operations/settings/settings#external_table_functions_use_nulls) كيفية التعامل مع الأعمدة Nullable. القيمة الافتراضية: 1. وإذا كانت القيمة 0، فلن تُنشئ دالة الجدول أعمدة Nullable، وستُدرج القيم الافتراضية بدلًا من قيم NULL. وينطبق ذلك أيضًا على قيم NULL داخل المصفوفات.

**معلمات المحرك**

* `datasource` — اسم القسم الذي يحتوي على إعدادات الاتصال في ملف `odbc.ini`.
* `external_database` — اسم قاعدة بيانات في نظام إدارة قواعد بيانات خارجي.
* `external_table` — اسم جدول في `external_database`.

يمكن أيضًا تمرير هذه المعلمات باستخدام [المجموعات المسماة](/ar/operations/named-collections.md).

<div id="usage-example">
  ## مثال على الاستخدام
</div>

**استرجاع البيانات من تثبيت MySQL المحلي عبر ODBC**

تم التحقق من هذا المثال على Ubuntu Linux 18.04 وMySQL server 5.7.

تأكد من تثبيت unixODBC وMySQL Connector.

بشكل افتراضي (إذا كان التثبيت من الحزم)، يبدأ ClickHouse العمل بالمستخدم `clickhouse`. لذلك، تحتاج إلى إنشاء هذا المستخدم وتهيئته على MySQL server.

```bash
$ sudo mysql
```

```sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'localhost' WITH GRANT OPTION;
```

ثم اضبط الاتصال في `/etc/odbc.ini`.

```bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USER = clickhouse
PASSWORD = clickhouse
```

يمكنك التحقق من الاتصال باستخدام الأداة `isql` المتوفرة ضمن تثبيت unixODBC.

```bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

جدول في MySQL:

```text
mysql> CREATE DATABASE test;
Query OK, 1 row affected (0,01 sec)

mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test.test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test.test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

جدول في ClickHouse يسترجع البيانات من جدول MySQL:

```sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```

```sql
SELECT * FROM odbc_t
```

```text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

<div id="see-also">
  ## راجع أيضًا
</div>

* [قواميس ODBC](/ar/sql-reference/statements/create/dictionary/sources/odbc)
* [دالة الجدول ODBC](../../../sql-reference/table-functions/odbc.md)