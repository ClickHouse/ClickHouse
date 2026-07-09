---
description: 'يتيح الاتصال بقواعد البيانات على خادم MySQL بعيد وإجراء استعلامات
  `INSERT` و `SELECT` لتبادل البيانات بين ClickHouse وMySQL.'
sidebar_label: 'MySQL'
sidebar_position: 50
slug: /engines/database-engines/mysql
title: 'MySQL'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mysql-database-engine">
  # محرك قاعدة بيانات MySQL
</div>

<CloudNotSupportedBadge />

يتيح هذا المحرك الاتصال بقواعد بيانات على خادم MySQL بعيد وتنفيذ استعلامات `INSERT` و`SELECT` لتبادل البيانات بين ClickHouse وMySQL.

يترجم محرك قاعدة البيانات `MySQL` الاستعلامات إلى خادم MySQL، ما يتيح لك تنفيذ عمليات مثل `SHOW TABLES` أو `SHOW CREATE TABLE`.

لا يمكنك تنفيذ الاستعلامات التالية:

* `RENAME`
* `CREATE TABLE`
* `ALTER`

<div id="creating-a-database">
  ## إنشاء قاعدة بيانات
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', ['database' | database], 'user', 'password')
[SETTINGS enable_compression=0]
```

**معلمات المحرك**

* `host:port` — عنوان خادم MySQL.
* `database` — اسم قاعدة البيانات البعيدة.
* `user` — مستخدم MySQL.
* `password` — كلمة مرور المستخدم.

**الإعدادات**

<div id="enable-compression">
  ### `enable_compression`
</div>

يُفعّل ضغط zlib لاتصال بروتوكول MySQL. عند ضبطه على `1`، يطلب ClickHouse ضغطًا على مستوى البروتوكول من خادم MySQL.

القيمة الافتراضية: `0`.

مثال:

```sql
CREATE DATABASE mysql_db
ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
SETTINGS enable_compression = 1;
```

<div id="data_types-support">
  ## دعم أنواع البيانات
</div>

| MySQL                            | ClickHouse                                                   |
| -------------------------------- | ------------------------------------------------------------ |
| UNSIGNED TINYINT                 | [UInt8](../../sql-reference/data-types/int-uint.md)          |
| TINYINT                          | [Int8](../../sql-reference/data-types/int-uint.md)           |
| UNSIGNED SMALLINT                | [UInt16](../../sql-reference/data-types/int-uint.md)         |
| SMALLINT                         | [Int16](../../sql-reference/data-types/int-uint.md)          |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| INT, MEDIUMINT                   | [Int32](../../sql-reference/data-types/int-uint.md)          |
| UNSIGNED BIGINT                  | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| BIGINT                           | [Int64](../../sql-reference/data-types/int-uint.md)          |
| FLOAT                            | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE                           | [Float64](../../sql-reference/data-types/float.md)           |
| DATE                             | [Date](../../sql-reference/data-types/date.md)               |
| DATETIME, TIMESTAMP              | [DateTime](../../sql-reference/data-types/datetime.md)       |
| BINARY                           | [FixedString](../../sql-reference/data-types/fixedstring.md) |

تُحوَّل جميع أنواع البيانات الأخرى في MySQL إلى [String](../../sql-reference/data-types/string.md).

نوع [Nullable](../../sql-reference/data-types/nullable.md) مدعوم.

<div id="global-variables-support">
  ## دعم المتغيرات العامة
</div>

لتحقيق توافق أفضل، يمكنك الإشارة إلى المتغيرات العامة بأسلوب MySQL، بالشكل `@@identifier`.

المتغيرات التالية مدعومة:

* `version`
* `max_allowed_packet`

:::note
حتى الآن، هذه المتغيرات مجرد تعريفات شكلية ولا تشير إلى أي شيء.
:::

مثال:

```sql
SELECT @@version;
```

<div id="examples-of-use">
  ## أمثلة للاستخدام
</div>

جدول في MySQL:

```text
mysql> USE test;
Database changed

mysql> CREATE TABLE `mysql_table` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from mysql_table;
+------+-----+
| int_id | value |
+------+-----+
|      1 |     2 |
+------+-----+
1 row in set (0,00 sec)
```

قاعدة بيانات في ClickHouse تتبادل البيانات مع خادم MySQL:

```sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password') SETTINGS read_write_timeout=10000, connect_timeout=100;
```

```sql
SHOW DATABASES
```

```text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```

```sql
SHOW TABLES FROM mysql_db
```

```text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

```sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```