---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 30
toc_title: MySQL
---

# MySQL {#mysql}

اجازه می دهد تا برای اتصال به پایگاه داده بر روی یک سرور خروجی از راه دور و انجام `INSERT` و `SELECT` نمایش داده شد به تبادل اطلاعات بین کلیک و خروجی زیر.

این `MySQL` موتور پایگاه داده ترجمه نمایش داده شد به سرور خروجی زیر بنابراین شما می توانید عملیات مانند انجام `SHOW TABLES` یا `SHOW CREATE TABLE`.

شما می توانید نمایش داده شد زیر را انجام دهد:

-   `RENAME`
-   `CREATE TABLE`
-   `ALTER`

## ایجاد یک پایگاه داده {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', ['database' | database], 'user', 'password')
```

**پارامترهای موتور**

-   `host:port` — MySQL server address.
-   `database` — Remote database name.
-   `user` — MySQL user.
-   `password` — User password.

## پشتیبانی از انواع داده ها {#data_types-support}

| MySQL                            | فاحشه خانه                                                 |
|----------------------------------|------------------------------------------------------------|
| UNSIGNED TINYINT                 | [UInt8](../../sql-reference/data-types/int-uint.md)        |
| TINYINT                          | [Int8](../../sql-reference/data-types/int-uint.md)         |
| UNSIGNED SMALLINT                | [UInt16](../../sql-reference/data-types/int-uint.md)       |
| SMALLINT                         | [Int16](../../sql-reference/data-types/int-uint.md)        |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../../sql-reference/data-types/int-uint.md)       |
| INT, MEDIUMINT                   | [Int32](../../sql-reference/data-types/int-uint.md)        |
| UNSIGNED BIGINT                  | [UInt64](../../sql-reference/data-types/int-uint.md)       |
| BIGINT                           | [Int64](../../sql-reference/data-types/int-uint.md)        |
| FLOAT                            | [Float32](../../sql-reference/data-types/float.md)         |
| DOUBLE                           | [جسم شناور64](../../sql-reference/data-types/float.md)     |
| DATE                             | [تاریخ](../../sql-reference/data-types/date.md)            |
| DATETIME, TIMESTAMP              | [DateTime](../../sql-reference/data-types/datetime.md)     |
| BINARY                           | [رشته ثابت](../../sql-reference/data-types/fixedstring.md) |

همه انواع داده خروجی زیر دیگر به تبدیل [رشته](../../sql-reference/data-types/string.md).

[Nullable](../../sql-reference/data-types/nullable.md) پشتیبانی می شود.

## نمونه هایی از استفاده {#examples-of-use}

جدول در خروجی زیر:

``` text
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

پایگاه داده در خانه, تبادل داده ها با سرور خروجی زیر:

``` sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
```

``` sql
SHOW DATABASES
```

``` text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```

``` sql
SHOW TABLES FROM mysql_db
```

``` text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```

``` sql
SELECT * FROM mysql_db.mysql_table
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

``` sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```

``` sql
SELECT * FROM mysql_db.mysql_table
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/database_engines/mysql/) <!--hide-->
