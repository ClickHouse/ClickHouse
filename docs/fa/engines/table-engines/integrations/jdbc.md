---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: JDBC
---

# JDBC {#table-engine-jdbc}

اجازه می دهد تا تاتر برای اتصال به پایگاه داده های خارجی از طریق [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

برای پیاده سازی اتصال جدی بی سی, خانه با استفاده از برنامه جداگانه [هومز-جد بی سی-پل](https://github.com/alex-krash/clickhouse-jdbc-bridge) که باید به عنوان یک شبح اجرا شود.

این موتور از [Nullable](../../../sql-reference/data-types/nullable.md) نوع داده.

## ایجاد یک جدول {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(dbms_uri, external_database, external_table)
```

**پارامترهای موتور**

-   `dbms_uri` — URI of an external DBMS.

    قالب: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    به عنوان مثال برای خروجی زیر: `jdbc:mysql://localhost:3306/?user=root&password=root`.

-   `external_database` — Database in an external DBMS.

-   `external_table` — Name of the table in `external_database`.

## مثال طریقه استفاده {#usage-example}

ایجاد یک جدول در سرور خروجی زیر با اتصال مستقیم با مشتری کنسول:

``` text
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

ایجاد یک جدول در سرور کلیک و انتخاب داده ها از:

``` sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

``` sql
SELECT *
FROM jdbc_table
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

## همچنین نگاه کنید به {#see-also}

-   [تابع جدول جدی بی سی](../../../sql-reference/table-functions/jdbc.md).

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/jdbc/) <!--hide-->
