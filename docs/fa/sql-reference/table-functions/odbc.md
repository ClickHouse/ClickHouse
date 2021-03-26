---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "\u062C\u0633\u062A\u062C\u0648"
---

# جستجو {#table-functions-odbc}

بازگرداندن جدول است که از طریق متصل [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

``` sql
odbc(connection_settings, external_database, external_table)
```

پارامترها:

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` پرونده.
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

با خیال راحت پیاده سازی اتصالات ان بی سی, تاتر با استفاده از یک برنامه جداگانه `clickhouse-odbc-bridge`. اگر راننده او بی سی به طور مستقیم از لود `clickhouse-server`, مشکلات راننده می تواند سرور تاتر سقوط. تاتر به طور خودکار شروع می شود `clickhouse-odbc-bridge` هنگامی که مورد نیاز است. برنامه پل او بی سی از همان بسته به عنوان نصب `clickhouse-server`.

زمینه های با `NULL` مقادیر از جدول خارجی به مقادیر پیش فرض برای نوع داده پایه تبدیل می شوند. مثلا, اگر یک میدان جدول خروجی زیر از راه دور است `INT NULL` نوع این است که به 0 تبدیل (مقدار پیش فرض برای کلیک `Int32` نوع داده).

## مثال طریقه استفاده {#usage-example}

**گرفتن اطلاعات از نصب و راه اندازی خروجی زیر محلی از طریق ان بی سی**

این مثال برای لینوکس اوبونتو 18.04 و سرور خروجی زیر 5.7 بررسی می شود.

اطمینان حاصل شود که unixODBC و MySQL اتصال نصب شده است.

به طور پیش فرض (در صورت نصب از بسته), کلیک خانه شروع می شود به عنوان کاربر `clickhouse`. بنابراین شما نیاز به ایجاد و پیکربندی این کاربر در سرور خروجی زیر.

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

سپس اتصال را پیکربندی کنید `/etc/odbc.ini`.

``` bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

شما می توانید اتصال با استفاده از بررسی `isql` ابزار از unixODBC نصب و راه اندازی.

``` bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

جدول در خروجی زیر:

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

بازیابی اطلاعات از جدول خروجی زیر در کلیک:

``` sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

## همچنین نگاه کنید به {#see-also}

-   [لغت نامه های خارجی ان بی سی](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [موتور جدول ان بی سی](../../engines/table-engines/integrations/odbc.md).

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
