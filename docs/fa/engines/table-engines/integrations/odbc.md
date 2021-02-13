---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: ODBC
---

# ODBC {#table-engine-odbc}

اجازه می دهد تا تاتر برای اتصال به پایگاه داده های خارجی از طریق [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

با خیال راحت پیاده سازی اتصالات ان بی سی, تاتر با استفاده از یک برنامه جداگانه `clickhouse-odbc-bridge`. اگر راننده او بی سی به طور مستقیم از لود `clickhouse-server`, مشکلات راننده می تواند سرور تاتر سقوط. تاتر به طور خودکار شروع می شود `clickhouse-odbc-bridge` هنگامی که مورد نیاز است. برنامه پل او بی سی از همان بسته به عنوان نصب `clickhouse-server`.

این موتور از [Nullable](../../../sql-reference/data-types/nullable.md) نوع داده.

## ایجاد یک جدول {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(connection_settings, external_database, external_table)
```

مشاهده شرح مفصلی از [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) پرس و جو.

ساختار جدول می تواند از ساختار جدول منبع متفاوت باشد:

-   نام ستون باید همان است که در جدول منبع باشد, اما شما می توانید تنها برخی از این ستون ها و در هر جهت استفاده.
-   انواع ستون ممکن است از کسانی که در جدول منبع متفاوت. فاحشه خانه تلاش می کند تا [بازیگران](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) ارزش ها را به انواع داده های کلیک.

**پارامترهای موتور**

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` پرونده.
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

## مثال طریقه استفاده {#usage-example}

**بازیابی داده ها از نصب و راه اندازی خروجی زیر محلی از طریق ان بی سی**

این مثال برای لینوکس اوبونتو 18.04 و سرور خروجی زیر 5.7 بررسی می شود.

اطمینان حاصل شود که unixODBC و MySQL اتصال نصب شده است.

به طور پیش فرض (در صورت نصب از بسته), کلیک خانه شروع می شود به عنوان کاربر `clickhouse`. بدین ترتیب, شما نیاز به ایجاد و پیکربندی این کاربر در سرور خروجی زیر.

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

جدول در تاتر بازیابی داده ها از جدول خروجی زیر:

``` sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```

``` sql
SELECT * FROM odbc_t
```

``` text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

## همچنین نگاه کنید به {#see-also}

-   [لغت نامه های خارجی ان بی سی](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [تابع جدول ان بی سی](../../../sql-reference/table-functions/odbc.md)

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/odbc/) <!--hide-->
