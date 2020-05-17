---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: MySQL
---

# Mysql {#mysql}

موتور خروجی زیر اجازه می دهد تا شما را به انجام `SELECT` نمایش داده شد در داده است که بر روی یک سرور خروجی از راه دور ذخیره می شود.

## ایجاد یک جدول {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

مشاهده شرح مفصلی از [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) پرس و جو.

ساختار جدول می تواند از ساختار جدول خروجی زیر اصلی متفاوت است:

-   نام ستون باید همان است که در جدول خروجی زیر اصلی باشد, اما شما می توانید تنها برخی از این ستون ها و در هر جهت استفاده.
-   انواع ستون ممکن است از کسانی که در جدول خروجی زیر اصلی متفاوت است. فاحشه خانه تلاش می کند تا [بازیگران](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) ارزش ها را به انواع داده های کلیک.

**پارامترهای موتور**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` نمایش داده شد به `REPLACE INTO`. اگر `replace_query=1`, پرس و جو جایگزین شده است.

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` بیان است که به اضافه `INSERT` پرس و جو.

    مثال: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1` کجا `on_duplicate_clause` هست `UPDATE c2 = c2 + 1`. دیدن [مستندات خروجی زیر](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) برای پیدا کردن که `on_duplicate_clause` شما می توانید با استفاده از `ON DUPLICATE KEY` بند بند.

    برای مشخص کردن `on_duplicate_clause` شما نیاز به تصویب `0` به `replace_query` پارامتر. اگر شما به طور همزمان عبور `replace_query = 1` و `on_duplicate_clause`, تاتر تولید یک استثنا.

ساده `WHERE` بند هایی مانند `=, !=, >, >=, <, <=` بر روی سرور خروجی زیر اجرا شده است.

بقیه شرایط و `LIMIT` محدودیت نمونه برداری در محل کلیک تنها پس از پرس و جو به پس از اتمام خروجی زیر اجرا شده است.

## مثال طریقه استفاده {#usage-example}

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

جدول در تاتر, بازیابی داده ها از جدول خروجی زیر ایجاد شده در بالا:

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## همچنین نگاه کنید به {#see-also}

-   [این ‘mysql’ تابع جدول](../../../sql-reference/table-functions/mysql.md)
-   [با استفاده از خروجی زیر به عنوان منبع فرهنگ لغت خارجی](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/mysql/) <!--hide-->
