---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "\u062E\u0631\u0648\u062C\u06CC \u0632\u06CC\u0631"
---

# خروجی زیر {#mysql}

اجازه می دهد `SELECT` نمایش داده شد به داده است که بر روی یک سرور خروجی از راه دور ذخیره می شود انجام می شود.

``` sql
mysql('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

**پارامترها**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` نمایش داده شد به `REPLACE INTO`. اگر `replace_query=1`, پرس و جو جایگزین شده است.

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` بیان است که به اضافه `INSERT` پرس و جو.

        Example: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, where `on_duplicate_clause` is `UPDATE c2 = c2 + 1`. See the MySQL documentation to find which `on_duplicate_clause` you can use with the `ON DUPLICATE KEY` clause.

        To specify `on_duplicate_clause` you need to pass `0` to the `replace_query` parameter. If you simultaneously pass `replace_query = 1` and `on_duplicate_clause`, ClickHouse generates an exception.

ساده `WHERE` بند هایی مانند `=, !=, >, >=, <, <=` در حال حاضر بر روی سرور خروجی زیر اجرا شده است.

بقیه شرایط و `LIMIT` محدودیت نمونه برداری در محل کلیک تنها پس از پرس و جو به پس از اتمام خروجی زیر اجرا شده است.

**مقدار بازگشتی**

یک شی جدول با ستون همان جدول خروجی زیر اصلی.

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

انتخاب داده ها از خانه کلیک:

``` sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

## همچنین نگاه کنید به {#see-also}

-   [این ‘MySQL’ موتور جدول](../../engines/table-engines/integrations/mysql.md)
-   [با استفاده از خروجی زیر به عنوان منبع فرهنگ لغت خارجی](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/mysql/) <!--hide-->
