---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: "\u06A9\u0644\u06CC\u06A9-\u0645\u062D\u0644\u06CC"
---

# کلیک-محلی {#clickhouse-local}

این `clickhouse-local` برنامه شما را قادر به انجام پردازش سریع بر روی فایل های محلی, بدون نیاز به استقرار و پیکربندی سرور کلیک.

داده هایی را می پذیرد که نشان دهنده جداول و نمایش داده شد با استفاده از [گویش کلیک مربعی](../../sql-reference/index.md).

`clickhouse-local` بنابراین پشتیبانی از بسیاری از ویژگی های و همان مجموعه ای از فرمت ها و موتورهای جدول با استفاده از هسته همان سرور تاتر.

به طور پیش فرض `clickhouse-local` دسترسی به داده ها در همان میزبان ندارد, اما پشتیبانی از پیکربندی سرور در حال بارگذاری با استفاده از `--config-file` استدلال کردن.

!!! warning "اخطار"
    توصیه نمی شود که پیکربندی سرور تولید را بارگیری کنید `clickhouse-local` زیرا داده ها می توانند در صورت خطای انسانی صدمه ببینند.

## استفاده {#usage}

استفاده عمومی:

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

نشانوندها:

-   `-S`, `--structure` — table structure for input data.
-   `-if`, `--input-format` — input format, `TSV` به طور پیش فرض.
-   `-f`, `--file` — path to data, `stdin` به طور پیش فرض.
-   `-q` `--query` — queries to execute with `;` به عنوان دسیمترتراپی.
-   `-N`, `--table` — table name where to put output data, `table` به طور پیش فرض.
-   `-of`, `--format`, `--output-format` — output format, `TSV` به طور پیش فرض.
-   `--stacktrace` — whether to dump debug output in case of exception.
-   `--verbose` — more details on query execution.
-   `-s` — disables `stderr` ثبت.
-   `--config-file` — path to configuration file in same format as for ClickHouse server, by default the configuration empty.
-   `--help` — arguments references for `clickhouse-local`.

همچنین استدلال برای هر متغیر پیکربندی کلیک که معمولا به جای استفاده می شود وجود دارد `--config-file`.

## مثالها {#examples}

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

مثال قبلی همان است:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

حالا اجازه دهید خروجی کاربر حافظه برای هر کاربر یونیکس:

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' | clickhouse-local -S "user String, mem Float64" -q "SELECT user, round(sum(mem), 2) as memTotal FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

``` text
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/utils/clickhouse-local/) <!--hide-->
