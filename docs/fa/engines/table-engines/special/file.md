---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: "\u067E\u0631\u0648\u0646\u062F\u0647"
---

# پرونده {#table_engines-file}

موتور جدول فایل داده ها را در یک فایل در یکی از پشتیبانی نگه می دارد [پرونده
فرشها](../../../interfaces/formats.md#formats) (تابسپار, بومی, و غیره.).

نمونه های استفاده:

-   صادرات داده ها از خانه کلیک به فایل.
-   تبدیل داده ها از یک فرمت به دیگری.
-   به روز رسانی داده ها در تاتر از طریق ویرایش یک فایل بر روی یک دیسک.

## استفاده در سرور کلیک {#usage-in-clickhouse-server}

``` sql
File(Format)
```

این `Format` پارامتر یکی از فرمت های فایل های موجود را مشخص می کند. برای انجام
`SELECT` نمایش داده شد, فرمت باید برای ورودی پشتیبانی می شود, و به انجام
`INSERT` queries – for output. The available formats are listed in the
[فرشها](../../../interfaces/formats.md#formats) بخش.

کلیک اجازه نمی دهد مسیر سیستم فایل را مشخص کنید`File`. این پوشه تعریف شده توسط استفاده کنید [مسیر](../../../operations/server-configuration-parameters/settings.md) تنظیم در پیکربندی سرور.

هنگام ایجاد جدول با استفاده از `File(Format)` این دایرکتوری فرعی خالی در این پوشه ایجاد می کند. هنگامی که داده ها به جدول نوشته شده است, این را به قرار `data.Format` فایل در دایرکتوری فرعی.

شما می توانید این زیر پوشه و فایل را در فایل سیستم سرور و سپس ایجاد کنید [ATTACH](../../../sql-reference/statements/misc.md) این جدول اطلاعات با نام تطبیق, بنابراین شما می توانید داده ها را از این فایل پرس و جو.

!!! warning "اخطار"
    مراقب باشید با این قابلیت, به دلیل تاتر می کند پیگیری تغییرات خارجی به چنین فایل را حفظ کند. نتیجه همزمان می نویسد: از طریق ClickHouse و خارج از ClickHouse تعریف نشده است.

**مثال:**

**1.** تنظیم `file_engine_table` جدول:

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

به طور پیش فرض کلیک خواهد پوشه ایجاد کنید `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** دستی ایجاد کنید `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` حاوی:

``` bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** پرسوجوی داده:

``` sql
SELECT * FROM file_engine_table
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## استفاده در کلیک-محلی {#usage-in-clickhouse-local}

داخل [کلیک-محلی](../../../operations/utilities/clickhouse-local.md) موتور فایل مسیر فایل علاوه بر می پذیرد `Format`. جریان های ورودی / خروجی پیش فرض را می توان با استفاده از نام های عددی یا قابل خواندن توسط انسان مشخص کرد `0` یا `stdin`, `1` یا `stdout`.
**مثال:**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## اطلاعات پیاده سازی {#details-of-implementation}

-   چندگانه `SELECT` نمایش داده شد را می توان به صورت همزمان انجام, ولی `INSERT` نمایش داده شد هر یک از دیگر صبر کنید.
-   پشتیبانی از ایجاد فایل جدید توسط `INSERT` پرس و جو.
-   اگر پرونده وجود داشته باشد, `INSERT` ارزش های جدید را در این برنامه اضافه کنید.
-   پشتیبانی نمیشود:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   شاخص ها
    -   تکرار

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/file/) <!--hide-->
