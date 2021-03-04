---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 20
toc_title: "\u0631\u0627\u0628\u0637 MySQL"
---

# رابط MySQL {#mysql-interface}

کلیک پروتکل سیم خروجی زیر را پشتیبانی می کند. این را می توان با فعال [_وارد کردن](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-mysql_port) تنظیم در پرونده پیکربندی:

``` xml
<mysql_port>9004</mysql_port>
```

مثال اتصال با استفاده از ابزار خط فرمان `mysql`:

``` bash
$ mysql --protocol tcp -u default -P 9004
```

خروجی اگر اتصال موفق شد:

``` text
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 4
Server version: 20.2.1.1-ClickHouse

Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

برای سازگاری با تمام مشتریان خروجی زیر, توصیه می شود برای مشخص کردن رمز عبور کاربر با [دو شی1](../operations/settings/settings-users.md#password_double_sha1_hex) در فایل پیکربندی.
اگر رمز عبور کاربر مشخص شده است با استفاده از [SHA256](../operations/settings/settings-users.md#password_sha256_hex) برخی از مشتریان قادر نخواهد بود به تصدیق (رمز و نسخه های قدیمی خط فرمان ابزار خروجی زیر).

محدودیت ها:

-   نمایش داده شد تهیه پشتیبانی نمی شوند

-   برخی از انواع داده ها به عنوان رشته ارسال می شود

[مقاله اصلی](https://clickhouse.tech/docs/en/interfaces/mysql/) <!--hide-->
