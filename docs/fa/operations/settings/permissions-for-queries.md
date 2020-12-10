---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "\u0645\u062C\u0648\u0632 \u0628\u0631\u0627\u06CC \u0646\u0645\u0627\u06CC\
  \u0634 \u062F\u0627\u062F\u0647 \u0634\u062F"
---

# مجوز برای نمایش داده شد {#permissions_for_queries}

نمایش داده شد در کلیک خانه را می توان به انواع مختلفی تقسیم شده است:

1.  خواندن نمایش داده شد داده: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  نوشتن نمایش داده شد داده ها: `INSERT`, `OPTIMIZE`.
3.  تغییر پرسوجوی تنظیمات: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) نمایش داده شد: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

تنظیمات زیر تنظیم مجوز کاربر بر اساس نوع پرس و جو:

-   [فقط خواندنی](#settings_readonly) — Restricts permissions for all types of queries except DDL queries.
-   [اجازه دادن به \_نشانی](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` را می توان با هر تنظیمات انجام می شود.

## فقط خواندنی {#settings_readonly}

محدود مجوز برای خواندن داده ها, نوشتن داده ها و تغییر تنظیمات نمایش داده شد.

ببینید که چگونه نمایش داده شد به انواع تقسیم [بالا](#permissions_for_queries).

مقادیر ممکن:

-   0 — All queries are allowed.
-   1 — Only read data queries are allowed.
-   2 — Read data and change settings queries are allowed.

پس از تنظیم `readonly = 1` کاربر نمیتواند تغییر کند `readonly` و `allow_ddl` تنظیمات در جلسه فعلی.

هنگام استفاده از `GET` روش در [رابط قام](../../interfaces/http.md), `readonly = 1` به طور خودکار تنظیم شده است. برای تغییر داده ها از `POST` روش.

تنظیم `readonly = 1` منع کاربر از تغییر تمام تنظیمات. یک راه برای منع کاربر وجود دارد
از تغییر تنظیمات تنها خاص, برای اطلاعات بیشتر ببینید [محدودیت در تنظیمات](constraints-on-settings.md).

مقدار پیشفرض: 0

## اجازه دادن به \_نشانی {#settings_allow_ddl}

اجازه می دهد یا رد می کند [DDL](https://en.wikipedia.org/wiki/Data_definition_language) نمایش داده شد.

ببینید که چگونه نمایش داده شد به انواع تقسیم [بالا](#permissions_for_queries).

مقادیر ممکن:

-   0 — DDL queries are not allowed.
-   1 — DDL queries are allowed.

شما نمی توانید اجرا کنید `SET allow_ddl = 1` اگر `allow_ddl = 0` برای جلسه فعلی.

مقدار پیشفرض: 1

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
