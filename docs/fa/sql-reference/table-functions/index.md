---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u062A\u0648\u0627\u0628\u0639 \u062C\u062F\u0648\u0644"
toc_priority: 34
toc_title: "\u0645\u0639\u0631\u0641\u06CC \u0634\u0631\u06A9\u062A"
---

# توابع جدول {#table-functions}

توابع جدول روش برای ساخت جداول.

شما می توانید توابع جدول در استفاده:

-   [FROM](../statements/select/from.md) بند از `SELECT` پرس و جو.

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [ایجاد جدول به عنوان \<table_function()\>](../statements/create.md#create-table-query) پرس و جو.

        It's one of the methods of creating a table.

!!! warning "اخطار"
    شما می توانید توابع جدول اگر استفاده نمی [اجازه دادن به _نشانی](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) تنظیم غیر فعال است.

| تابع                  | توصیف                                                                                                                                          |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| [پرونده](file.md)     | ایجاد یک [پرونده](../../engines/table-engines/special/file.md)- جدول موتور.                                                                    |
| [ادغام](merge.md)     | ایجاد یک [ادغام](../../engines/table-engines/special/merge.md)- جدول موتور.                                                                    |
| [اعداد](numbers.md)   | ایجاد یک جدول با یک ستون پر از اعداد صحیح.                                                                                                     |
| [دور](remote.md)      | اجازه می دهد تا شما را به دسترسی به سرور از راه دور بدون ایجاد یک [توزیع شده](../../engines/table-engines/special/distributed.md)- جدول موتور. |
| [نشانی وب](url.md)    | ایجاد یک [نشانی وب](../../engines/table-engines/special/url.md)- جدول موتور.                                                                   |
| [خروجی زیر](mysql.md) | ایجاد یک [MySQL](../../engines/table-engines/integrations/mysql.md)- جدول موتور.                                                               |
| [جستجو](jdbc.md)      | ایجاد یک [JDBC](../../engines/table-engines/integrations/jdbc.md)- جدول موتور.                                                                 |
| [جستجو](odbc.md)      | ایجاد یک [ODBC](../../engines/table-engines/integrations/odbc.md)- جدول موتور.                                                                 |
| [hdfs](hdfs.md)       | ایجاد یک [HDFS](../../engines/table-engines/integrations/hdfs.md)- جدول موتور.                                                                 |

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/) <!--hide-->
