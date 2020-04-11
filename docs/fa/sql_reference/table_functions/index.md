---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_folder_title: Table Functions
toc_priority: 34
toc_title: "\u0645\u0639\u0631\u0641\u06CC \u0634\u0631\u06A9\u062A"
---

# توابع جدول {#table-functions}

توابع جدول روش برای ساخت جداول.

شما می توانید توابع جدول در استفاده:

-   [FROM](../statements/select.md#select-from) بند از `SELECT` پرس و جو.

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [ایجاد جدول به عنوان \<table\_function()\>](../statements/create.md#create-table-query) پرس و جو.

        It's one of the methods of creating a table.

!!! warning "اخطار"
    شما می توانید توابع جدول اگر استفاده نمی [اجازه دادن به \_نشانی](../../operations/settings/permissions_for_queries.md#settings_allow_ddl) تنظیم غیر فعال است.

| تابع                  | توصیف                                                                                                                                          |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| [پرونده](file.md)     | ایجاد یک [پرونده](../../engines/table_engines/special/file.md)- جدول موتور.                                                                    |
| [ادغام](merge.md)     | ایجاد یک [ادغام](../../engines/table_engines/special/merge.md)- جدول موتور.                                                                    |
| [اعداد](numbers.md)   | ایجاد یک جدول با یک ستون پر از اعداد صحیح.                                                                                                     |
| [دور](remote.md)      | اجازه می دهد تا شما را به دسترسی به سرور از راه دور بدون ایجاد یک [توزیع شده](../../engines/table_engines/special/distributed.md)- جدول موتور. |
| [نشانی وب](url.md)    | ایجاد یک [نشانی وب](../../engines/table_engines/special/url.md)- جدول موتور.                                                                   |
| [خروجی زیر](mysql.md) | ایجاد یک [MySQL](../../engines/table_engines/integrations/mysql.md)- جدول موتور.                                                               |
| [جستجو](jdbc.md)      | ایجاد یک [JDBC](../../engines/table_engines/integrations/jdbc.md)- جدول موتور.                                                                 |
| [جستجو](odbc.md)      | ایجاد یک [ODBC](../../engines/table_engines/integrations/odbc.md)- جدول موتور.                                                                 |
| [hdfs](hdfs.md)       | ایجاد یک [HDFS](../../engines/table_engines/integrations/hdfs.md)- جدول موتور.                                                                 |

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/) <!--hide-->
