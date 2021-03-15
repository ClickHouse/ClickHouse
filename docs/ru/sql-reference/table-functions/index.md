---
toc_folder_title: "Табличные функции"
toc_priority: 34
toc_title: "Введение"
---

# Табличные функции {#table-functions}

Табличные функции — это метод создания таблиц.

Табличные функции можно использовать в:

-   Секции [FROM](../statements/select/from.md#select-from) запроса `SELECT`.

        Это способ создания временной таблицы, которая доступна только в текущем запросе.

-   Запросе [CREATE TABLE AS \<table_function()\>](../statements/create/index.md#create-table-query).

        Это один из методов создания таблицы.

!!! warning "Предупреждение"
    Если настройка [allow_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) выключена, то использовать табличные функции невозможно.

| Функция               | Описание                                                                                                                              |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| [file](../../sql-reference/table-functions/file.md)             | Создаёт таблицу с движком [File](../../engines/table-engines/special/file.md).                                                              |
| [merge](../../sql-reference/table-functions/merge.md)           | Создаёт таблицу с движком [Merge](../../engines/table-engines/special/merge.md).                                                            |
| [numbers](../../sql-reference/table-functions/numbers.md)       | Создаёт таблицу с единственным столбцом, заполненным целыми числами.                                                                        |
| [remote](../../sql-reference/table-functions/remote.md)         | Предоставляет доступ к удалённым серверам, не создавая таблицу с движком [Distributed](../../engines/table-engines/special/distributed.md). |
| [url](../../sql-reference/table-functions/url.md)               | Создаёт таблицу с движком [Url](../../engines/table-engines/special/url.md).                                                                |
| [mysql](../../sql-reference/table-functions/mysql.md)           | Создаёт таблицу с движком [MySQL](../../engines/table-engines/integrations/mysql.md).                                                       |
| [postgresql](../../sql-reference/table-functions/postgresql.md) | Создаёт таблицу с движком [PostgreSQL](../../engines/table-engines/integrations/postgresql.md).                                             |
| [jdbc](../../sql-reference/table-functions/jdbc.md)             | Создаёт таблицу с движком [JDBC](../../engines/table-engines/integrations/jdbc.md).                                                         |
| [odbc](../../sql-reference/table-functions/odbc.md)             | Создаёт таблицу с движком [ODBC](../../engines/table-engines/integrations/odbc.md).                                                         |
| [hdfs](../../sql-reference/table-functions/hdfs.md)             | Создаёт таблицу с движком [HDFS](../../engines/table-engines/integrations/hdfs.md).                                                         |
| [s3](../../sql-reference/table-functions/s3.md)                 | Создаёт таблицу с движком [S3](../../engines/table-engines/integrations/s3.md).                                                            |

