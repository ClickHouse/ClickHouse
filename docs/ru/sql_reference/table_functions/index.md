---
toc_folder_title: Табличные функции
toc_priority: 34
toc_title: Введение
---

# Табличные функции {#tablichnye-funktsii}

Табличные функции — это метод создания таблиц.

Табличные функции можно использовать в:

-   Секции [FROM](../statements/select.md#select-from) запроса `SELECT`.

        Это способ создания временной таблицы, которая доступна только в текущем запросе.

-   Запросе [CREATE TABLE AS \<table\_function()\>](../statements/create.md#create-table-query).

        Это один из методов создания таблицы.

!!! warning "Предупреждение"
    Если настройка [allow\_ddl](../../operations/settings/permissions_for_queries.md#settings_allow_ddl) выключена, то использовать табличные функции невозможно.

| Функция               | Описание                                                                                                                               |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| [file](file.md)       | Создаёт таблицу с движком [File](../../sql_reference/table_functions/index.md).                                                              |
| [merge](merge.md)     | Создаёт таблицу с движком [Merge](../../sql_reference/table_functions/index.md).                                                            |
| [numbers](numbers.md) | Создаёт таблицу с единственным столбцом, заполненным целыми числами.                                                                   |
| [remote](remote.md)   | Предоставляет доступ к удалённым серверам, не создавая таблицу с движком [Distributed](../../sql_reference/table_functions/index.md). |
| [url](url.md)         | Создаёт таблицу с движком [Url](../../sql_reference/table_functions/index.md).                                                                |
| [mysql](mysql.md)     | Создаёт таблицу с движком [MySQL](../../sql_reference/table_functions/index.md).                                                            |
| [jdbc](jdbc.md)       | Создаёт таблицу с дижком [JDBC](../../sql_reference/table_functions/index.md).                                                               |
| [odbc](odbc.md)       | Создаёт таблицу с движком [ODBC](../../sql_reference/table_functions/index.md).                                                              |
| [hdfs](hdfs.md)       | Создаёт таблицу с движком [HDFS](../../sql_reference/table_functions/index.md).                                                              |

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/table_functions/) <!--hide-->
