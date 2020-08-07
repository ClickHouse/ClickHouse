---
toc_folder_title: "\u0422\u0430\u0431\u043B\u0438\u0447\u043D\u044B\u0435 \u0444\u0443\
  \u043D\u043A\u0446\u0438\u0438"
toc_priority: 34
toc_title: "\u0412\u0432\u0435\u0434\u0435\u043D\u0438\u0435"
---

# Табличные функции {#table-functions}

Табличные функции — это метод создания таблиц.

Табличные функции можно использовать в:

-   Секции [FROM](../statements/select/from.md#select-from) запроса `SELECT`.

        Это способ создания временной таблицы, которая доступна только в текущем запросе.

-   Запросе [CREATE TABLE AS \<table\_function()\>](../statements/create.md#create-table-query).

        Это один из методов создания таблицы.

!!! warning "Предупреждение"
    Если настройка [allow\_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) выключена, то использовать табличные функции невозможно.

| Функция               | Описание                                                                                                                              |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| [file](file.md)       | Создаёт таблицу с движком [File](../../sql-reference/table-functions/index.md).                                                       |
| [merge](merge.md)     | Создаёт таблицу с движком [Merge](../../sql-reference/table-functions/index.md).                                                      |
| [numbers](numbers.md) | Создаёт таблицу с единственным столбцом, заполненным целыми числами.                                                                  |
| [remote](remote.md)   | Предоставляет доступ к удалённым серверам, не создавая таблицу с движком [Distributed](../../sql-reference/table-functions/index.md). |
| [url](url.md)         | Создаёт таблицу с движком [Url](../../sql-reference/table-functions/index.md).                                                        |
| [mysql](mysql.md)     | Создаёт таблицу с движком [MySQL](../../sql-reference/table-functions/index.md).                                                      |
| [jdbc](jdbc.md)       | Создаёт таблицу с дижком [JDBC](../../sql-reference/table-functions/index.md).                                                        |
| [odbc](odbc.md)       | Создаёт таблицу с движком [ODBC](../../sql-reference/table-functions/index.md).                                                       |
| [hdfs](hdfs.md)       | Создаёт таблицу с движком [HDFS](../../sql-reference/table-functions/index.md).                                                       |

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/table_functions/) <!--hide-->
