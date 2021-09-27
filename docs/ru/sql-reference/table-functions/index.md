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

-   Запросе [CREATE TABLE AS \<table_function()\>](../statements/create/index.md#create-table-query).

        Это один из методов создания таблицы.

!!! warning "Предупреждение"
    Если настройка [allow_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) выключена, то использовать табличные функции невозможно.

| Функция               | Описание                                                                                                                              |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| [file](file.md)       | Создаёт таблицу с движком [File](../../engines/table-engines/special/file.md).                                                       |
| [merge](merge.md)     | Создаёт таблицу с движком [Merge](../../engines/table-engines/special/merge.md).                                                      |
| [numbers](numbers.md) | Создаёт таблицу с единственным столбцом, заполненным целыми числами.                                                                  |
| [remote](remote.md)   | Предоставляет доступ к удалённым серверам, не создавая таблицу с движком [Distributed](../../engines/table-engines/special/distributed.md). |
| [url](url.md)         | Создаёт таблицу с движком [Url](../../engines/table-engines/special/url.md).                                                        |
| [mysql](mysql.md)     | Создаёт таблицу с движком [MySQL](../../engines/table-engines/integrations/mysql.md).                                                      |
| [jdbc](jdbc.md)       | Создаёт таблицу с дижком [JDBC](../../engines/table-engines/integrations/jdbc.md).                                                        |
| [odbc](odbc.md)       | Создаёт таблицу с движком [ODBC](../../engines/table-engines/integrations/odbc.md).                                                       |
| [hdfs](hdfs.md)       | Создаёт таблицу с движком [HDFS](../../engines/table-engines/integrations/hdfs.md).                                                       |

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/table_functions/) <!--hide-->
