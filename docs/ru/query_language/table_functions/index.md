# Табличные функции

Табличные функции — это метод создания таблиц.

Табличные функции можно использовать в:

* Секции [FROM](../select.md#select-from) запроса `SELECT`.

    Это способ создания временной таблицы, которая доступна только в текущем запросе.

* Запросе [CREATE TABLE AS <table_function()>](../create.md#create-table-query).

    Это один из методов создания таблицы.

!!! warning "Предупреждение"
    Если настройка [allow_ddl](../../operations/settings/permissions_for_queries.md#settings_allow_ddl) выключена, то использовать табличные функции невозможно.

Функция  | Описание
---------|------------
[file](file.md) | Создаёт таблицу с движком [File](../../operations/table_engines/file.md).
[merge](merge.md) | Создаёт таблицу с движком [Merge](../../operations/table_engines/merge.md).
[numbers](numbers.md) | Создаёт таблицу с единственным столбцом, заполненным целыми числами.
[remote](remote.md) | Предоставляет доступ к удалённым серверам, не создавая таблицу с движком [Distributed](../../operations/table_engines/distributed.md).
[url](url.md) | Создаёт таблицу с движком [Url](../../operations/table_engines/url.md).
[mysql](mysql.md) | Создаёт таблицу с движком [MySQL](../../operations/table_engines/mysql.md).
[jdbc](jdbc.md) | Создаёт таблицу с дижком [JDBC](../../operations/table_engines/jdbc.md).
[odbc](odbc.md) | Создаёт таблицу с движком [ODBC](../../operations/table_engines/odbc.md).
[hdfs](hdfs.md) | Создаёт таблицу с движком [HDFS](../../operations/table_engines/hdfs.md).

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/table_functions/) <!--hide-->
