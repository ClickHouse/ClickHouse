---
toc_priority: 54
toc_title: postgresql
---

# postgresql {#postgresql}

Позволяет выполнять заполсы `SELECT` и `INSERT` над таблицами удаленной БД PostgreSQL.

**Синтаксис**

``` sql
postgresql('host:port', 'database', 'table', 'user', 'password')
```

**Аргументы**

-   `host:port` — адрес сервера PostgreSQL.
-   `database` — имя удаленной БД.
-   `table` — таблица удаленной БД.
-   `user` — пользователь PostgreSQL.
-   `password` — пароль пользователя.

**Возвращаемое значение**

Таблица с теми же столбцами, как ив исходной таблице PostgreSQL.

!!! info "Замечения"
    В запросах `INSERT` чтобы различать табличную функцию `postgresql(...)` от таблицы с таким именем и списком колонок, используйте ключевые слова `FUNCTION` или `TABLE FUNCTION`. См. примеры ниже. 

**Примеры**

Рассмотрим таблицу в БД PostgreSQL:

``` sql
postgre> CREATE TABLE IF NOT EXISTS test_table (a integer, b text, c integer)
postgre> INSERT INTO test_table (a, b, c) VALUES (1, 2, 3), (4, 5, 6)
```

Получение данных из ClickHouse:

``` sql
SELECT * FROM postgresql('localhost:5432', 'test_database', 'test_table', 'postgres', 'mysecretpassword');
```

``` text
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 4 │ 5 │ 6 │
└───┴───┴───┘
```

Вставка данных в таблицу PostgreSQL из таблицы ClickHouse:

```sql
INSERT INTO FUNCTION postgresql('localhost:5432', 'test_database', 'test_table', 'postgres', 'mysecretpassword') (a, b, c) VALUES (7, 8, 9);
SELECT * FROM postgresql('localhost:5432', 'test_database', 'test_table', 'postgres', 'mysecretpassword');
```

``` text
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 4 │ 5 │ 6 │
│ 7 │ 8 │ 9 │
└───┴───┴───┘
```

**См. также**

-   [Табличный бвижок PostgreSQL](../../engines/table-engines/integrations/postgresql.md)
-   [Пример подключения PostgreSQL как источника внешнего словаря](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/table-functions/postgresql/) <!--hide-->
