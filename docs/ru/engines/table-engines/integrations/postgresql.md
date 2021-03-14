---
toc_priority: 11
toc_title: PostgreSQL
---

#PostgreSQL {#postgresql}

Движок PostgreSQL позволяет выполнять запросы `SELECT` и `INSERT` для таблиц на удаленном сервере PostgreSQL.

## Создание таблицы {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = PostgreSQL('host:port', 'database', 'table', 'user', 'password');
```

См. подробное описание запроса [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query).

**Параметры движка**

-   `host:port` — адрес сервера MySQL.

-   `database` — имя удаленной БД.

-   `table` — имя удаленной таблицы БД.

-   `user` — пользователь MySQL.

-   `password` — пароль пользователя.

## Примеры использования {#usage-example}

Рассмотрим таблицу ClickHouse, которая получает данные из таблицы PostgreSQL:

``` sql
CREATE TABLE test_table
(
    `int_id` Int32,
    'value' Int32
)
ENGINE = PostgreSQL('localhost:5432', 'test_database', 'test_table', 'postgres', 'mysecretpassword');
```

``` sql
SELECT * FROM test_database.test_table;
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

Добавление данных из таблицы ClickHouse в таблицу PosegreSQL:

``` sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

## См. также {#see-also}

-   [Функция 'postgresql'](../../../sql-reference/table-functions/postgresql.md)
-   [Пример подключения PostgreSQL как источника внешнего словаря](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

[Оригинальная статья](https://clickhouse.tech/docs/en/operations/table-engines/integrations/postgresql/) <!--hide-->
