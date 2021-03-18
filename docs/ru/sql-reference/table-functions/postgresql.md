---
toc_priority: 42
toc_title: postgresql
---

# postgresql {#postgresql}

Позволяет выполнять запросы `SELECT` над данными, хранящимися на удалённом PostgreSQL сервере.

**Синтаксис**
``` sql
postgresql('host:port', 'database', 'table', 'user', 'password')
```

**Параметры**

-   `host:port` — адрес сервера PostgreSQL.

-   `database` — имя базы данных на удалённом сервере.

-   `table` — имя таблицы на удалённом сервере.

-   `user` — пользователь PostgreSQL.

-   `password` — пароль пользователя.


SELECT запросы на стороне PostgreSQL выполняются как `COPY (SELECT ...) TO STDOUT` внутри транзакции PostgreSQL только на чтение  с коммитом после каждого `SELECT` запроса.

Простые условия для `WHERE` такие как `=, !=, >, >=, <, <=, IN` исполняются на стороне PostgreSQL сервера.

Все операции объединения, аггрегации, сортировки, условия `IN [ array ]` и ограничения `LIMIT` выполняются на стороне ClickHouse только после того как запрос к PostgreSQL закончился.

INSERT запросы на стороне PostgreSQL выполняются как `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` внутри PostgreSQL транзакции с автоматическим коммитом после каждого `INSERT` запроса.

PostgreSQL массивы конвертируются в массивы ClickHouse.
Будьте осторожны в PostgreSQL массивы созданные как type_name[], являются многомерными и могут содержать в себе разное количество измерений в разных строках одной таблицы, внутри ClickHouse допустипы только многомерные массивы с одинаковым кол-вом измерений во всех строках таблицы.

**Возвращаемое значение**

Объект таблицы с теми же столбцами, что и в исходной таблице PostgreSQL.

!!! info "Примечание"
В запросах `INSERT` для того чтобы отличить табличную функцию `postgresql(...)` от таблицы со списком имен столбцов вы должны указывать ключевые слова `FUNCTION` или `TABLE FUNCTION`. See examples below.

**Примеры**

Таблица в PostgreSQL:

``` text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# insert into test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> select * from test;
 int_id | int_nullable | float | str  | float_nullable
--------+--------------+-------+------+----------------
      1 |              |     2 | test |
(1 row)
```

Получение данных в ClickHouse:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test'); 
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Вставка:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

**Смотрите также**

-   [Движок таблиц ‘PostgreSQL’](../../sql-reference/table-functions/postgresql.md)
-   [Использование PostgreSQL как источника данных для внешнего словаря](../../sql-reference/table-functions/postgresql.md#dicts-external_dicts_dict_sources-postgresql)
