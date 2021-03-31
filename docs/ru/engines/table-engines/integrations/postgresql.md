---
toc_priority: 8
toc_title: PostgreSQL
---

# PosgtreSQL {#postgresql}

Движок PostgreSQL позволяет выполнять запросы `SELECT` над данными, хранящимися на удалённом PostgreSQL сервере.

## Создание таблицы {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = PostgreSQL('host:port', 'database', 'table', 'user', 'password');
```

Смотрите подробное описание запроса [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query).

Структура таблицы может отличаться от исходной структуры таблицы PostgreSQL:

-   Имена столбцов должны быть такими же, как в исходной таблице MySQL, но вы можете использовать только некоторые из этих столбцов и в любом порядке.
-   Типы столбцов могут отличаться от типов в исходной таблице PostgreSQL. ClickHouse пытается [приводить](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) values to the ClickHouse data types.
-   Настройка `external_table_functions_use_nulls` определяет как обрабатывать Nullable столбцы. По умолчанию 1, если 0 - табличная функция не будет делать nullable столбцы и будет вместо null выставлять значения по умолчанию для скалярного типа. Это также применимо для null значений внутри массивов.

**Параметры движка**

-   `host:port` — адрес сервера PostgreSQL.

-   `database` — Имя базы данных на сервере PostgreSQL.

-   `table` — Имя таблицы.

-   `user` — Имя пользователя PostgreSQL.

-   `password` — Пароль пользователя PostgreSQL.

SELECT запросы на стороне PostgreSQL выполняются как `COPY (SELECT ...) TO STDOUT` внутри транзакции PostgreSQL только на чтение  с коммитом после каждого `SELECT` запроса.

Простые условия для `WHERE` такие как `=, !=, >, >=, <, <=, IN` исполняются на стороне PostgreSQL сервера.

Все операции объединения, аггрегации, сортировки, условия `IN [ array ]` и ограничения `LIMIT` выполняются на стороне ClickHouse только после того как запрос к PostgreSQL закончился.

INSERT запросы на стороне PostgreSQL выполняются как `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` внутри PostgreSQL транзакции с автоматическим коммитом после каждого `INSERT` запроса.

PostgreSQL массивы конвертируются в массивы ClickHouse.
Будьте осторожны в PostgreSQL массивы созданные как type_name[], являются многомерными и могут содержать в себе разное количество измерений в разных строках одной таблицы, внутри ClickHouse допустипы только многомерные массивы с одинаковым кол-вом измерений во всех строках таблицы.

## Пример использования {#usage-example}

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

Таблица в ClickHouse, получение данных из PostgreSQL таблицы созданной выше:

``` sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
```

``` sql
SELECT * FROM postgresql_table WHERE str IN ('test') 
```

``` text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
1 rows in set. Elapsed: 0.019 sec.
```


## Смотри также {#see-also}

-   [Табличная функция ‘postgresql’](../../../sql-reference/table-functions/postgresql.md)
-   [Использование PostgreSQL в качестве истояника для внешнего словаря](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

