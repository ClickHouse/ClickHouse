# MySQL {#mysql}

Движок MySQL позволяет выполнять запросы `SELECT` над данными, хранящимися на удалённом MySQL сервере.

## Создание таблицы {#sozdanie-tablitsy}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

Смотрите подробное описание запроса [CREATE TABLE](../../../engines/table-engines/integrations/mysql.md#create-table-query).

Структура таблицы может отличаться от исходной структуры таблицы MySQL:

-   Имена столбцов должны быть такими же, как в исходной таблице MySQL, но вы можете использовать только некоторые из этих столбцов и в любом порядке.
-   Типы столбцов могут отличаться от типов в исходной таблице MySQL. ClickHouse пытается [приводить](../../../engines/table-engines/integrations/mysql.md#type_conversion_function-cast) значения к типам данных ClickHouse.

**Параметры движка**

-   `host:port` — адрес сервера MySQL.

-   `database` — имя базы данных на удалённом сервере.

-   `table` — имя таблицы на удалённом сервере.

-   `user` — пользователь MySQL.

-   `password` — пароль пользователя.

-   `replace_query` — флаг, отвечающий за преобразование запросов `INSERT INTO` в `REPLACE INTO`. Если `replace_query=1`, то запрос заменяется.

-   `on_duplicate_clause` — выражение `ON DUPLICATE KEY on_duplicate_clause`, добавляемое к запросу `INSERT`.

        Пример: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, где `on_duplicate_clause` это `UPDATE c2 = c2 + 1`. Чтобы узнать какие `on_duplicate_clause` можно использовать с секцией `ON DUPLICATE KEY`  обратитесь к [документации MySQL](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html).

        Чтобы указать `on_duplicate_clause` необходимо передать `0` в параметр `replace_query`. Если одновременно передать `replace_query = 1` и `on_duplicate_clause`, то ClickHouse сгенерирует исключение.

Простые условия `WHERE` такие как `=, !=, >, >=, <, =` выполняются на стороне сервера MySQL.

Остальные условия и ограничение выборки `LIMIT` будут выполнены в ClickHouse только после выполнения запроса к MySQL.

## Пример использования {#primer-ispolzovaniia}

Таблица в MySQL:

``` text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+--------+--------------+-------+----------------+
| int_id | int_nullable | float | float_nullable |
+--------+--------------+-------+----------------+
|      1 |         NULL |     2 |           NULL |
+--------+--------------+-------+----------------+
1 row in set (0,00 sec)
```

Таблица в ClickHouse, которая получает данные из созданной ранее таблицы MySQL:

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## Смотрите также {#smotrite-takzhe}

-   [Табличная функция ‘mysql’](../../../engines/table-engines/integrations/mysql.md)
-   [Использование MySQL в качестве источника для внешнего словаря](../../../engines/table-engines/integrations/mysql.md#dicts-external_dicts_dict_sources-mysql)

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/mysql/) <!--hide-->
