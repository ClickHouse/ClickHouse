# mysql {#mysql}

Позволяет выполнять запросы `SELECT` над данными, хранящимися на удалённом MySQL сервере.

``` sql
mysql('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

**Параметры**

-   `host:port` — адрес сервера MySQL.

-   `database` — имя базы данных на удалённом сервере.

-   `table` — имя таблицы на удалённом сервере.

-   `user` — пользователь MySQL.

-   `password` — пароль пользователя.

-   `replace_query` — флаг, отвечающий за преобразование запросов `INSERT INTO` в `REPLACE INTO`. Если `replace_query=1`, то запрос заменяется.

-   `on_duplicate_clause` — выражение `ON DUPLICATE KEY on_duplicate_clause`, добавляемое в запрос `INSERT`.

        Пример: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, где `on_duplicate_clause` это `UPDATE c2 = c2 + 1`. Чтобы узнать какие `on_duplicate_clause` можно использовать с секцией `ON DUPLICATE KEY`  обратитесь к документации MySQL.

        Чтобы указать `'on_duplicate_clause'` необходимо передать `0` в параметр `replace_query`. Если одновременно передать `replace_query = 1` и `'on_duplicate_clause'`, то ClickHouse сгенерирует исключение.

Простые условия `WHERE` такие как `=, !=, >, >=, <, =` выполняются на стороне сервера MySQL.

Остальные условия и ограничение выборки `LIMIT` будут выполнены в ClickHouse только после выполнения запроса к MySQL.

**Возвращаемое значение**

Объект таблицы с теми же столбцами, что и в исходной таблице MySQL.

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

Получение данных в ClickHouse:

``` sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

## Смотрите также {#smotrite-takzhe}

-   [Движок таблиц ‘MySQL’](../../sql-reference/table-functions/mysql.md)
-   [Использование MySQL как источника данных для внешнего словаря](../../sql-reference/table-functions/mysql.md#dicts-external_dicts_dict_sources-mysql)

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/table_functions/mysql/) <!--hide-->
