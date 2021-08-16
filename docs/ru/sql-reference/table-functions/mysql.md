---
toc_priority: 42
toc_title: mysql
---

# mysql {#mysql}

Позволяет выполнять запросы `SELECT` и `INSERT` над данными, хранящимися на удалённом MySQL сервере.

**Синтаксис**

``` sql
mysql('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause'])
```

**Аргументы**

-   `host:port` — адрес сервера MySQL.

-   `database` — имя базы данных на удалённом сервере.

-   `table` — имя таблицы на удалённом сервере.

-   `user` — пользователь MySQL.

-   `password` — пароль пользователя.

-   `replace_query` — флаг, отвечающий за преобразование запросов `INSERT INTO` в `REPLACE INTO`. Возможные значения:
    - `0` - выполняется запрос `INSERT INTO`.
    - `1` - выполняется запрос `REPLACE INTO`.

-   `on_duplicate_clause` — выражение `ON DUPLICATE KEY on_duplicate_clause`, добавляемое в запрос `INSERT`. Может быть передано только с помощью  `replace_query = 0` (если вы одновременно передадите `replace_query = 1` и `on_duplicate_clause`, будет сгенерировано исключение).

        Пример: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, где `on_duplicate_clause` это `UPDATE c2 = c2 + 1`.
        Выражения, которые могут использоваться в качестве `on_duplicate_clause` в секции `ON DUPLICATE KEY`, можно посмотреть в документации по [MySQL](http://www.mysql.ru/docs/).

Простые условия `WHERE` такие как `=, !=, >, >=, <, =` выполняются на стороне сервера MySQL.

Остальные условия и ограничение выборки `LIMIT` будут выполнены в ClickHouse только после выполнения запроса к MySQL.

Поддерживает несколько реплик, которые должны быть перечислены через `|`. Например:

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

или

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

**Возвращаемое значение**

Объект таблицы с теми же столбцами, что и в исходной таблице MySQL.

!!! note "Примечание"
    Чтобы отличить табличную функцию `mysql (...)` в запросе `INSERT` от имени таблицы со списком столбцов, используйте ключевые слова `FUNCTION` или `TABLE FUNCTION`. См. примеры ниже.

**Примеры**

Таблица в MySQL:

``` text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));

mysql> INSERT INTO test (`int_id`, `float`) VALUES (1,2);

mysql> SELECT * FROM test;
+--------+-------+
| int_id | float |
+--------+-------+
|      1 |     2 |
+--------+-------+
```

Получение данных в ClickHouse:

``` sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

``` text
┌─int_id─┬─float─┐
│      1 │     2 │
└────────┴───────┘
```

Замена и вставка:

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

``` text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

**Смотрите также**

-   [Движок таблиц ‘MySQL’](../../sql-reference/table-functions/mysql.md)
-   [Использование MySQL как источника данных для внешнего словаря](../../sql-reference/table-functions/mysql.md#dicts-external_dicts_dict_sources-mysql)

