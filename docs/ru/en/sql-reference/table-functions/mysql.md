---
description: 'Позволяет выполнять запросы `SELECT` и `INSERT` с данными, хранящимися
  на удалённом сервере MySQL.'
sidebar_label: 'mysql'
sidebar_position: 137
slug: /sql-reference/table-functions/mysql
title: 'mysql'
doc_type: 'reference'
---

Позволяет выполнять запросы `SELECT` и `INSERT` с данными, хранящимися на удалённом сервере MySQL.

<div id="syntax">
  ## Синтаксис
</div>

```sql
mysql({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## Аргументы
</div>

| Аргумент              | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host:port`           | Адрес сервера MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `database`            | Имя удалённой базы данных.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `table`               | Имя удалённой таблицы или запрос, передаваемый в MySQL как есть (см. [Передача запроса вместо имени таблицы](#passing-a-query)).                                                                                                                                                                                                                                                                                                                                                                                                           |
| `user`                | Пользователь MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `password`            | Пароль пользователя.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `replace_query`       | Флаг, преобразующий запросы `INSERT INTO` в `REPLACE INTO`. Возможные значения:<br />    - `0` — запрос выполняется как `INSERT INTO`.<br />    - `1` — запрос выполняется как `REPLACE INTO`.                                                                                                                                                                                                                                                                                                                                             |
| `on_duplicate_clause` | Выражение `ON DUPLICATE KEY on_duplicate_clause`, добавляемое к запросу `INSERT`. Может быть указано только при `replace_query = 0` (если одновременно передать `replace_query = 1` и `on_duplicate_clause`, ClickHouse генерирует исключение).<br />    Пример: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1;`<br />    Здесь `on_duplicate_clause` — это `UPDATE c2 = c2 + 1`. О том, какие значения `on_duplicate_clause` можно использовать с предложением `ON DUPLICATE KEY`, см. в документации MySQL. |

Аргументы также можно передавать с помощью [именованных коллекций](/ru/operations/named-collections.md). В этом случае `host` и `port` нужно указывать отдельно. Этот подход рекомендуется для продакшн-окружения.

Простые предложения `WHERE`, такие как `=, !=, >, >=, <, <=`, в настоящее время выполняются на сервере MySQL.

Остальные условия и ограничение `LIMIT` для сэмплирования выполняются в ClickHouse только после завершения запроса к MySQL.

<div id="passing-a-query">
  ## Передача запроса вместо имени таблицы
</div>

Вместо имени таблицы в качестве третьего аргумента можно указать запрос `SELECT`, который передаётся в MySQL как есть. Структура результирующей таблицы автоматически определяется по результату запроса. Запрос можно записать либо как подзапрос, либо обернуть в функцию `query`:

```sql
SELECT * FROM mysql('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM mysql('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Это полезно для проталкивания JOIN, агрегаций и любой другой обработки в MySQL. Такая таблица доступна только в режиме только для чтения: `INSERT` в неё не допускается. Тот же синтаксис поддерживается движком таблицы [`MySQL`](/ru/engines/table-engines/integrations/mysql).

:::note
Форма с подзапросом `(SELECT ...)` разбирается ClickHouse и перед отправкой на сервер повторно сериализуется в диалекте MySQL (с экранированием идентификаторов обратными кавычками). Поэтому она должна быть корректным ClickHouse SQL. Чтобы передать специфичный для MySQL синтаксис, который ClickHouse не разбирает, используйте форму `query('...')`, текст которой отправляется в MySQL дословно.

Любые внешние `предложение WHERE`, `LIMIT`, агрегации и т. д. из окружающего запроса к ClickHouse **не** проталкиваются в переданный запрос — они применяются в ClickHouse после получения полного результата запроса. Чтобы ограничить объём данных, считываемых из MySQL, поместите фильтр внутрь переданного запроса. При [`external_table_strict_query = 1`](/ru/operations/settings/settings#external_table_strict_query) внешний фильтр, который нельзя протолкнуть, отклоняется с исключением вместо локального применения.
:::

Поддерживается несколько реплик, которые должны быть перечислены через `|`. Например:

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

или

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

<div id="returned_value">
  ## Возвращаемое значение
</div>

Табличный объект с теми же столбцами, что и исходная таблица MySQL.

:::note
Некоторые типы данных MySQL могут сопоставляться с разными типами ClickHouse — это задаётся настройкой уровня запроса [mysql&#95;datatypes&#95;support&#95;level](/ru/operations/settings/settings.md#mysql_datatypes_support_level)
:::

:::note
В запросе `INSERT`, чтобы отличить табличную функцию `mysql(...)` от имени таблицы со списком имён столбцов, необходимо использовать ключевые слова `FUNCTION` или `TABLE FUNCTION`. См. примеры ниже.
:::

<div id="examples">
  ## Примеры
</div>

Таблица в MySQL:

```text
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

Выборка данных из ClickHouse:

```sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

Или с помощью [именованных коллекций](/ru/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
SELECT * FROM mysql(creds, table='test');
```

```text
┌─int_id─┬─float─┐
│      1 │     2 │
└────────┴───────┘
```

<div id="enable-compression">
  ### `enable_compression`
</div>

Включает сжатие для подключения по протоколу MySQL.

Значение по умолчанию: `false`.

Этот параметр применяется к:

* табличной функции `mysql`;
* движку таблицы `MySQL`;
* движку базы данных `MySQL`;
* именованным коллекциям, используемым в интеграциях MySQL.

Когда параметр включен, ClickHouse запрашивает сжатие для подключения.

Пример:

```sql
SELECT *
FROM mysql(
    'mysql80:3306',
    'clickhouse',
    'test_table',
    'root',
    'password',
    SETTINGS enable_compression = 1
);
```

Замена и вставка:

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

```text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

Копирование данных из таблицы MySQL в таблицу ClickHouse:

```sql
CREATE TABLE mysql_copy
(
   `id` UInt64,
   `datetime` DateTime('UTC'),
   `description` String,
)
ENGINE = MergeTree
ORDER BY (id,datetime);

INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password');
```

Или, если копируется только инкрементальный батч из MySQL на основе текущего максимального значения id:

```sql
INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password')
WHERE id > (SELECT max(id) FROM mysql_copy);
```

<div id="related">
  ## Связанные материалы
</div>

* [Движок таблицы &#39;MySQL&#39;](../../engines/table-engines/integrations/mysql.md)
* [Использование MySQL в качестве источника словаря](/ru/sql-reference/statements/create/dictionary/sources/mysql)
* [mysql&#95;datatypes&#95;support&#95;level](/ru/operations/settings/settings.md#mysql_datatypes_support_level)
* [mysql&#95;map&#95;fixed&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/ru/operations/settings/settings.md#mysql_map_fixed_string_to_text_in_show_columns)
* [mysql&#95;map&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/ru/operations/settings/settings.md#mysql_map_string_to_text_in_show_columns)
* [mysql&#95;max&#95;rows&#95;to&#95;insert](/ru/operations/settings/settings.md#mysql_max_rows_to_insert)