---
description: 'Документация по движку таблицы MySQL'
sidebar_label: 'MySQL'
sidebar_position: 138
slug: /engines/table-engines/integrations/mysql
title: 'Движок таблицы MySQL'
doc_type: 'reference'
---

Движок MySQL позволяет выполнять запросы `SELECT` и `INSERT` к данным, хранящимся на удалённом сервере MySQL.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = MySQL({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
SETTINGS
    [ connection_pool_size=16, ]
    [ connection_max_tries=3, ]
    [ connection_wait_timeout=5, ]
    [ connection_auto_close=true, ]
    [ connect_timeout=10, ]
    [ read_write_timeout=300, ]
    [ enable_compression=false ]
;
```

См. подробное описание запроса [CREATE TABLE](/ru/sql-reference/statements/create/table).

Структура таблицы может отличаться от структуры исходной таблицы MySQL:

* Имена столбцов должны совпадать с именами в исходной таблице MySQL, но вы можете использовать только часть этих столбцов и в любом порядке.
* Типы столбцов могут отличаться от типов в исходной таблице MySQL. ClickHouse пытается [привести](../../../engines/database-engines/mysql.md#data_types-support) значения к типам данных ClickHouse.
* Настройка [external&#95;table&#95;functions&#95;use&#95;nulls](/ru/operations/settings/settings#external_table_functions_use_nulls) определяет, как обрабатывать столбец с типом Nullable. Значение по умолчанию: 1. Если 0, табличная функция не создает столбцы с типом Nullable и вместо значений NULL выполняет вставку значений по умолчанию. Это также относится к значениям NULL внутри массивов.

**Параметры движка**

* `host:port` — адрес сервера MySQL.
* `database` — имя удаленной базы данных.
* `table` — имя удаленной таблицы или запрос, передаваемый в MySQL как есть (см. [Передача запроса вместо имени таблицы](#passing-a-query)).
* `user` — пользователь MySQL.
* `password` — пароль пользователя.
* `replace_query` — флаг, который преобразует запросы `INSERT INTO` в `REPLACE INTO`. Если `replace_query=1`, запрос заменяется.
* `on_duplicate_clause` — выражение `ON DUPLICATE KEY on_duplicate_clause`, которое добавляется к запросу `INSERT`.
  Пример: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, где `on_duplicate_clause` — это `UPDATE c2 = c2 + 1`. Чтобы узнать, какие значения `on_duplicate_clause` можно использовать с секцией `ON DUPLICATE KEY`, см. [документацию MySQL](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html).
  Чтобы указать `on_duplicate_clause`, необходимо передать `0` в параметр `replace_query`. Если одновременно передать `replace_query = 1` и `on_duplicate_clause`, ClickHouse сгенерирует исключение.

Аргументы также можно передавать с помощью [именованных коллекций](/ru/operations/named-collections.md). В этом случае `host` и `port` должны быть указаны отдельно. Такой подход рекомендуется для продакшн-среды.

Простые условия в секции `WHERE`, такие как `=, !=, >, >=, <, <=`, выполняются на сервере MySQL.

Остальные условия и ограничение сэмплирования `LIMIT` выполняются в ClickHouse только после завершения запроса к MySQL.

<div id="passing-a-query">
  ## Передача запроса вместо имени таблицы
</div>

Вместо имени таблицы аргумент `table` может содержать запрос `SELECT`, который передаётся в MySQL как есть. Структура таблицы определяется по результату запроса. Запрос можно записать либо как подзапрос, либо обернуть в функцию `query`:

```sql
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Это полезно, чтобы перенести выполнение JOIN, агрегаций или любой другой обработки в MySQL. Такая таблица доступна только для чтения: `INSERT` в нее не допускается. Тот же синтаксис поддерживается табличной функцией [`mysql`](/ru/sql-reference/table-functions/mysql).

:::note
Форма с подзапросом `(SELECT ...)` разбирается ClickHouse и повторно сериализуется в диалекте MySQL (с экранированием идентификаторов обратными кавычками) перед отправкой на сервер. Поэтому она должна быть корректным выражением ClickHouse SQL. Чтобы передать синтаксис, специфичный для MySQL и не разбираемый ClickHouse, используйте форму `query('...')`, текст которой отправляется в MySQL дословно.

Любые внешние `WHERE`, `LIMIT`, агрегация и т. д. из окружающего запроса к ClickHouse **не** проталкиваются в переданный запрос — они применяются в ClickHouse после получения полного результата запроса. Чтобы ограничить объем данных, читаемых из MySQL, поместите фильтр внутрь переданного запроса. При [`external_table_strict_query = 1`](/ru/operations/settings/settings#external_table_strict_query) внешний фильтр, который нельзя протолкнуть, отклоняется с исключением вместо локального применения.
:::

Поддерживается несколько реплик, которые должны быть перечислены через `|`. Например:

```sql
CREATE TABLE test_replicas (id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL(`mysql{2|3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

<div id="usage-example">
  ## Пример использования
</div>

Создайте таблицу в MySQL:

```text
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
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Создайте таблицу в ClickHouse, используя простые аргументы:

```sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

Или с помощью [именованных коллекций](/ru/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL(creds, table='test')
```

Получение данных из таблицы MySQL:

```sql
SELECT * FROM mysql_table
```

```text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

<div id="mysql-settings">
  ## Настройки
</div>

Настройки по умолчанию не слишком эффективны, поскольку в них даже не используется повторное использование соединений. Эти настройки позволяют увеличить количество запросов, которые сервер выполняет в секунду.

<div id="connection-auto-close">
  ### `connection_auto_close`
</div>

Позволяет автоматически закрывать соединение после выполнения запроса, то есть отключать повторное использование соединения.

Возможные значения:

* 1 — автоматическое закрытие соединения разрешено, поэтому его повторное использование отключено
* 0 — автоматическое закрытие соединения не разрешено, поэтому его повторное использование включено

Значение по умолчанию: `1`.

<div id="connection-max-tries">
  ### `connection_max_tries`
</div>

Задаёт количество повторных попыток для отказоустойчивого пула.

Возможные значения:

* Положительное целое число.
* 0 — Для отказоустойчивого пула повторные попытки не выполняются.

Значение по умолчанию: `3`.

<div id="connection-pool-size">
  ### `connection_pool_size`
</div>

Размер пула соединений (если все соединения заняты, запрос будет ждать, пока не освободится какое-либо соединение).

Возможные значения:

* Положительное целое число.

Значение по умолчанию: `16`.

<div id="connection-wait-timeout">
  ### `connection_wait_timeout`
</div>

Тайм-аут (в секундах) ожидания свободного соединения (если уже есть `connection_pool_size` активных соединений), 0 — не ждать.

Возможные значения:

* Положительное целое число.

Значение по умолчанию: `5`.

<div id="connect-timeout">
  ### `connect_timeout`
</div>

Тайм-аут подключения (в секундах).

Возможные значения:

* Положительное целое число.

Значение по умолчанию: `10`.

<div id="read-write-timeout">
  ### `read_write_timeout`
</div>

Тайм-аут чтения и записи (в секундах).

Возможные значения:

* Положительное целое число.

Значение по умолчанию: `300`.

<div id="enable-compression">
  ### `enable_compression`
</div>

Включает сжатие для соединения по протоколу MySQL.

Значение по умолчанию: `false`.

Этот параметр применяется к:

* движку таблицы `MySQL`;
* движку базы данных `MySQL`;
* табличной функции `mysql`;
* именованным коллекциям, используемым в интеграциях MySQL.

Когда параметр включён, ClickHouse запрашивает сжатие для соединения.

Пример:

```sql
CREATE TABLE mysql_engine_compression
(
    id UInt32,
    name String,
    age UInt32,
    money UInt32
)
ENGINE = MySQL('mysql80:3306', 'clickhouse', 'test_table', 'root', 'password')
SETTINGS enable_compression = 1;
```

<div id="see-also">
  ## См. также
</div>

* [Табличная функция MySQL](../../../sql-reference/table-functions/mysql.md)
* [Использование MySQL в качестве источника данных для словаря](/ru/sql-reference/statements/create/dictionary/sources/mysql)