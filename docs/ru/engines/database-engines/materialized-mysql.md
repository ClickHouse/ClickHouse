---
toc_priority: 29
---

# [experimental] MaterializedMySQL {#materialized-mysql}

Создает базу данных ClickHouse со всеми таблицами, существующими в MySQL, и всеми данными в этих таблицах.

Сервер ClickHouse работает как реплика MySQL. Он читает файл binlog и выполняет DDL and DML-запросы.

`MaterializedMySQL` — экспериментальный движок баз данных.

## Создание базы данных {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedMySQL('host:port', ['database' | database], 'user', 'password') [SETTINGS ...]
```

**Параметры движка**

-   `host:port` — адрес сервера MySQL.
-   `database` — имя базы данных на удалённом сервере.
-   `user` — пользователь MySQL.
-   `password` — пароль пользователя.

**Настройки движка**

-   `max_rows_in_buffer` — максимальное количество строк, которые могут находиться в кеше в памяти для одной таблицы. Если это значение превышается, то данные материализуются. По умолчанию: `65505`.
-   `max_bytes_in_buffer` — максимальное количество байтов, которые могут находиться в кеше в памяти для одной таблицы. Если это значение превышается, то данные материализуются. По умолчанию: `1048576`.
-   `max_rows_in_buffers` — максимальное количество строк, которые могут находиться в кеше в памяти для всей базы данных. Если это значение превышается, то данные материализуются. По умолчанию: `65505`.
-   `max_bytes_in_buffers` — максимальное количество байтов, которые могут находиться в кеше в памяти для всей базы данных. Если это значение превышается, то данные материализуются. По умолчанию: `1048576`.
-   `max_flush_data_time` — максимальное время в секундах, которое данные могут находиться в кеше в памяти для всей базы данных. Если это значение превышается, то данные материализуются. По умолчанию: `1000`.
-   `max_wait_time_when_mysql_unavailable` — интервал в миллисекундах перед повторным обращением, если сервер MySQL недоступен. Отрицательное значение отключает повторы. По умолчанию: `1000`.
-   `allows_query_when_mysql_lost` — обращаться ли к материализованной таблице, если сервер MySQL недоступен. По умолчанию: `0` (`false`).

**Настройки на стороне сервера MySQL**

Для правильной работы `MaterializeMySQL` на стороне сервера `MySQL` должны быть установлены обязательные настройки:

- `default_authentication_plugin = mysql_native_password` - `MaterializeMySQL` может авторизоваться только с помощью такого метода.
- `gtid_mode = on` - GTID логирование необходимо для работы репликации `MaterializeMySQL`. Обратите внимание, что при включении логирования необходимо также установить настройку `enforce_gtid_consistency = on`.

**Пример**

```
CREATE DATABASE mysql ENGINE = MaterializedMySQL('localhost:3306', 'db', 'user', '***')
     SETTINGS
        allows_query_when_mysql_lost=true,
        max_wait_time_when_mysql_unavailable=10000;
```

## Виртуальные столбцы {#virtual-columns}

При работе с движком баз данных `MaterializedMySQL` используются таблицы семейства [ReplacingMergeTree](../../engines/table-engines/mergetree-family/replacingmergetree.md) с виртуальными столбцами `_sign` и `_version`.

- `_version` — счетчик транзакций. Тип [UInt64](../../sql-reference/data-types/int-uint.md).
- `_sign` — метка удаления. Тип [Int8](../../sql-reference/data-types/int-uint.md). Возможные значения:
    - `1` — строка не удалена,
    - `-1` — строка удалена.

## Поддержка типов данных {#data_types-support}

| MySQL                   | ClickHouse                                                   |
|-------------------------|--------------------------------------------------------------|
| TINY                    | [Int8](../../sql-reference/data-types/int-uint.md)           |
| SHORT                   | [Int16](../../sql-reference/data-types/int-uint.md)          |
| INT24                   | [Int32](../../sql-reference/data-types/int-uint.md)          |
| LONG                    | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| LONGLONG                | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| FLOAT                   | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE                  | [Float64](../../sql-reference/data-types/float.md)           |
| DECIMAL, NEWDECIMAL     | [Decimal](../../sql-reference/data-types/decimal.md)         |
| DATE, NEWDATE           | [Date](../../sql-reference/data-types/date.md)               |
| DATETIME, TIMESTAMP     | [DateTime](../../sql-reference/data-types/datetime.md)       |
| DATETIME2, TIMESTAMP2   | [DateTime64](../../sql-reference/data-types/datetime64.md)   |
| ENUM                    | [Enum](../../sql-reference/data-types/enum.md)               |
| STRING                  | [String](../../sql-reference/data-types/string.md)           |
| VARCHAR, VAR_STRING     | [String](../../sql-reference/data-types/string.md)           |
| BLOB                    | [String](../../sql-reference/data-types/string.md)           |

Другие типы не поддерживаются. Если таблица MySQL содержит столбец другого типа, ClickHouse выдаст исключение "Неподдерживаемый тип данных" ("Unhandled data type") и остановит репликацию.

Тип [Nullable](../../sql-reference/data-types/nullable.md) поддерживается.

## Особенности и рекомендации {#specifics-and-recommendations}

### DDL-запросы {#ddl-queries}

DDL-запросы в MySQL конвертируются в соответствующие DDL-запросы в ClickHouse ([ALTER](../../sql-reference/statements/alter/index.md), [CREATE](../../sql-reference/statements/create/index.md), [DROP](../../sql-reference/statements/drop.md), [RENAME](../../sql-reference/statements/rename.md)). Если ClickHouse не может конвертировать какой-либо DDL-запрос, он его игнорирует.

### Репликация данных {#data-replication}

Данные являются неизменяемыми со стороны пользователя ClickHouse, но автоматически обновляются путём репликации следующих запросов из MySQL:

- Запрос `INSERT` конвертируется в ClickHouse в `INSERT` с `_sign=1`.

- Запрос `DELETE` конвертируется в ClickHouse в `INSERT` с `_sign=-1`.

- Запрос `UPDATE` конвертируется в ClickHouse в `INSERT` с `_sign=-1` и `INSERT` с `_sign=1`.

### Выборка из таблиц движка MaterializedMySQL {#select}

Запрос `SELECT` из таблиц движка `MaterializedMySQL` имеет некоторую специфику:

- Если в запросе `SELECT` напрямую не указан столбец `_version`, то используется модификатор [FINAL](../../sql-reference/statements/select/from.md#select-from-final). Таким образом, выбираются только строки с `MAX(_version)`.

- Если в запросе `SELECT` напрямую не указан столбец `_sign`, то по умолчанию используется `WHERE _sign=1`. Таким образом, удаленные строки не включаются в результирующий набор.

- Результат включает комментарии к столбцам, если они существуют в таблицах базы данных MySQL.

### Конвертация индексов {#index-conversion}

Секции `PRIMARY KEY` и `INDEX` в MySQL конвертируются в кортежи `ORDER BY` в таблицах ClickHouse.

В таблицах ClickHouse данные физически хранятся в том порядке, который определяется секцией `ORDER BY`. Чтобы физически перегруппировать данные, используйте [материализованные представления](../../sql-reference/statements/create/view.md#materialized).

**Примечание**

- Строки с `_sign=-1` физически не удаляются из таблиц.
- Каскадные запросы `UPDATE/DELETE` не поддерживаются движком `MaterializedMySQL`.
- Репликация может быть легко нарушена.
- Прямые операции изменения данных в таблицах и базах данных `MaterializedMySQL` запрещены.
- На работу `MaterializedMySQL` влияет настройка [optimize_on_insert](../../operations/settings/settings.md#optimize-on-insert). Когда таблица на MySQL сервере меняется, происходит слияние данных в соответсвующей таблице в базе данных `MaterializedMySQL`.

## Примеры использования {#examples-of-use}

Запросы в MySQL:

``` sql
mysql> CREATE DATABASE db;
mysql> CREATE TABLE db.test (a INT PRIMARY KEY, b INT);
mysql> INSERT INTO db.test VALUES (1, 11), (2, 22);
mysql> DELETE FROM db.test WHERE a=1;
mysql> ALTER TABLE db.test ADD COLUMN c VARCHAR(16);
mysql> UPDATE db.test SET c='Wow!', b=222;
mysql> SELECT * FROM test;
```

```text
+---+------+------+
| a |    b |    c |
+---+------+------+
| 2 |  222 | Wow! |
+---+------+------+
```

База данных в ClickHouse, обмен данными с сервером MySQL:

База данных и созданная таблица:

``` sql
CREATE DATABASE mysql ENGINE = MaterializedMySQL('localhost:3306', 'db', 'user', '***');
SHOW TABLES FROM mysql;
```

``` text
┌─name─┐
│ test │
└──────┘
```

После вставки данных:

``` sql
SELECT * FROM mysql.test;
```

``` text
┌─a─┬──b─┐
│ 1 │ 11 │
│ 2 │ 22 │
└───┴────┘
```

После удаления данных, добавления столбца и обновления:

``` sql
SELECT * FROM mysql.test;
```

``` text
┌─a─┬───b─┬─c────┐
│ 2 │ 222 │ Wow! │
└───┴─────┴──────┘
```

