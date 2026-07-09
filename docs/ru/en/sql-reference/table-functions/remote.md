---
description: 'Табличная функция `remote` позволяет обращаться к удалённым серверам на лету,
  то есть без создания [Distributed](../../engines/table-engines/special/distributed.md) таблицы. Табличная функция `remoteSecure` работает так же,
  как `remote`, но использует защищённое соединение.'
sidebar_label: 'remote'
sidebar_position: 175
slug: /sql-reference/table-functions/remote
title: 'remote, remoteSecure'
doc_type: 'reference'
---

Табличная функция `remote` позволяет обращаться к удалённым серверам на лету, то есть без создания [Distributed](../../engines/table-engines/special/distributed.md) таблицы. Табличная функция `remoteSecure` работает так же, как `remote`, но использует защищённое соединение.

Обе функции можно использовать в запросах `SELECT` и `INSERT`.

<div id="syntax">
  ## Синтаксис
</div>

```sql
remote(addresses_expr, [db, table, user [, password], sharding_key])
remote(addresses_expr, [db.table, user [, password], sharding_key])
remote(named_collection[, option=value [,..]])
remoteSecure(addresses_expr, [db, table, user [, password], sharding_key])
remoteSecure(addresses_expr, [db.table, user [, password], sharding_key])
remoteSecure(named_collection[, option=value [,..]])
```

<div id="parameters">
  ## Параметры
</div>

| Аргумент         | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `addresses_expr` | Адрес удалённого сервера или выражение, генерирующее несколько адресов удалённых серверов. Формат: `host` или `host:port`.<br /><br />    `host` можно указать как имя сервера, IPv4-адрес или IPv6-адрес. IPv6-адрес должен быть заключён в `[]`.<br /><br />    `port` — TCP-порт на удалённом сервере. Если порт не указан, используется [tcp&#95;port](../../operations/server-configuration-parameters/settings.md#tcp_port) из файла конфигурации сервера для табличной функции `remote` (по умолчанию 9000) и [tcp&#95;port&#95;secure](../../operations/server-configuration-parameters/settings.md#tcp_port_secure) для табличной функции `remoteSecure` (по умолчанию 9440).<br /><br />    Для IPv6-адресов указание порта обязательно.<br /><br />    Если указан только параметр `addresses_expr`, для `db` и `table` по умолчанию используется `system.one`.<br /><br />    Тип: [String](../../sql-reference/data-types/string.md). |
| `db`             | Имя базы данных. Тип: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `table`          | Имя таблицы. Тип: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `user`           | Имя пользователя. Если не указано, используется `default`. Тип: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `password`       | Пароль пользователя. Если не указан, используется пустой пароль. Тип: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `sharding_key`   | Ключ сегментирования для распределения данных по узлам. Например: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`. Тип: [UInt32](../../sql-reference/data-types/int-uint.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

Аргументы также можно передавать с помощью [именованных коллекций](/ru/operations/named-collections.md).

<div id="returned-value">
  ## Возвращаемое значение
</div>

Таблица, расположенная на удалённом сервере.

<div id="usage">
  ## Использование
</div>

Поскольку табличные функции `remote` и `remoteSecure` заново устанавливают соединение для каждого запроса, вместо них рекомендуется использовать таблицу `Distributed`. Кроме того, если заданы имена хостов, они разрешаются, а ошибки при работе с различными репликами не учитываются. При обработке большого количества запросов всегда заранее создавайте таблицу `Distributed` и не используйте табличную функцию `remote`.

Табличная функция `remote` может быть полезна в следующих случаях:

* Разовая миграция данных из одной системы в другую
* Доступ к конкретному серверу для сравнения данных, отладки и тестирования, то есть для разовых соединений.
* Запросы между различными кластерами ClickHouse в исследовательских целях.
* Редкие распределённые запросы, выполняемые вручную.
* Распределённые запросы, в которых набор серверов каждый раз задаётся заново.

<div id="addresses">
  ### Адреса
</div>

```text
example01-01-1
example01-01-1:9440
example01-01-1:9000
localhost
127.0.0.1
[::]:9440
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Несколько адресов можно указать через запятую. В этом случае ClickHouse будет использовать распределённую обработку и отправит запрос на все указанные адреса (как на сегменты с разными данными). Пример:

```text
example01-01-1,example01-02-1
```

<div id="examples">
  ## Примеры
</div>

<div id="selecting-data-from-a-remote-server">
  ### Выборка данных с удалённого сервера:
</div>

```sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

Или с помощью [именованных коллекций](/ru/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = '127.0.0.1',
        database = 'db';
SELECT * FROM remote(creds, table='remote_engine_table') LIMIT 3;
```

<div id="inserting-data-into-a-table-on-a-remote-server">
  ### Вставка данных в таблицу на удалённом сервере:
</div>

```sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

<div id="migration-of-tables-from-one-system-to-another">
  ### Миграция таблиц из одной системы в другую:
</div>

В этом примере используется одна таблица из демонстрационного набора данных. База данных — `imdb`, а таблица — `actors`.

<div id="on-the-source-clickhouse-system-the-system-that-currently-hosts-the-data">
  #### В исходной системе ClickHouse (системе, где сейчас размещены данные)
</div>

* Проверьте имя исходной базы данных и таблицы (`imdb.actors`)

  ```sql
  show databases
  ```

  ```sql
  show tables in imdb
  ```

* Получите оператор CREATE TABLE из исходной системы:

```sql
  SELECT create_table_query
  FROM system.tables
  WHERE database = 'imdb' AND table = 'actors'
```

Результат

```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
```

<div id="on-the-destination-clickhouse-system">
  #### В целевой системе ClickHouse
</div>

* Создайте целевую базу данных:

  ```sql
  CREATE DATABASE imdb
  ```

* Используя оператор CREATE TABLE из исходной системы, создайте целевую таблицу:

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

<div id="back-on-the-source-deployment">
  #### Вернемся к исходному развертыванию
</div>

Вставьте данные в новую базу данных и таблицу, созданные в удаленной системе. Вам понадобятся хост, порт, имя пользователя, пароль, целевая база данных и целевая таблица.

```sql
INSERT INTO FUNCTION
remoteSecure('remote.clickhouse.cloud:9440', 'imdb.actors', 'USER', 'PASSWORD')
SELECT * from imdb.actors
```

<div id="globs-in-addresses">
  ## Глоббинг
</div>

Шаблоны в `{ }` используются для формирования набора сегментов и указания реплик. Если пар `{ }` несколько, генерируется декартово произведение соответствующих наборов.

Поддерживаются следующие типы шаблонов.

* `{a,b,c}` - Представляет любую из альтернативных строк `a`, `b` или `c`. В адресе первого сегмента шаблон заменяется на `a`, в адресе второго — на `b` и так далее. Например, `example0{1,2}-1` генерирует адреса `example01-1` и `example02-1`.
* `{N..M}` - Диапазон чисел. Этот шаблон генерирует адреса сегментов с индексами, последовательно увеличивающимися от `N` до `M` включительно. Например, `example0{1..2}-1` генерирует `example01-1` и `example02-1`.
* `{0n..0m}` - Диапазон чисел с ведущими нулями. Этот шаблон сохраняет ведущие нули в индексах. Например, `example{01..03}-1` генерирует `example01-1`, `example02-1` и `example03-1`.
* `{a|b}` - Любое количество вариантов, разделённых символом `|`. Шаблон задаёт реплики. Например, `example01-{1|2}` генерирует реплики `example01-1` и `example01-2`.

Запрос будет отправлен на первую доступную исправную реплику. Однако для `remote` реплики перебираются в порядке, заданном текущей настройкой [load&#95;balancing](../../operations/settings/settings.md#load_balancing).
Количество сгенерированных адресов ограничено настройкой [table&#95;function&#95;remote&#95;max&#95;addresses](../../operations/settings/settings.md#table_function_remote_max_addresses).