---
description: 'Движок `ExternalDistributed` позволяет выполнять `SELECT`-запросы к данным,
  хранящимся на удалённых серверах MySQL или PostgreSQL. В качестве аргумента
  принимает движки MySQL или PostgreSQL, что позволяет использовать шардирование.'
sidebar_label: 'ExternalDistributed'
sidebar_position: 55
slug: /engines/table-engines/integrations/ExternalDistributed
title: 'Движок таблицы `ExternalDistributed`'
doc_type: 'reference'
---

Движок `ExternalDistributed` позволяет выполнять `SELECT`-запросы к данным, хранящимся на удалённых серверах MySQL или PostgreSQL. В качестве аргумента принимает движки [MySQL](../../../engines/table-engines/integrations/mysql.md) или [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md), что позволяет использовать шардирование.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

См. подробное описание запроса [CREATE TABLE](/ru/sql-reference/statements/create/table).

Структура таблицы может отличаться от структуры исходной таблицы:

* Имена столбцов должны совпадать с именами столбцов исходной таблицы, но можно использовать только часть этих столбцов и в любом порядке.
* Типы столбцов могут отличаться от типов в исходной таблице. ClickHouse пытается [привести](/ru/sql-reference/functions/type-conversion-functions#CAST) значения к типам данных ClickHouse.

**Параметры движка**

* `engine` — движок таблицы `MySQL` или `PostgreSQL`.
* `host:port` — адрес сервера MySQL или PostgreSQL.
* `database` — имя удалённой базы данных.
* `table` — имя удалённой таблицы.
* `user` — имя пользователя.
* `password` — пароль пользователя.

<div id="implementation-details">
  ## Подробности реализации
</div>

Поддерживается несколько реплик, которые должны быть перечислены через `|`, а сегменты — через `,`. Например:

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

При указании реплик для каждого сегмента при чтении выбирается одна из доступных реплик. Если соединение установить не удаётся, выбирается следующая реплика, и так по очереди перебираются все реплики. Если подключиться не удаётся ни к одной из реплик, попытка таким же образом повторяется несколько раз.

Вы можете указать любое количество сегментов и любое количество реплик для каждого сегмента.

**См. также**

* [движок таблицы MySQL](../../../engines/table-engines/integrations/mysql.md)
* [движок таблицы PostgreSQL](../../../engines/table-engines/integrations/postgresql.md)
* [движок таблицы Distributed](../../../engines/table-engines/special/distributed.md)