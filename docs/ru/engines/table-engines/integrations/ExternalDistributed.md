---
toc_priority: 12
toc_title: ExternalDistributed
---

# ExternalDistributed {#externaldistributed}

Движок `ExternalDistributed` позволяет выполнять запросы `SELECT` для таблиц на удаленном сервере MySQL или PostgreSQL. Принимает в качестве аргумента табличные движки [MySQL](../../../engines/table-engines/integrations/mysql.md) или [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md), поэтому возможно шардирование.

## Создание таблицы {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

Смотрите подробное описание запроса [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query).

Структура таблицы может отличаться от структуры исходной таблицы:

-   Имена столбцов должны быть такими же, как в исходной таблице, но можно использовать только некоторые из этих столбцов и в любом порядке.
-   Типы столбцов могут отличаться от типов в исходной таблице. ClickHouse пытается [привести](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) значения к типам данных ClickHouse.

**Параметры движка**

-   `engine` — табличный движок `MySQL` или `PostgreSQL`.
-   `host:port` — адрес сервера MySQL или PostgreSQL.
-   `database` — имя базы данных на сервере.
-   `table` — имя таблицы.
-   `user` — имя пользователя.
-   `password` — пароль пользователя.

## Особенности реализации {#implementation-details}

Поддерживает несколько реплик, которые должны быть перечислены через `|`, а шарды — через `,`. Например:

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

При указании реплик для каждого из шардов при чтении выбирается одна из доступных реплик. Если соединиться не удалось, то выбирается следующая реплика, и так для всех реплик. Если попытка соединения не удалась для всех реплик, то сервер ClickHouse снова пытается соединиться с одной из реплик, перебирая их по кругу, и так несколько раз.

Вы можете указать любое количество шардов и любое количество реплик для каждого шарда.

**Смотрите также**

-   [Табличный движок MySQL](../../../engines/table-engines/integrations/mysql.md)
-   [Табличный движок PostgreSQL](../../../engines/table-engines/integrations/postgresql.md)
-   [Табличный движок Distributed](../../../engines/table-engines/special/distributed.md)
