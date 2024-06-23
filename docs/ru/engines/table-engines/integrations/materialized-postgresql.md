---
slug: /ru/engines/table-engines/integrations/materialized-postgresql
sidebar_position: 12
sidebar_label: MaterializedPostgreSQL
---

# MaterializedPostgreSQL {#materialize-postgresql}

Создает таблицу ClickHouse с исходным дампом данных таблицы PostgreSQL и запускает процесс репликации, т.е. выполняется применение новых изменений в фоне, как эти изменения происходят в таблице PostgreSQL в удаленной базе данных PostgreSQL.

Если требуется более одной таблицы, вместо движка таблиц рекомендуется использовать движок баз данных [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) и с помощью настройки [materialized_postgresql_tables_list](../../../operations/settings/settings.md#materialized-postgresql-tables-list) указывать таблицы, которые нужно реплицировать. Это будет намного лучше с точки зрения нагрузки на процессор, уменьшит количество подключений и количество слотов репликации внутри удаленной базы данных PostgreSQL.

## Создание таблицы {#creating-a-table}

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**Параметры движка**

-   `host:port` — адрес сервера PostgreSQL.
-   `database` — имя базы данных на удалённом сервере.
-   `table` — имя таблицы на удалённом сервере.
-   `user` — пользователь PostgreSQL.
-   `password` — пароль пользователя.

## Требования {#requirements}

1. Настройка [wal_level](https://postgrespro.ru/docs/postgrespro/10/runtime-config-wal) должна иметь значение `logical`, параметр `max_replication_slots` должен быть равен по меньшей мере `2` в конфигурационном файле в PostgreSQL.

2. Таблица, созданная с помощью движка `MaterializedPostgreSQL`, должна иметь первичный ключ — такой же, как replica identity index (по умолчанию: первичный ключ) таблицы PostgreSQL (смотрите [replica identity index](../../../engines/database-engines/materialized-postgresql.md#requirements)).

3. Допускается только база данных [Atomic](https://en.wikipedia.org/wiki/Atomicity_(database_systems)).

## Виртуальные столбцы {#virtual-columns}

-   `_version` — счетчик транзакций. Тип: [UInt64](../../../sql-reference/data-types/int-uint.md).
-   `_sign` — метка удаления. Тип: [Int8](../../../sql-reference/data-types/int-uint.md). Возможные значения:
    - `1` — строка не удалена, 
    - `-1` — строка удалена.

Эти столбцы не нужно добавлять при создании таблицы. Они всегда доступны в `SELECT` запросе.
Столбец `_version` равен позиции `LSN` в `WAL`, поэтому его можно использовать для проверки актуальности репликации.

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::danger Предупреждение
Репликация **TOAST**-значений не поддерживается. Для типа данных будет использоваться значение по умолчанию.
:::
