---
toc_priority: 12
toc_title: MaterializedPostgreSQL
---

# MaterializedPostgreSQL {#materialize-postgresql}

Движок `MaterializedPostgreSQL` позволяет выполнять запросы `SELECT` и `INSERT` над данными, хранящимися на удалённом PostgreSQL сервере.

## Создание таблицы {#creating-a-table}

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

## Требования {#requirements}

1. Настройка [wal_level](https://postgrespro.ru/docs/postgrespro/10/runtime-config-wal) должна иметь значение `logical`, параметр `max_replication_slots` должен быть равен по меньшей мере `2` в конфигурационном файле в PostgreSQL.

2. Таблица, созданная с помощью движка `MaterializedPostgreSQL`, должна иметь первичный ключ — такой же, как индекс идентичности реплики (по умолчанию: первичный ключ) таблицы PostgreSQL (смотрите [индекс идентичности реплики](../../database-engines/materialized-postgresql.md#requirements)).

3. Допускается только база данных [Atomic](https://en.wikipedia.org/wiki/Atomicity_(database_systems)).

## Виртуальные столбцы {#virtual-columns}

-   `_version` (тип: UInt64)

-   `_sign` (тип: Int8)

Эти столбцы не нужно добавлять при создании таблицы. Они всегда доступны в `SELECT` запросе.
Столбец `_version` равен позиции `LSN` в `WAL`, поэтому его можно использовать для проверки актуальности репликации.

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

!!! warning "Предупреждение"
    Репликация **TOAST**-значений не поддерживается. Для типа данных будет использоваться значение по умолчанию.
