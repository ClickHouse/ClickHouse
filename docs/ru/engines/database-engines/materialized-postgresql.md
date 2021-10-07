---
toc_priority: 30
toc_title: MaterializedPostgreSQL
---

# [экспериментальный] MaterializedPostgreSQL {#materialize-postgresql}

Создает базу данных ClickHouse с исходным дампом данных таблиц PostgreSQL и запускает процесс репликации, т.е. выполняется применение новых изменений в фоне, как эти изменения происходят в таблице PostgreSQL в удаленной базе данных PostgreSQL.

Сервер ClickHouse работает как реплика PostgreSQL. Он читает WAL и выполняет DML запросы. Данные, полученные в результате DDL запросов, не реплицируются, но сами запросы могут быть обработаны (описано ниже).

## Создание базы данных {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', ['database' | database], 'user', 'password') [SETTINGS ...]
```

**Параметры движка**

-   `host:port` — адрес сервера PostgreSQL.
-   `database` — имя базы данных на удалённом сервере.
-   `user` — пользователь PostgreSQL.
-   `password` — пароль пользователя.

## Настройки {#settings}

-   [materialized_postgresql_max_block_size](../../operations/settings/settings.md#materialized-postgresql-max-block-size)

-   [materialized_postgresql_tables_list](../../operations/settings/settings.md#materialized-postgresql-tables-list)

-   [materialized_postgresql_allow_automatic_update](../../operations/settings/settings.md#materialized-postgresql-allow-automatic-update)

``` sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_max_block_size = 65536,
         materialized_postgresql_tables_list = 'table1,table2,table3';

SELECT * FROM database1.table1;
```

## Требования {#requirements}

1. Настройка [wal_level](https://postgrespro.ru/docs/postgrespro/10/runtime-config-wal) должна иметь значение `logical`, параметр `max_replication_slots` должен быть равен по меньшей мере `2` в конфигурационном файле в PostgreSQL.

2. Каждая реплицируемая таблица должна иметь один из следующих [репликационных идентификаторов](https://postgrespro.ru/docs/postgresql/10/sql-altertable#SQL-CREATETABLE-REPLICA-IDENTITY):

-   первичный ключ (по умолчанию)

-   индекс

``` bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

Первичный ключ всегда проверяется первым. Если он отсутствует, то проверяется индекс, определенный как replica identity index (репликационный идентификатор).
Если индекс используется в качестве репликационного идентификатора, то в таблице должен быть только один такой индекс.
Вы можете проверить, какой тип используется для указанной таблицы, выполнив следующую команду:

``` bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

!!! warning "Предупреждение"
    Репликация **TOAST**-значений не поддерживается. Для типа данных будет использоваться значение по умолчанию.
	
## Пример использования {#example-of-use}

``` sql
CREATE DATABASE postgresql_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SELECT * FROM postgresql_db.postgres_table;
```
