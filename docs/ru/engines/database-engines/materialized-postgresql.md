---
toc_priority: 30
toc_title: MaterializedPostgreSQL
---

# MaterializedPostgreSQL {#materialize-postgresql}

## Создание базы данных {#creating-a-database}

``` sql
CREATE DATABASE test_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SELECT * FROM test_database.postgres_table;
```

## Настройки {#settings}

1. `materialized_postgresql_max_block_size` — задает максимальное количество строк, собранных перед вставкой данных в таблицу. По умолчанию: `65536`.

2. `materialized_postgresql_tables_list` — задает список таблиц для движка баз данных `MaterializedPostgreSQL`. По умолчанию: `whole database`.

3. `materialized_postgresql_allow_automatic_update` — позволяет автоматически обновить таблицу в фоновом режиме при обнаружении изменений схемы. По умолчанию: `0` (`false`).

``` sql
CREATE DATABASE test_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_max_block_size = 65536,
         materialized_postgresql_tables_list = 'table1,table2,table3';

SELECT * FROM test_database.table1;
```

## Требования {#requirements}

-   Настройка `wal_level` должна иметь значение `logical`, настройка `max_replication_slots` должна быть равна по меньшей мере `2` в конфигурационном файле в PostgreSQL.

-   Каждая реплицируемая таблица должна иметь один из следующих **идентификаторов реплики**:

1. **по умолчанию** (первичный ключ)

2. **индекс**

``` bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

Первичный ключ всегда проверяется первым. Если он отсутствует, то проверяется индекс, определенный как индекс идентичности реплики.
Если индекс используется в качестве идентификатора реплики, то в таблице должен быть только один такой индекс.
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

## Предупреждение {#warning}

1.  Преобразование **TOAST**-значений не поддерживается. Для типа данных будет использоваться значение по умолчанию.
