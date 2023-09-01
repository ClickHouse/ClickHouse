---
sidebar_position: 30
sidebar_label: MaterializedPostgreSQL
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

## Динамическое добавление новых таблиц в репликацию {#dynamically-adding-table-to-replication}

``` sql
ATTACH TABLE postgres_database.new_table;
```

При указании конкретного списка таблиц в базе с помощью настройки [materialized_postgresql_tables_list](../../operations/settings/settings.md#materialized-postgresql-tables-list), он будет обновлен (в `.sql` метаданных) на актуальный с учетом таблиц, добавленных с помощью запроса `ATTACH TABLE`.

## Динамическое удаление таблиц из репликации {#dynamically-removing-table-from-replication}

``` sql
DETACH TABLE postgres_database.table_to_remove;
```

## Настройки {#settings}

-   [materialized_postgresql_max_block_size](../../operations/settings/settings.md#materialized-postgresql-max-block-size)

-   [materialized_postgresql_tables_list](../../operations/settings/settings.md#materialized-postgresql-tables-list)

-   [materialized_postgresql_allow_automatic_update](../../operations/settings/settings.md#materialized-postgresql-allow-automatic-update)

-   [materialized_postgresql_replication_slot](../../operations/settings/settings.md#materialized-postgresql-replication-slot)

-   [materialized_postgresql_snapshot](../../operations/settings/settings.md#materialized-postgresql-snapshot)

``` sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_max_block_size = 65536,
         materialized_postgresql_tables_list = 'table1,table2,table3';

SELECT * FROM database1.table1;
```

Настройки можно при необходимости изменить с помощью DDL запроса. Однако с помощью него нельзя изменить настройку `materialized_postgresql_tables_list`, для обновления списка таблиц в данной настройке нужно использовать запрос `ATTACH TABLE`.

``` sql
ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
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

:::danger "Предупреждение"
    Репликация **TOAST**-значений не поддерживается. Для типа данных будет использоваться значение по умолчанию.
	
## Пример использования {#example-of-use}

``` sql
CREATE DATABASE postgresql_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SELECT * FROM postgresql_db.postgres_table;
```
## Примечания {#notes}

### Сбой слота логической репликации {#logical-replication-slot-failover}

Слоты логической репликации, которые есть на основном сервере, не доступны на резервных репликах.
Поэтому в случае сбоя новый основной сервер (который раньше был резевным) не будет знать о слотах репликации, которые были созданы на вышедшем из строя основном сервере. Это приведет к нарушению репликации из PostgreSQL.
Решением этой проблемы может стать ручное управление слотами репликации и определение постоянного слота репликации (об этом можно прочитать [здесь](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). Этот слот нужно передать с помощью настройки [materialized_postgresql_replication_slot](../../operations/settings/settings.md#materialized-postgresql-replication-slot), и он должен быть экспортирован в параметре `EXPORT SNAPSHOT`. Идентификатор снэпшота нужно передать в настройке [materialized_postgresql_snapshot](../../operations/settings/settings.md#materialized-postgresql-snapshot).

Имейте в виду, что это стоит делать только если есть реальная необходимость. Если такой необходимости нет, или если нет полного понимания того, как это работает, то самостоятельно слот репликации конфигурировать не стоит, он будет создан таблицей.

**Пример (от [@bchrobot](https://github.com/bchrobot))** 

1. Сконфигурируйте слот репликации в PostgreSQL.

```yaml
apiVersion: "acid.zalan.do/v1"
kind: postgresql
metadata:
  name: acid-demo-cluster
spec:
  numberOfInstances: 2
  postgresql:
    parameters:
      wal_level: logical
  patroni:
    slots:
      clickhouse_sync:
        type: logical
        database: demodb
        plugin: pgoutput
```

2. Дождитесь готовности слота репликации, затем инициируйте транзакцию и экспортируйте идентификатор снэпшота этой транзакции:

```sql
BEGIN;
SELECT pg_export_snapshot();
```

3. Создайте базу данных в ClickHouse:

```sql
CREATE DATABASE demodb
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS
  materialized_postgresql_replication_slot = 'clickhouse_sync',
  materialized_postgresql_snapshot = '0000000A-0000023F-3',
  materialized_postgresql_tables_list = 'table1,table2,table3';
```

4. Когда начнет выполняться репликация БД в ClickHouse, прервите транзакцию в PostgreSQL. Убедитесь, что репликация продолжается после сбоя:

```bash
kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
```
