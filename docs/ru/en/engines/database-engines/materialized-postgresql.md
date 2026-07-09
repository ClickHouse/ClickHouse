---
description: 'Создаёт базу данных ClickHouse с таблицами из базы данных PostgreSQL.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 60
slug: /engines/database-engines/materialized-postgresql
title: 'MaterializedPostgreSQL'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql">
  # MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
Пользователям ClickHouse Cloud рекомендуется использовать [ClickPipes](/ru/integrations/clickpipes) для репликации PostgreSQL в ClickHouse. Это обеспечивает нативную поддержку высокопроизводительной CDC (фиксация изменений данных) для PostgreSQL.
:::

Создаёт базу данных ClickHouse с таблицами из базы данных PostgreSQL. Сначала база данных с движком `MaterializedPostgreSQL` создаёт снимок базы данных PostgreSQL и загружает необходимые таблицы. В необходимые таблицы может входить любое подмножество таблиц из любого подмножества схем указанной базы данных. Одновременно со снимком движок базы данных получает LSN и после выполнения первоначальной выгрузки таблиц начинает считывать изменения из WAL. После создания базы данных новые таблицы, добавленные в базу данных PostgreSQL, автоматически в репликацию не включаются. Их нужно добавлять вручную запросом `ATTACH TABLE db.table`.

Репликация реализована с использованием протокола логической репликации PostgreSQL, который не позволяет реплицировать DDL, но позволяет определить, произошли ли изменения, нарушающие репликацию (изменение типа столбца, добавление/удаление столбцов). Такие изменения обнаруживаются, и соответствующие таблицы перестают получать обновления. В этом случае для полной перезагрузки таблицы следует использовать запросы `ATTACH`/ `DETACH PERMANENTLY`. Если DDL не нарушает репликацию (например, при переименовании столбца), таблица всё равно будет получать обновления (вставка выполняется по позиции).

:::note
Этот движок базы данных является экспериментальным. Чтобы использовать его, установите `allow_experimental_database_materialized_postgresql` в 1 в конфигурационных файлах или с помощью команды `SET`:

```sql
SET allow_experimental_database_materialized_postgresql=1
```

:::

<div id="creating-a-database">
  ## Создание базы данных
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password') [SETTINGS ...]
```

**Параметры движка**

* `host:port` — конечная точка сервера PostgreSQL.
* `database` — имя базы данных PostgreSQL.
* `user` — имя пользователя PostgreSQL.
* `password` — пароль пользователя.

<div id="example-of-use">
  ## Пример использования
</div>

```sql
CREATE DATABASE postgres_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SHOW TABLES FROM postgres_db;

┌─name───┐
│ table1 │
└────────┘

SELECT * FROM postgres_db.postgres_table;
```

<div id="dynamically-adding-table-to-replication">
  ## Динамическое добавление новых таблиц в репликацию
</div>

После создания базы данных `MaterializedPostgreSQL` новые таблицы в соответствующей базе данных PostgreSQL не обнаруживаются автоматически. Такие таблицы можно добавить вручную:

```sql
ATTACH TABLE postgres_database.new_table;
```

:::warning
До версии 22.1 при добавлении таблицы в репликацию оставался неудалённый временный слот репликации (с именем `{db_name}_ch_replication_slot_tmp`). Если вы подключаете таблицы в ClickHouse версии до 22.1, обязательно удалите его вручную (`SELECT pg_drop_replication_slot('{db_name}_ch_replication_slot_tmp')`). В противном случае будет расти использование диска. Эта проблема исправлена в версии 22.1.
:::

<div id="dynamically-removing-table-from-replication">
  ## Динамическое исключение таблиц из репликации
</div>

Из репликации можно исключить отдельные таблицы:

```sql
DETACH TABLE postgres_database.table_to_remove PERMANENTLY;
```

<div id="schema">
  ## Схема PostgreSQL
</div>

[Схему](https://www.postgresql.org/docs/9.1/ddl-schemas.html) PostgreSQL можно настроить тремя способами (начиная с версии 21.12).

1. Одна схема для одного движка базы данных `MaterializedPostgreSQL`. Для этого требуется использовать настройку `materialized_postgresql_schema`.
   Доступ к таблицам осуществляется только по имени таблицы:

```sql
CREATE DATABASE postgres_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema = 'postgres_schema';

SELECT * FROM postgres_database.table1;
```

2. Любое количество схем с указанным набором таблиц для одного движка базы данных `MaterializedPostgreSQL`. Для этого требуется использовать настройку `materialized_postgresql_tables_list`. Каждая таблица указывается вместе со своей схемой.
   Доступ к таблицам осуществляется по имени схемы и имени таблицы одновременно:

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'schema1.table1,schema2.table2,schema1.table3',
         materialized_postgresql_tables_list_with_schema = 1;

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema2.table2`;
```

Но в этом случае все таблицы в `materialized_postgresql_tables_list` должны быть указаны вместе с именем своей схемы.
Требуется `materialized_postgresql_tables_list_with_schema = 1`.

Предупреждение: в этом случае точки в имени таблицы не допускаются.

3. Любое количество схем с полным набором таблиц для одного движка базы данных `MaterializedPostgreSQL`. Для этого требуется использовать настройку `materialized_postgresql_schema_list`.

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema_list = 'schema1,schema2,schema3';

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema1.table2`;
SELECT * FROM database1.`schema2.table2`;
```

Предупреждение: в этом случае точки в имени таблицы недопустимы.

<div id="requirements">
  ## Требования
</div>

1. Параметр [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) должен иметь значение `logical`, а параметр `max_replication_slots` — значение не менее `2` в файле конфигурации PostgreSQL.

2. Для каждой реплицируемой таблицы должен быть задан один из следующих вариантов [replica identity](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY):

* первичный ключ (по умолчанию)

* индекс

```bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

Сначала всегда проверяется первичный ключ. Если он отсутствует, проверяется индекс, заданный как replica identity index.
Если индекс используется в качестве replica identity, в таблице может быть только один такой индекс.
Проверить, какой тип используется для конкретной таблицы, можно следующей командой:

```bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

:::note
Репликация значений [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) не поддерживается. Будет использоваться значение по умолчанию для этого типа данных.
:::

<div id="settings">
  ## Настройки
</div>

<div id="materialized-postgresql-tables-list">
  ### `materialized_postgresql_tables_list`
</div>

Задаёт разделённый запятыми список таблиц базы данных PostgreSQL, которые будут реплицироваться с помощью движка базы данных [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md).

Для каждой таблицы в скобках можно указать подмножество реплицируемых столбцов. Если подмножество столбцов не указано, будут реплицироваться все столбцы этой таблицы.

```sql
    materialized_postgresql_tables_list = 'table1(co1, col2),table2,table3(co3, col5, col7)
```

Значение по умолчанию: пустой список — будет реплицирована вся база данных PostgreSQL.

<div id="materialized-postgresql-schema">
  ### `materialized_postgresql_schema`
</div>

Значение по умолчанию: пустая строка. (Используется схема по умолчанию)

<div id="materialized-postgresql-schema-list">
  ### `materialized_postgresql_schema_list`
</div>

Значение по умолчанию: пустой список. (Используется схема по умолчанию.)

<div id="materialized-postgresql-max-block-size">
  ### `materialized_postgresql_max_block_size`
</div>

Задаёт количество строк, накапливаемых в памяти перед записью данных в таблицу базы данных PostgreSQL.

Возможные значения:

* Положительное целое число.

Значение по умолчанию: `65536`.

<div id="materialized-postgresql-replication-slot">
  ### `materialized_postgresql_replication_slot`
</div>

Слот репликации, созданный пользователем. Должен использоваться вместе с `materialized_postgresql_snapshot`.

<div id="materialized-postgresql-snapshot">
  ### `materialized_postgresql_snapshot`
</div>

Строка, идентифицирующая снимок, на основе которого будет выполнена [первоначальная выгрузка таблиц PostgreSQL](../../engines/database-engines/materialized-postgresql.md). Должен использоваться вместе с `materialized_postgresql_replication_slot`.

```sql
    CREATE DATABASE database1
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS materialized_postgresql_tables_list = 'table1,table2,table3';

    SELECT * FROM database1.table1;
```

При необходимости настройки можно изменить с помощью DDL-запроса. Однако изменить настройку `materialized_postgresql_tables_list` нельзя. Чтобы обновить список таблиц в этой настройке, используйте запрос `ATTACH TABLE`.

```sql
    ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
```

<div id="materialized_postgresql_use_unique_replication_consumer_identifier">
  ### `materialized_postgresql_use_unique_replication_consumer_identifier`
</div>

Использовать для репликации уникальный идентификатор consumer. Значение по умолчанию: `0`.
Если установлено значение `1`, позволяет настроить несколько таблиц `MaterializedPostgreSQL`, указывающих на одну и ту же таблицу `PostgreSQL`.

<div id="materialized-postgresql-use-extended-date-and-time-types">
  ### `materialized_postgresql_use_extended_date_and_time_types`
</div>

Сопоставляет типы PostgreSQL `date` и `timestamp`/`timestamptz` с типами ClickHouse `Date32` и `DateTime64`, которые покрывают более широкий диапазон значений, чем типы PostgreSQL. По умолчанию: `1`.
Если установлено значение `0`, вместо них используются более узкие типы `Date` и `DateTime` (значения вне их диапазона или с долями секунды в них не представимы).

Этот параметр влияет только на типы столбцов, которые вывод типов выбирает при создании вложенных таблиц, поэтому его нужно указывать при `CREATE DATABASE`. Позже изменить его с помощью `ALTER DATABASE ... MODIFY SETTING` нельзя (уже созданные вложенные таблицы сохраняют свои фиксированные типы столбцов, и такое изменение будет отклонено); чтобы изменить этот параметр, пересоздайте базу данных. Он не применяется к движку таблицы `MaterializedPostgreSQL`, где типы столбцов задаются явно.

<div id="notes">
  ## Примечания
</div>

<div id="logical-replication-slot-failover">
  ### Переключение при отказе для слота логической репликации
</div>

Слоты логической репликации, существующие на основном узле, недоступны на резервных репликах.
Поэтому при переключении новый основной узел (бывший физический резервный) не будет иметь сведений о слотах, которые существовали на старом основном узле. В результате репликация из PostgreSQL нарушится.
Решение — самостоятельно управлять слотами репликации и настроить постоянный слот репликации (дополнительную информацию можно найти [здесь](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). Имя слота нужно передать через настройку `materialized_postgresql_replication_slot`, а сам слот должен использоваться с опцией `EXPORT SNAPSHOT`. Идентификатор снимка нужно передать через настройку `materialized_postgresql_snapshot`.

Обратите внимание: использовать это следует только при реальной необходимости. Если такой необходимости нет или вы не до конца понимаете, зачем это нужно, лучше позволить движку таблицы самостоятельно создавать слот репликации и управлять им.

**Пример (от [@bchrobot](https://github.com/bchrobot))**

1. Настройте слот репликации в PostgreSQL.

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

2. Дождитесь, пока слот репликации будет готов, затем начните транзакцию и экспортируйте идентификатор её снимка:

   ```sql
   BEGIN;
   SELECT pg_export_snapshot();
   ```

3. В ClickHouse создайте базу данных:

   ```sql
   CREATE DATABASE demodb
   ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
   SETTINGS
     materialized_postgresql_replication_slot = 'clickhouse_sync',
     materialized_postgresql_snapshot = '0000000A-0000023F-3',
     materialized_postgresql_tables_list = 'table1,table2,table3';
   ```

4. После подтверждения репликации в БД ClickHouse завершите транзакцию PostgreSQL. Убедитесь, что после переключения при отказе репликация продолжается:

   ```bash
   kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
   ```

<div id="required-permissions">
  ### Необходимые привилегии
</div>

1. [CREATE PUBLICATION](https://www.postgresql.org/docs/14/sql-createpublication.html) -- привилегия на выполнение запроса CREATE.

2. [CREATE&#95;REPLICATION&#95;SLOT](https://www.postgresql.org/docs/10/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-SLOT) -- привилегия репликации.

3. [pg&#95;drop&#95;replication&#95;slot](https://www.postgresql.org/docs/9.5/functions-admin.html#FUNCTIONS-REPLICATION) -- привилегия репликации или роль superuser.

4. [DROP PUBLICATION](https://www.postgresql.org/docs/10/sql-droppublication.html) -- владелец публикации (`username` в самом движке MaterializedPostgreSQL).

Можно не выполнять команды `2` и `3` и не иметь этих привилегий. Используйте настройки `materialized_postgresql_replication_slot` и `materialized_postgresql_snapshot`. Но с большой осторожностью.

Доступ к таблицам:

1. pg&#95;publication

2. pg&#95;replication&#95;slots

3. pg&#95;publication&#95;tables

<div id="backup-and-restore">
  ### Резервное копирование и восстановление
</div>

Для базы данных `MaterializedPostgreSQL` можно создать резервную копию. Данные каждой реплицируемой таблицы хранятся во вложенной таблице `ReplacingMergeTree`, поэтому команда `BACKUP DATABASE` включает эти данные в резервную копию, делегируя эту операцию вложенной таблице.

```sql
BACKUP DATABASE postgres_db TO Disk('backups', 'postgres_db.zip');
```

Восстановление базы данных или таблицы `MaterializedPostgreSQL` **на месте не поддерживается**. Восстановленный объект `MaterializedPostgreSQL` сразу начинает реплицироваться из рабочего источника PostgreSQL, поэтому восстановление поверх него снимка из резервной копии привело бы к смешению этого снимка с текущим состоянием удалённого источника. Поэтому в этом случае команда RESTORE завершается безопасным отказом. Вместо этого восстановите захваченные данные в обычные таблицы `ReplacingMergeTree`:

* В резервной копии базы данных сохранённое определение каждой таблицы уже представляет собой синтетическую вложенную таблицу `ReplacingMergeTree` (а не движок `MaterializedPostgreSQL`), поэтому каждую таблицу можно восстановить напрямую в новую, ещё не существующую таблицу:

  ```sql
  RESTORE TABLE postgres_db.table1 AS restored_db.table1
  FROM Disk('backups', 'postgres_db.zip')
  SETTINGS allow_different_table_def = 1;
  ```

* Для резервной копии автономной таблицы `MaterializedPostgreSQL` сохранённым определением является сам движок `MaterializedPostgreSQL`. Заранее создайте таблицу `ReplacingMergeTree` с той же структурой, что и у вложенной таблицы (включая столбцы `_sign` и `_version`), и восстановите данные в неё:

  ```sql
  RESTORE TABLE src AS existing_replacing_mergetree
  FROM Disk('backups', 'table.zip')
  SETTINGS allow_different_table_def = 1;
  ```