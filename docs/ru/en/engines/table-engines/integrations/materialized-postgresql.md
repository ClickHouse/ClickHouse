---
description: 'Создает таблицу ClickHouse с начальным дампом данных из таблицы PostgreSQL
  и запускает процесс репликации.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 130
slug: /engines/table-engines/integrations/materialized-postgresql
title: 'Движок таблицы MaterializedPostgreSQL'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql-table-engine">
  # Движок таблицы MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
Пользователям ClickHouse Cloud рекомендуется использовать [ClickPipes](/ru/integrations/clickpipes) для репликации PostgreSQL в ClickHouse. Это решение нативно поддерживает высокопроизводительный CDC (фиксацию изменений данных) для PostgreSQL.
:::

Создает таблицу ClickHouse с начальным дампом данных из таблицы PostgreSQL и запускает процесс репликации, то есть выполняет фоновую задачу, которая применяет новые изменения по мере их появления в таблице PostgreSQL в удаленной базе данных.

:::note
Этот движок таблицы является экспериментальным. Чтобы использовать его, установите `allow_experimental_materialized_postgresql_table` в значение 1 в файлах конфигурации или с помощью команды `SET`:

```sql
SET allow_experimental_materialized_postgresql_table=1
```

:::

Если требуется реплицировать несколько таблиц, настоятельно рекомендуется использовать движок базы данных [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) вместо движка таблицы и параметр `materialized_postgresql_tables_list`, в котором указываются таблицы для репликации (также можно будет добавить схему базы данных). Это значительно лучше с точки зрения CPU, а также требует меньше соединений и меньше слотов репликации в удалённой базе данных PostgreSQL.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**Параметры движка**

* `host:port` — адрес сервера PostgreSQL.
* `database` — имя удалённой базы данных.
* `table` — имя удалённой таблицы.
* `user` — пользователь PostgreSQL.
* `password` — пароль пользователя.

<div id="requirements">
  ## Требования
</div>

1. Параметр [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) должен быть установлен в значение `logical`, а параметр `max_replication_slots` в файле конфигурации PostgreSQL должен иметь значение не менее `2`.

2. Таблица с движком `MaterializedPostgreSQL` должна иметь первичный ключ — такой же, как индекс replica identity (по умолчанию это первичный ключ) таблицы PostgreSQL (см. [подробнее об индексе replica identity](../../../engines/database-engines/materialized-postgresql.md#requirements)).

3. Допускается только база данных [Atomic](https://en.wikipedia.org/wiki/Atomicity_\(database_systems\)).

4. Движок таблицы `MaterializedPostgreSQL` работает только с PostgreSQL версии &gt;= 11, поскольку для его реализации требуется функция PostgreSQL [pg&#95;replication&#95;slot&#95;advance](https://pgpedia.info/p/pg_replication_slot_advance.html).

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_version` — Счётчик транзакций. Тип: [UInt64](../../../sql-reference/data-types/int-uint.md).

* `_sign` — Метка удаления. Тип: [Int8](../../../sql-reference/data-types/int-uint.md). Возможные значения:
  * `1` — Строка не удалена,
  * `-1` — Строка удалена.

Эти столбцы не нужно добавлять при создании таблицы. Они всегда доступны в запросе `SELECT`.
Столбец `_version` соответствует позиции `LSN` в `WAL`, поэтому его можно использовать для проверки того, насколько актуальна репликация.

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
Репликация значений [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) не поддерживается. Будет использоваться значение по умолчанию для этого типа данных.
:::