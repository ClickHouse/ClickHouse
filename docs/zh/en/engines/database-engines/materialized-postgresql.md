---
description: '基于 PostgreSQL 数据库中的表创建 ClickHouse 数据库。'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 60
slug: /engines/database-engines/materialized-postgresql
title: 'MaterializedPostgreSQL'
doc_type: '参考'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql">
  # MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
建议 ClickHouse Cloud 用户使用 [ClickPipes](/zh/integrations/clickpipes) 将 PostgreSQL 复制到 ClickHouse。它原生支持面向 PostgreSQL 的高性能 CDC (变更数据捕获) 。
:::

基于 PostgreSQL 数据库中的表创建一个 ClickHouse 数据库。首先，使用 `MaterializedPostgreSQL` 引擎的数据库会为 PostgreSQL 数据库创建一个 快照，并加载所需的表。所需表可以是指定数据库中任意 schema 下任意子集的表。在获取 快照 的同时，数据库引擎还会获取 LSN；完成表的 初始转储 后，便开始从 WAL 拉取更新。数据库创建完成后，后续新添加到 PostgreSQL 数据库中的表不会自动加入复制，必须通过 `ATTACH TABLE db.table` 查询手动添加。

复制基于 PostgreSQL Logical Replication Protocol 实现。该协议不支持复制 DDL，但能够识别是否发生了会破坏复制的变更 (如列类型变更、添加/删除列) 。检测到此类变更后，对应表将停止接收更新。此时，应使用 `ATTACH`/ `DETACH PERMANENTLY` 查询重新完整加载该表。如果 DDL 不会破坏复制 (例如重命名列) ，表仍会继续接收更新 (插入按位置执行) 。

:::note
此数据库引擎仍处于实验阶段。要使用它，请在配置文件中将 `allow_experimental_database_materialized_postgresql` 设置为 1，或使用 `SET` 命令：

```sql
SET allow_experimental_database_materialized_postgresql=1
```

:::

<div id="creating-a-database">
  ## 创建数据库
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password') [SETTINGS ...]
```

**引擎参数**

* `host:port` — PostgreSQL server 的端点。
* `database` — PostgreSQL 数据库名称。
* `user` — PostgreSQL 用户名。
* `password` — 用户密码。

<div id="example-of-use">
  ## 使用示例
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
  ## 动态将新表添加到复制中
</div>

创建 `MaterializedPostgreSQL` 数据库后，它不会自动检测对应 PostgreSQL 数据库中的新表。此类表可以手动添加：

```sql
ATTACH TABLE postgres_database.new_table;
```

:::warning
在 22.1 之前的版本中，将表加入复制时会遗留一个未删除的临时 replication slot (名为 `{db_name}_ch_replication_slot_tmp`) 。如果你在 22.1 之前的 ClickHouse 版本中 Attach 表，请务必手动删除它 (`SELECT pg_drop_replication_slot('{db_name}_ch_replication_slot_tmp')`) 。否则，磁盘占用会持续增长。此问题已在 22.1 中修复。
:::

<div id="dynamically-removing-table-from-replication">
  ## 动态地将表移出复制
</div>

可以将特定表移出复制：

```sql
DETACH TABLE postgres_database.table_to_remove PERMANENTLY;
```

<div id="schema">
  ## PostgreSQL schema
</div>

PostgreSQL 的 [schema](https://www.postgresql.org/docs/9.1/ddl-schemas.html) 可通过 3 种方式配置 (自 21.12 版本起) 。

1. 一个 schema 对应一个 `MaterializedPostgreSQL` 数据库引擎。需要使用 setting `materialized_postgresql_schema`。
   表仅通过表名访问：

```sql
CREATE DATABASE postgres_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema = 'postgres_schema';

SELECT * FROM postgres_database.table1;
```

2. 对于一个 `MaterializedPostgreSQL` 数据库引擎，可以使用任意数量的 schema，并指定一组表。需要使用设置 `materialized_postgresql_tables_list`。每个表都需要连同其所属的 schema 一起写明。
   访问表时需要同时使用 schema 名称和表名：

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'schema1.table1,schema2.table2,schema1.table3',
         materialized_postgresql_tables_list_with_schema = 1;

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema2.table2`;
```

但在这种情况下，`materialized_postgresql_tables_list` 中的所有表都必须写为带 schema 名称的形式。
要求 `materialized_postgresql_tables_list_with_schema = 1`。

警告：在这种情况下，表名中不允许出现点号。

3. 对于一个 `MaterializedPostgreSQL` 数据库引擎，可以指定任意数量的 schema，并包含每个 schema 下的全部表。要求使用设置 `materialized_postgresql_schema_list`。

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema_list = 'schema1,schema2,schema3';

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema1.table2`;
SELECT * FROM database1.`schema2.table2`;
```

警告：在此情况下，表名中不允许包含点号。

<div id="requirements">
  ## 要求
</div>

1. 在 PostgreSQL 配置文件中，[wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) 设置的值必须为 `logical`，且 `max_replication_slots` 参数的值必须至少为 `2`。

2. 每个复制表都必须具有以下任一[副本标识](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY)：

* 主键 (默认)

* 索引

```bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

系统始终会先检查主键。如果主键不存在，则会检查被定义为副本标识索引的索引。
如果将索引用作副本标识，那么一张表中只能有一个这样的索引。
你可以使用以下命令查看特定表使用的是哪种类型：

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
不支持复制 [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) 值，将使用该数据类型的默认值。
:::

<div id="settings">
  ## 设置
</div>

<div id="materialized-postgresql-tables-list">
  ### `materialized_postgresql_tables_list`
</div>

设置一个以逗号分隔的 PostgreSQL 数据库表列表，这些表将通过 [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md) 数据库引擎进行复制。

每个表都可以在括号中指定要复制的部分列。如果未指定列子集，则会复制该表的所有列。

```sql
    materialized_postgresql_tables_list = 'table1(co1, col2),table2,table3(co3, col5, col7)
```

默认值：空列表——即复制整个 PostgreSQL 数据库。

<div id="materialized-postgresql-schema">
  ### `materialized_postgresql_schema`
</div>

默认值：空字符串。 (使用默认 schema)

<div id="materialized-postgresql-schema-list">
  ### `materialized_postgresql_schema_list`
</div>

默认值：空列表。 (将使用默认 schema)

<div id="materialized-postgresql-max-block-size">
  ### `materialized_postgresql_max_block_size`
</div>

设置在将数据刷新到 PostgreSQL 数据库表之前，可先在内存中收集的行数。

可能的值：

* 正整数。

默认值：`65536`。

<div id="materialized-postgresql-replication-slot">
  ### `materialized_postgresql_replication_slot`
</div>

用户创建的 replication slot。必须与 `materialized_postgresql_snapshot` 配合使用。

<div id="materialized-postgresql-snapshot">
  ### `materialized_postgresql_snapshot`
</div>

用于标识快照的文本字符串，[PostgreSQL 表的初始转储](../../engines/database-engines/materialized-postgresql.md)将基于该快照执行。必须与 `materialized_postgresql_replication_slot` 一起使用。

```sql
    CREATE DATABASE database1
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS materialized_postgresql_tables_list = 'table1,table2,table3';

    SELECT * FROM database1.table1;
```

如有必要，可使用 DDL 查询更改这些设置。但设置 `materialized_postgresql_tables_list` 无法修改。要更新该设置中的表列表，请使用 `ATTACH TABLE` 查询。

```sql
    ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
```

<div id="materialized_postgresql_use_unique_replication_consumer_identifier">
  ### `materialized_postgresql_use_unique_replication_consumer_identifier`
</div>

复制时使用唯一的复制消费者标识符。默认值：`0`。
如果设置为 `1`，则允许配置多个指向同一个 `PostgreSQL` 表的 `MaterializedPostgreSQL` 表。

<div id="materialized-postgresql-use-extended-date-and-time-types">
  ### `materialized_postgresql_use_extended_date_and_time_types`
</div>

将 PostgreSQL 的 `date` 和 `timestamp`/`timestamptz` 类型映射为 ClickHouse 的 `Date32` 和 `DateTime64`，后两者可覆盖 PostgreSQL 这些类型更宽的取值范围。默认值：`1`。
如果设置为 `0`，则改用范围较窄的 `Date` 和 `DateTime` 类型 (超出其取值范围的值，或带有子秒级精度的值，将无法表示) 。

此设置仅控制创建嵌套表时由类型推断选定的列类型，因此必须在 `CREATE DATABASE` 时指定。之后无法通过 `ALTER DATABASE ... MODIFY SETTING` 修改它 (已创建的嵌套表会保留其固定的列类型，并且此类更改会被拒绝) ；如需修改，请重新创建数据库。它不适用于 `MaterializedPostgreSQL` 表引擎，因为该引擎中的列类型是显式声明的。

<div id="notes">
  ## 说明
</div>

<div id="logical-replication-slot-failover">
  ### logical replication slot 的故障转移
</div>

主节点上已有的 Logical Replication Slot 在 standby 副本上不可用。
因此，一旦发生故障转移，新的主节点 (即原来的物理 standby) 将无法识别旧主节点上已有的任何 replication slot。这会导致来自 PostgreSQL 的复制中断。
解决方法之一是自行管理 replication slot，并定义一个永久 replication slot (可在[此处](https://patroni.readthedocs.io/en/latest/SETTINGS.html)找到一些相关信息) 。你需要通过 `materialized_postgresql_replication_slot` setting 传入 slot 名称，并且该 slot 必须使用 `EXPORT SNAPSHOT` 选项导出。snapshot 标识符则需要通过 `materialized_postgresql_snapshot` setting 传入。

请注意，只有在确实需要时才应使用此方法。如果没有实际需求，或者并不完全清楚这样做的原因，最好还是让 表引擎 自行创建并管理 replication slot。

**示例 (来自 [@bchrobot](https://github.com/bchrobot))&#x20;**

1. 在 PostgreSQL 中配置 replication slot。

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

2. 等待 replication slot 就绪，然后开始一个事务并导出该事务的 snapshot 标识符：

   ```sql
   BEGIN;
   SELECT pg_export_snapshot();
   ```

3. 在 ClickHouse 中创建 database：

   ```sql
   CREATE DATABASE demodb
   ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
   SETTINGS
     materialized_postgresql_replication_slot = 'clickhouse_sync',
     materialized_postgresql_snapshot = '0000000A-0000023F-3',
     materialized_postgresql_tables_list = 'table1,table2,table3';
   ```

4. 确认已复制到 ClickHouse DB 后，结束 PostgreSQL 事务。然后验证故障转移后复制是否仍会继续：

   ```bash
   kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
   ```

<div id="required-permissions">
  ### 所需权限
</div>

1. [CREATE PUBLICATION](https://www.postgresql.org/docs/14/sql-createpublication.html) -- 创建查询权限。

2. [CREATE&#95;REPLICATION&#95;SLOT](https://www.postgresql.org/docs/10/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-SLOT) -- 复制权限。

3. [pg&#95;drop&#95;replication&#95;slot](https://www.postgresql.org/docs/9.5/functions-admin.html#FUNCTIONS-REPLICATION) -- 复制权限或 superuser 权限。

4. [DROP PUBLICATION](https://www.postgresql.org/docs/10/sql-droppublication.html) -- publication 的所有者 (即 MaterializedPostgreSQL engine 自身中的 `username`) 。

可以通过使用设置 `materialized_postgresql_replication_slot` 和 `materialized_postgresql_snapshot`，避免执行 `2` 和 `3` 命令，也无需具备这些权限。但必须格外谨慎。

对以下表的访问权限：

1. pg&#95;publication

2. pg&#95;replication&#95;slots

3. pg&#95;publication&#95;tables

<div id="backup-and-restore">
  ### 备份与恢复
</div>

`MaterializedPostgreSQL` 数据库可以备份。每个复制表的数据都存储在一个嵌套的 `ReplacingMergeTree` 表中，因此 `BACKUP DATABASE` 会通过该嵌套表将这些数据一并备份。

```sql
BACKUP DATABASE postgres_db TO Disk('backups', 'postgres_db.zip');
```

不支持对 `MaterializedPostgreSQL` database 或 table **进行原地恢复**。恢复后的 `MaterializedPostgreSQL` object 会立即开始从在线 PostgreSQL source 复制，因此如果在其上恢复 backup 快照，就会把该 快照 与当前远程 state 混在一起。因此，在这种情况下，RESTORE 会直接失败。请改为将已捕获的数据恢复到普通的 `ReplacingMergeTree` 表中：

* 对于 database backup，每个表存储的定义已经是合成的嵌套 `ReplacingMergeTree` (而不是 `MaterializedPostgreSQL` engine) ，因此每个表都可以直接恢复到一个新的、尚不存在的表中：

  ```sql
  RESTORE TABLE postgres_db.table1 AS restored_db.table1
  FROM Disk('backups', 'postgres_db.zip')
  SETTINGS allow_different_table_def = 1;
  ```

* 对于独立的 `MaterializedPostgreSQL` 表 backup，存储的定义是 `MaterializedPostgreSQL` engine 本身。请预先创建一个 `ReplacingMergeTree` 表，其结构与嵌套表相同 (包括 `_sign` 和 `_version` 列) ，然后将数据恢复到该表中：

  ```sql
  RESTORE TABLE src AS existing_replacing_mergetree
  FROM Disk('backups', 'table.zip')
  SETTINGS allow_different_table_def = 1;
  ```