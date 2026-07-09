---
description: '创建一个 ClickHouse 表，使用 PostgreSQL 表的初始数据转储进行填充，并启动复制过程。'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 130
slug: /engines/table-engines/integrations/materialized-postgresql
title: 'MaterializedPostgreSQL 表引擎'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql-table-engine">
  # MaterializedPostgreSQL 表引擎
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
建议 ClickHouse Cloud 用户使用 [ClickPipes](/zh/integrations/clickpipes) 将 PostgreSQL 复制到 ClickHouse。它原生支持 PostgreSQL 的高性能 CDC (变更数据捕获) 。
:::

基于 PostgreSQL 表的初始数据转储创建 ClickHouse 表，并启动复制过程；也就是说，它会执行后台作业，在远程 PostgreSQL 数据库中的 PostgreSQL 表发生新变更时应用这些变更。

:::note
此表引擎为 Experimental。要使用它，请在配置文件中将 `allow_experimental_materialized_postgresql_table` 设置为 1，或使用 `SET` 命令：

```sql
SET allow_experimental_materialized_postgresql_table=1
```

:::

如果需要多个表，强烈建议使用 [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) 数据库引擎，而不是表引擎，并使用 `materialized_postgresql_tables_list` 设置来指定要复制的表 (后续也将支持添加数据库 `schema`) 。这样在 CPU 占用、连接数以及远程 PostgreSQL 数据库中的 replication slots 方面都会更好。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**引擎参数**

* `host:port` — PostgreSQL server 的地址。
* `database` — 远程数据库名称。
* `table` — 远程表名称。
* `user` — PostgreSQL 用户。
* `password` — 用户密码。

<div id="requirements">
  ## 要求
</div>

1. 在 PostgreSQL 配置文件中，[wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) 设置的值必须为 `logical`，并且 `max_replication_slots` 参数的值必须至少为 `2`。

2. 使用 `MaterializedPostgreSQL` 引擎的表必须具有主键，并且该主键必须与 PostgreSQL 表的副本标识索引相同 (默认情况下为主键)  (请参阅[副本标识索引的详细信息](../../../engines/database-engines/materialized-postgresql.md#requirements)) 。

3. 仅允许使用 [Atomic](https://en.wikipedia.org/wiki/Atomicity_\(database_systems\)) 数据库引擎。

4. `MaterializedPostgreSQL` 表引擎仅适用于 PostgreSQL 11 及以上版本，因为其实现需要 PostgreSQL 函数 [pg&#95;replication&#95;slot&#95;advance](https://pgpedia.info/p/pg_replication_slot_advance.html)。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_version` — 事务计数器。类型：[UInt64](../../../sql-reference/data-types/int-uint.md)。

* `_sign` — 删除标记。类型：[Int8](../../../sql-reference/data-types/int-uint.md)。可能的值：
  * `1` — 行未删除，
  * `-1` — 行已删除。

创建表时无需添加这些列。它们始终可在 `SELECT` 查询中访问。
`_version` 列等于 `WAL` 中的 `LSN` 位置，因此可用于检查复制的最新进度。

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
不支持复制 [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) 的值。将使用该数据类型的默认值。
:::