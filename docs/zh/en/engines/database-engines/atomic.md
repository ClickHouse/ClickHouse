---
description: '`Atomic` 引擎支持非阻塞的 [`DROP TABLE`](#drop-detach-table) 和 [`RENAME TABLE`](#rename-table)
  查询，以及原子性的 [`EXCHANGE TABLES`](#exchange-tables) 查询。开源版 ClickHouse 默认使用
  `Atomic` 数据库引擎。'
sidebar_label: 'Atomic'
sidebar_position: 10
slug: /engines/database-engines/atomic
title: 'Atomic'
doc_type: 'reference'
---

`Atomic` 引擎支持非阻塞的 [`DROP TABLE`](#drop-detach-table) 和 [`RENAME TABLE`](#rename-table) 查询，以及原子性的 [`EXCHANGE TABLES`](#exchange-tables) 查询。开源版 ClickHouse 默认使用 `Atomic` 数据库引擎。

:::note
在 ClickHouse Cloud 中，默认使用 [`Shared` 数据库引擎](/zh/cloud/reference/shared-catalog#shared-database-engine)，也支持上述操作。
:::

<div id="creating-a-database">
  ## 创建数据库
</div>

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS disk=...];
```

<div id="specifics-and-recommendations">
  ## 具体说明与建议
</div>

<div id="table-uuid">
  ### 表 UUID
</div>

在 `Atomic` 数据库中，每个表都具有持久的 [UUID](../../sql-reference/data-types/uuid.md)，其数据存储在以下目录中：

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

其中，`xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` 是该表的 UUID。

默认情况下，UUID 会自动生成。不过，用户也可以在创建表时明确指定 UUID，但不建议这样做。

例如：

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
您可以使用 [show&#95;table&#95;uuid&#95;in&#95;table&#95;create&#95;query&#95;if&#95;not&#95;nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil) 设置，在 `SHOW CREATE` 查询中显示 UUID。
:::

<div id="rename-table">
  ### RENAME TABLE
</div>

[`RENAME`](../../sql-reference/statements/rename.md) 查询不会修改 UUID，也不会移动表中的数据。这些查询会立即执行，不会等待其他正在使用该表的查询结束。

<div id="drop-detach-table">
  ### DROP/DETACH 表
</div>

使用 `DROP TABLE` 时，不会删除任何数据。`Atomic` 引擎只是将表的元数据移动到 `/clickhouse_path/metadata_dropped/`，把该表标记为已删除，并通知后台线程。最终删除表数据前的延迟由 [`database_atomic_delay_before_drop_table_sec`](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec) 设置指定。
你可以使用 `SYNC` 修饰符来指定同步模式。为此，请使用 [`database_atomic_wait_for_drop_and_detach_synchronously`](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously) 设置。在这种情况下，`DROP` 会等待正在运行且使用该表的 `SELECT`、`INSERT` 及其他查询完成。该表会在不再被使用时被移除。

<div id="exchange-tables">
  ### EXCHANGE 表/字典
</div>

[`EXCHANGE`](../../sql-reference/statements/exchange.md) 查询可以原子性地交换表或字典。例如，相比下面这种非原子操作：

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```

你可以使用 atomic 数据库：

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

<div id="replicatedmergetree-in-atomic-database">
  ### atomic 数据库中的 ReplicatedMergeTree
</div>

对于 [`ReplicatedMergeTree`](/zh/engines/table-engines/mergetree-family/replication) 表，建议不要指定 ZooKeeper 中路径和副本名称对应的引擎参数。在这种情况下，将使用配置参数 [`default_replica_path`](../../operations/server-configuration-parameters/settings.md#default_replica_path) 和 [`default_replica_name`](../../operations/server-configuration-parameters/settings.md#default_replica_name)。如果你想显式指定引擎参数，建议使用 `{uuid}` 宏。这样可以确保在 ZooKeeper 中为每个表自动生成唯一的路径。

<div id="metadata-disk">
  ### 元数据磁盘
</div>

当在 `SETTINGS` 中指定 `disk` 时，该磁盘将用于存储表元数据文件。
例如：

```sql
CREATE TABLE db (n UInt64) ENGINE = Atomic SETTINGS disk=disk(type='local', path='/var/lib/clickhouse-disks/db_disk');
```

如果未指定，默认会使用在 `database_disk.disk` 中定义的 disk。

<div id="see-also">
  ## 另请参见
</div>

* [system.databases](../../operations/system-tables/databases.md) 系统表