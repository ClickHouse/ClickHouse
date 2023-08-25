---
sidebar_position: 32
sidebar_label: Atomic
---

# Atomic {#atomic}

它支持非阻塞的[DROP TABLE](#drop-detach-table)和[RENAME TABLE](#rename-table)查询和原子的[EXCHANGE TABLES t1 AND t2](#exchange-tables)查询。默认情况下使用`Atomic`数据库引擎。

## 创建数据库 {#creating-a-database}

``` sql
  CREATE DATABASE test[ ENGINE = Atomic];
```

## 使用方式 {#specifics-and-recommendations}

### Table UUID {#table-uuid}

数据库`Atomic`中的所有表都有唯一的[UUID](../../sql-reference/data-types/uuid.md)，并将数据存储在目录`/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/`，其中`xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy`是该表的UUID。

通常，UUID是自动生成的，但用户也可以在创建表时以相同的方式显式指定UUID(不建议这样做)。可以使用 [show_table_uuid_in_table_create_query_if_not_nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil)设置。显示UUID的使用`SHOW CREATE`查询。例如:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

### RENAME TABLES {#rename-table}

`RENAME`查询是在不更改UUID和移动表数据的情况下执行的。这些查询不会等待使用表的查询完成，而是会立即执行。

### DROP/DETACH TABLES {#drop-detach-table}

在`DROP TABLE`上，不删除任何数据，数据库`Atomic`只是通过将元数据移动到`/clickhouse_path/metadata_dropped/`将表标记为已删除，并通知后台线程。最终表数据删除前的延迟由[database_atomic_delay_before_drop_table_sec](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec)设置指定。

可以使用`SYNC`修饰符指定同步模式。使用[database_atomic_wait_for_drop_and_detach_synchronously](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously)设置执行此操作。在本例中，`DROP`等待运行 `SELECT`, `INSERT`和其他使用表完成的查询。表在不使用时将被实际删除。

### EXCHANGE TABLES {#exchange-tables}

`EXCHANGE`以原子方式交换表。因此，不是这种非原子操作：

```sql
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```
可以使用一个原子查询：

``` sql
EXCHANGE TABLES new_table AND old_table;
```

### ReplicatedMergeTree in Atomic Database {#replicatedmergetree-in-atomic-database}

对于[ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication)表，建议不要在ZooKeeper和副本名称中指定engine-path的参数。在这种情况下，将使用配置的参数[default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path)和[default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name)。如果要显式指定引擎的参数，建议使用{uuid}宏。这是非常有用的，以便为ZooKeeper中的每个表自动生成唯一的路径。

## 另请参考

-   [system.databases](../../operations/system-tables/databases.md) 系统表
