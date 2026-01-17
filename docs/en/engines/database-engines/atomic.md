---
description: 'The `Atomic` engine supports non-blocking `DROP TABLE` and `RENAME TABLE`
  queries, and atomic `EXCHANGE TABLES`queries. The `Atomic` database engine is used
  by default.'
sidebar_label: 'Atomic'
sidebar_position: 10
slug: /engines/database-engines/atomic
title: 'Atomic'
doc_type: 'reference'
---

# Atomic 

The `Atomic` engine supports non-blocking [`DROP TABLE`](#drop-detach-table) and [`RENAME TABLE`](#rename-table) queries, and atomic [`EXCHANGE TABLES`](#exchange-tables) queries. The `Atomic` database engine is used by default in open-source ClickHouse. 

:::note
On ClickHouse Cloud, the [`Shared` database engine](/cloud/reference/shared-catalog#shared-database-engine) is used by default and also supports
the above mentioned operations.
:::

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS disk=...];
```

## Specifics and recommendations {#specifics-and-recommendations}

### Table UUID {#table-uuid}

Each table in the `Atomic` database has a persistent [UUID](../../sql-reference/data-types/uuid.md) and stores its data in the following directory:

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

Where `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` is the UUID of the table.

By default, the UUID is generated automatically. However, users can explicitly specify the UUID when creating a table, though this is not recommended.

For example:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
You can use the [show_table_uuid_in_table_create_query_if_not_nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil) setting to display the UUID with the `SHOW CREATE` query. 
:::

### RENAME TABLE {#rename-table}

[`RENAME`](../../sql-reference/statements/rename.md) queries do not modify the UUID or move table data. These queries execute immediately and do not wait for other queries that are using the table to complete.

### DROP/DETACH TABLE {#drop-detach-table}

When using `DROP TABLE`, no data is removed. The `Atomic` engine just marks the table as dropped by moving it's metadata to `/clickhouse_path/metadata_dropped/` and notifies the background thread. The delay before the final table data deletion is specified by the [`database_atomic_delay_before_drop_table_sec`](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec) setting.
You can specify synchronous mode using `SYNC` modifier. Use the [`database_atomic_wait_for_drop_and_detach_synchronously`](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously) setting to do this. In this case `DROP` waits for running `SELECT`, `INSERT` and other queries which are using the table to finish. The table will be removed when it's not in use.

### EXCHANGE TABLES/DICTIONARIES {#exchange-tables}

The [`EXCHANGE`](../../sql-reference/statements/exchange.md) query swaps tables or dictionaries atomically. For instance, instead of this non-atomic operation:

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```
you can use an atomic one:

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

### ReplicatedMergeTree in atomic database {#replicatedmergetree-in-atomic-database}

For [`ReplicatedMergeTree`](/engines/table-engines/mergetree-family/replication) tables, it is recommended not to specify the engine parameters for the path in ZooKeeper and the replica name. In this case, the configuration parameters [`default_replica_path`](../../operations/server-configuration-parameters/settings.md#default_replica_path) and [`default_replica_name`](../../operations/server-configuration-parameters/settings.md#default_replica_name) will be used. If you want to specify engine parameters explicitly, it is recommended to use the `{uuid}` macros. This ensures that unique paths are automatically generated for each table in ZooKeeper.

### Metadata disk {#metadata-disk}
When `disk` is specified in `SETTINGS`, the disk is used to store table metadata files.
For example:

```sql
CREATE TABLE db (n UInt64) ENGINE = Atomic SETTINGS disk=disk(type='local', path='/var/lib/clickhouse-disks/db_disk');
```
If unspecified, the disk defined in `database_disk.disk` is used by default.

## See also {#see-also}

- [system.databases](../../operations/system-tables/databases.md) system table
