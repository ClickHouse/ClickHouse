---
toc_priority: 32
toc_title: Atomic
---

# Atomic {#atomic}

It supports non-blocking [DROP TABLE](#drop-detach-table) and [RENAME TABLE](#rename-table) queries and atomic [EXCHANGE TABLES t1 AND t2](#exchange-tables) queries. `Atomic` database engine is used by default.

## Creating a Database {#creating-a-database}

``` sql
    CREATE DATABASE test[ ENGINE = Atomic];
```

## Specifics and recommendations {#specifics-and-recommendations}

### Table UUID {#table-uuid}

All tables in database `Atomic` have persistent [UUID](../../sql-reference/data-types/uuid.md) and store data in directory `/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/`, where `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` is UUID of the table. 
Usually, the UUID is generated automatically, but the user can also explicitly specify the UUID in the same way when creating the table (this is not recommended). To display the `SHOW CREATE` query with the UUID you can use setting [show_table_uuid_in_table_create_query_if_not_nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil). For example:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```
### RENAME TABLE {#rename-table}

`RENAME` queries are performed without changing UUID and moving table data. These queries do not wait for the completion of queries using the table and will be executed instantly.

### DROP/DETACH TABLE {#drop-detach-table}

On `DROP TABLE` no data is removed, database `Atomic` just marks table as dropped by moving metadata to `/clickhouse_path/metadata_dropped/` and notifies background thread. Delay before final table data deletion is specify by [database_atomic_delay_before_drop_table_sec](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec) setting.
You can specify synchronous mode using `SYNC` modifier. Use the [database_atomic_wait_for_drop_and_detach_synchronously](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously) setting to do this. In this case `DROP` waits for running `SELECT`, `INSERT` and other queries which are using the table to finish. Table will be actually removed when it's not in use.

### EXCHANGE TABLES {#exchange-tables}

`EXCHANGE` query swaps tables atomically. So instead of this non-atomic operation:

```sql
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```
you can use one atomic query:

``` sql
EXCHANGE TABLES new_table AND old_table;
```

### ReplicatedMergeTree in Atomic Database {#replicatedmergetree-in-atomic-database}

For [ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication) tables, it is recommended to not specify engine parameters - path in ZooKeeper and replica name. In this case, configuration parameters will be used [default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path) and [default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name). If you want to specify engine parameters explicitly, it is recommended to use {uuid} macros. This is useful so that unique paths are automatically generated for each table in ZooKeeper.

## See Also

-   [system.databases](../../operations/system-tables/databases.md) system table
