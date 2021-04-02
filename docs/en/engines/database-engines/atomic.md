---
toc_priority: 32
toc_title: Atomic
---

# Atomic {#atomic}

It supports non-blocking [DROP](#drop-table) and [RENAME TABLE](#rename-table) queries and atomic [EXCHANGE TABLES t1 AND t2](#exchange-tables) queries. `Atomic` database engine is used by default.

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

### DROP TABLE {#drop-table}

On `DROP TABLE` no data is removed, database `Atomic` just marks table as dropped by moving metadata to `/clickhouse_path/metadata_dropped/` and notifies background thread. Running queries still may use dropped table. Table will be actually removed when it's not in use.

### RENAME TABLE {#rename-table}

`RENAME` queries are performed without changing UUID and moving table data. These queries do not wait for the completion of queries using the table and will be executed instantly.

### DROP/DETACH {#drop-detach}

`DELETE` and `DETACH` queries are executed asynchronously — waits for the running `SELECT` queries to finish but is invisible to the new queries.
You also can specify `NO DELAY` or `SYNC` mode. See [database_atomic_delay_before_drop_table_sec](../../operations/settings/settings.md#database_atomic_delay_before_drop_table_sec), [database_atomic_wait_for_drop_and_detach_synchronously](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously) settings.

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

For [ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication) tables is recomended do not specify parameters of engine - path in ZooKeeper and replica name. In this case will be used parameters of the configuration [default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path) and [default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name). If you want specify parameters of engine explicitly than recomended to use {uuid} macros. This is useful so that unique paths are automatically generated for each table in the ZooKeeper.

## See Also

-   [system.databases](../../operations/system-tables/databases.md) system table
