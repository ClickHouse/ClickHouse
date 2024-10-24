---
slug: /en/engines/table-engines/special/keeper-map
sidebar_position: 150
sidebar_label: KeeperMap
---

# KeeperMap {#keepermap}

This engine allows you to use Keeper/ZooKeeper cluster as consistent key-value store with linearizable writes and sequentially consistent reads.

To enable KeeperMap storage engine, you need to define a ZooKeeper path where the tables will be stored using `<keeper_map_path_prefix>` config.

For example:

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

where path can be any other valid ZooKeeper path.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = KeeperMap(root_path, [keys_limit]) PRIMARY KEY(primary_key_name)
```

Engine parameters:

- `root_path` - ZooKeeper path where the `table_name` will be stored.  
This path should not contain the prefix defined by `<keeper_map_path_prefix>` config because the prefix will be automatically appended to the `root_path`.  
Additionally, format of `auxiliary_zookeeper_cluster_name:/some/path` is also supported where `auxiliary_zookeeper_cluster` is a ZooKeeper cluster defined inside `<auxiliary_zookeepers>` config.  
By default, ZooKeeper cluster defined inside `<zookeeper>` config is used.
- `keys_limit` - number of keys allowed inside the table.  
This limit is a soft limit and it can be possible that more keys will end up in the table for some edge cases.
- `primary_key_name` – any column name in the column list.
- `primary key` must be specified, it supports only one column in the primary key. The primary key will be serialized in binary as a `node name` inside ZooKeeper. 
- columns other than the primary key will be serialized to binary in corresponding order and stored as a value of the resulting node defined by the serialized key.
- queries with key `equals` or `in` filtering will be optimized to multi keys lookup from `Keeper`, otherwise all values will be fetched.

Example:

``` sql
CREATE TABLE keeper_map_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = KeeperMap('/keeper_map_table', 4)
PRIMARY KEY key
```

with

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```


Each value, which is binary serialization of `(v1, v2, v3)`, will be stored inside `/keeper_map_tables/keeper_map_table/data/serialized_key` in `Keeper`.
Additionally, number of keys will have a soft limit of 4 for the number of keys.

If multiple tables are created on the same ZooKeeper path, the values are persisted until there exists at least 1 table using it.  
As a result, it is possible to use `ON CLUSTER` clause when creating the table and sharing the data from multiple ClickHouse instances.  
Of course, it's possible to manually run `CREATE TABLE` with same path on unrelated ClickHouse instances to have same data sharing effect.

## Supported operations {#supported-operations}

### Inserts

When new rows are inserted into `KeeperMap`, if the key does not exist, a new entry for the key is created.
If the key exists, and setting `keeper_map_strict_mode` is set to `true`, an exception is thrown, otherwise, the value for the key is overwritten.

Example:

```sql
INSERT INTO keeper_map_table VALUES ('some key', 1, 'value', 3.2);
```

### Deletes

Rows can be deleted using `DELETE` query or `TRUNCATE`. 
If the key exists, and setting `keeper_map_strict_mode` is set to `true`, fetching and deleting data will succeed only if it can be executed atomically.

```sql
DELETE FROM keeper_map_table WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE keeper_map_table DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE keeper_map_table;
```

### Updates

Values can be updated using `ALTER TABLE` query. Primary key cannot be updated.
If setting `keeper_map_strict_mode` is set to `true`, fetching and updating data will succeed only if it's executed atomically.

```sql
ALTER TABLE keeper_map_table UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

## Related content

- Blog: [Building a Real-time Analytics Apps with ClickHouse and Hex](https://clickhouse.com/blog/building-real-time-applications-with-clickhouse-and-hex-notebook-keeper-engine)
