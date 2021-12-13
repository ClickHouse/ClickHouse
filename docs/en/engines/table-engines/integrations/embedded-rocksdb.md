---
toc_priority: 9
toc_title: EmbeddedRocksDB
---

# EmbeddedRocksDB Engine {#EmbeddedRocksDB-engine}

This engine allows integrating ClickHouse with [rocksdb](http://rocksdb.org/).

## Creating a Table {#table_engine-EmbeddedRocksDB-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB PRIMARY KEY(primary_key_name)
```

Required parameters:

-  `primary_key_name` – any column name in the column list.
- `primary key` must be specified, it supports only one column in the primary key. The primary key will be serialized in binary as a `rocksdb key`.
- columns other than the primary key will be serialized in binary as `rocksdb` value in corresponding order.
- queries with key `equals` or `in` filtering will be optimized to multi keys lookup from `rocksdb`.

Example:

``` sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32,
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

[Original article](https://clickhouse.tech/docs/en/engines/table-engines/integrations/embedded-rocksdb/) <!--hide-->
