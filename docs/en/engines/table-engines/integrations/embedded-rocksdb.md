---
toc_priority: 6
toc_title: EmbeddedRocksdb
---

# EmbeddedRocksdb Engine {#EmbeddedRocksdb-engine}

This engine allows integrating ClickHouse with [rocksdb](http://rocksdb.org/).

`EmbeddedRocksdb` lets you:

## Creating a Table {#table_engine-EmbeddedRocksdb-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksdb PRIMARY KEY(primary_key_name)
```

Required parameters:

-  `primary_key_name` – any column name in the column list.

Example:

``` sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32,
)
ENGINE = EmbeddedRocksdb
PRIMARY KEY key
```

## Description {#description}

- `primary key` must be specified, only support one primary key. The primary key will serializeBinary as rocksdb key.
- Columns other than the primary key will be serializeBinary as rocksdb value in corresponding order.
- Queries with key `equals` or `in` filtering will be optimized to multi keys look up from rocksdb.
