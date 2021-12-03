---
toc_priority: 9
toc_title: EmbeddedRocksDB
---

# EmbeddedRocksDB 引擎 {#EmbeddedRocksDB-engine}

这个引擎允许 ClickHouse 与 [rocksdb](http://rocksdb.org/) 进行集成。

## 创建一张表 {#table_engine-EmbeddedRocksDB-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB PRIMARY KEY(primary_key_name)
```

必要参数:

-  `primary_key_name` – any column name in the column list.
- 必须指定 `primary key`, 仅支持主键中的一个列. 主键将被序列化为二进制的`rocksdb key`.
- 主键以外的列将以相应的顺序在二进制中序列化为`rocksdb`值.
- 带有键 `equals` 或 `in` 过滤的查询将被优化为从 `rocksdb` 进行多键查询.

示例:

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

[原始文章](https://clickhouse.tech/docs/en/engines/table-engines/integrations/embedded-rocksdb/) <!--hide-->
