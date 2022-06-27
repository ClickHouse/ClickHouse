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
## 指标

还有一个`system.rocksdb` 表, 公开rocksdb的统计信息:

```sql
SELECT
    name,
    value
FROM system.rocksdb

┌─name──────────────────────┬─value─┐
│ no.file.opens             │     1 │
│ number.block.decompressed │     1 │
└───────────────────────────┴───────┘
```

## 配置

你能修改任何[rocksdb options](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map) 配置，使用配置文件:

```xml
<rocksdb>
    <options>
        <max_background_jobs>8</max_background_jobs>
    </options>
    <column_family_options>
        <num_levels>2</num_levels>
    </column_family_options>
    <tables>
        <table>
            <name>TABLE</name>
            <options>
                <max_background_jobs>8</max_background_jobs>
            </options>
            <column_family_options>
                <num_levels>2</num_levels>
            </column_family_options>
        </table>
    </tables>
</rocksdb>
```

[原始文章](https://clickhouse.com/docs/en/engines/table-engines/integrations/embedded-rocksdb/) <!--hide-->
