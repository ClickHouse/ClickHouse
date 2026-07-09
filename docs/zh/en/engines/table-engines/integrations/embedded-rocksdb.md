---
description: '该引擎支持将 ClickHouse 与 RocksDB 集成'
sidebar_label: 'EmbeddedRocksDB'
sidebar_position: 50
slug: /engines/table-engines/integrations/embedded-rocksdb
title: 'EmbeddedRocksDB 表引擎'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="embeddedrocksdb-table-engine">
  # EmbeddedRocksDB 表引擎
</div>

<CloudNotSupportedBadge />

该引擎可将 ClickHouse 与 [RocksDB](http://rocksdb.org/) 集成。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
[ SETTINGS name=value, ... ]
```

引擎参数：

* `ttl` - 值的生存时间。TTL 以秒为单位指定。如果 TTL 为 0，则使用常规 RocksDB 实例 (不启用 TTL) 。
* `rocksdb_dir` - 现有 RocksDB 的目录路径，或新创建 RocksDB 的目标路径。使用指定的 `rocksdb_dir` 打开表。
* `read_only` - 当 `read_only` 设置为 true 时，使用只读模式。对于启用了 TTL 的存储，不会触发合并整理 (无论手动还是自动) ，因此不会删除任何已过期条目。
* `primary_key_name` – 列列表中的任意列名。
* 必须指定 `primary key`，且主键只支持单列。主键将作为 `rocksdb key` 以二进制格式序列化。
* 除主键外的列将按对应顺序作为 `rocksdb` value 以二进制格式序列化。
* 带有 key `equals` 或 `in` 过滤器的查询将被优化为从 `rocksdb` 进行多 key 查找。

引擎设置：

* `optimize_for_bulk_insert` – 表已针对批量插入进行优化 (insert 管道会创建 SST 文件并将其导入 rocksdb database，而不是写入 memtables) ；默认值：`1`。
* `bulk_insert_block_size` - 通过批量插入创建的 SST 文件最小大小 (按行数计) ；默认值：`1048449`。

示例：

```sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

<div id="metrics">
  ## 指标
</div>

此外，还有 `system.rocksdb` 表，用于提供 rocksdb 统计信息：

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

<div id="configuration">
  ## 配置
</div>

你也可以通过 config 修改任意 [RocksDB 选项](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map)：

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

默认情况下，简单近似计数优化是关闭的，这可能会影响 `count()` 查询的性能。要启用此
优化，请将 `optimize_trivial_approximate_count_query` 设置为 `1`。此外，此设置也会影响 EmbeddedRocksDB engine 的 `system.tables`，
启用后即可查看 `total_rows` 和 `total_bytes` 的近似值。

<div id="supported-operations">
  ## 支持的操作
</div>

<div id="inserts">
  ### 插入操作
</div>

当新行插入到 `EmbeddedRocksDB` 中时，如果键已存在，则会更新其值；否则会创建一个新键。

示例：

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### 删除
</div>

可使用 `DELETE` 查询或 `TRUNCATE` 删除数据行。

```sql
DELETE FROM test WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE test DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE test;
```

<div id="updates">
  ### 更新
</div>

可以使用 `ALTER TABLE` 查询来更新值。主键不能更新。

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="joins">
  ### JOIN
</div>

支持对 EmbeddedRocksDB 表使用一种特殊的 `direct` JOIN。
这种 `direct` JOIN 无需在内存中构建哈希表，
而是直接从 EmbeddedRocksDB 读取数据。

对于大型 JOIN，使用 `direct` JOIN 时内存使用量可能会显著降低，
因为无需创建哈希表。

要启用 `direct` JOIN：

```sql
SET join_algorithm = 'direct, hash'
```

:::tip
当 `join_algorithm` 设置为 `direct, hash` 时，会在可能的情况下优先使用直接 JOIN，否则使用哈希 JOIN。
:::

<div id="example">
  #### 示例
</div>

<div id="create-and-populate-an-embeddedrocksdb-table">
  ##### 创建并填充 EmbeddedRocksDB 表
</div>

```sql
CREATE TABLE rdb
(
    `key` UInt32,
    `value` Array(UInt32),
    `value2` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

```sql
INSERT INTO rdb
    SELECT
        toUInt32(sipHash64(number) % 10) AS key,
        [key, key+1] AS value,
        ('val2' || toString(key)) AS value2
    FROM numbers_mt(10);
```

<div id="create-and-populate-a-table-to-join-with-table-rdb">
  ##### 创建并填充一个要与表 `rdb` 进行 join 的表
</div>

```sql
CREATE TABLE t2
(
    `k` UInt16
)
ENGINE = TinyLog
```

```sql
INSERT INTO t2 SELECT number AS k
FROM numbers_mt(10)
```

<div id="set-the-join-algorithm-to-direct">
  ##### 将 join 算法设为 `direct`
</div>

```sql
SET join_algorithm = 'direct'
```

<div id="an-inner-join">
  ##### INNER JOIN
</div>

```sql
SELECT *
FROM
(
    SELECT k AS key
    FROM t2
) AS t2
INNER JOIN rdb ON rdb.key = t2.key
ORDER BY key ASC
```

```response
┌─key─┬─rdb.key─┬─value──┬─value2─┐
│   0 │       0 │ [0,1]  │ val20  │
│   2 │       2 │ [2,3]  │ val22  │
│   3 │       3 │ [3,4]  │ val23  │
│   6 │       6 │ [6,7]  │ val26  │
│   7 │       7 │ [7,8]  │ val27  │
│   8 │       8 │ [8,9]  │ val28  │
│   9 │       9 │ [9,10] │ val29  │
└─────┴─────────┴────────┴────────┘
```

<div id="more-information-on-joins">
  ### 关于 JOIN 的更多信息
</div>

* [`join_algorithm` 设置](/zh/operations/settings/settings.md#join_algorithm)
* [JOIN 子句](/zh/sql-reference/statements/select/join.md)