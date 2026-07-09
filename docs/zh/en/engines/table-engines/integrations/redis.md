---
description: '此引擎支持将 ClickHouse 与 Redis 集成。'
sidebar_label: 'Redis'
sidebar_position: 175
slug: /engines/table-engines/integrations/redis
title: 'Redis 表引擎'
doc_type: 'guide'
---

此引擎支持将 ClickHouse 与 [Redis](https://redis.io/) 集成。由于 Redis 采用键值模型，我们强烈建议仅进行点查询，例如 `where k=xx` 或 `where k in (xx, xx)`。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] | named_collection[, option=value [,..]] })
PRIMARY KEY(primary_key_name);
```

**引擎参数**

* `host:port` — Redis 服务器地址，可省略 port，此时将使用默认的 Redis 端口 6379。
* `db_index` — Redis 数据库索引，范围为 0 到 15，默认为 0。
* `password` — 用户密码，默认为空字符串。
* `pool_size` — Redis 最大连接池大小，默认为 16。
* `primary_key_name` - 列列表中的任意列名。

:::note 序列化
`PRIMARY KEY` 仅支持一列。主键将以二进制形式序列化为 Redis key。
除主键外的其他列将按相应顺序以二进制形式序列化为 Redis value。
:::

参数也可以通过 [named collections](/zh/operations/named-collections.md) 传递。在这种情况下，需要分别指定 `host` 和 `port`。这种方式推荐用于生产环境。目前，通过 named collections 传递给 Redis 的所有参数都是必需的。

:::note 筛选
带有 `key equals` 或 `in filtering` 的查询会被优化为从 Redis 进行多 key 查找。如果查询中没有 filtering key，则会发生全表扫描，这是一项开销很大的操作。
:::

<div id="usage-example">
  ## 使用示例
</div>

在 ClickHouse 中使用普通参数创建一个采用 `Redis` 引擎的表：

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis('redis1:6379') PRIMARY KEY(key);
```

或者使用 [named collections](/zh/operations/named-collections.md)：

```xml
<named_collections>
    <redis_creds>
        <host>localhost</host>
        <port>6379</port>
        <password>****</password>
        <pool_size>16</pool_size>
        <db_index>0</db_index>
    </redis_creds>
</named_collections>
```

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis(redis_creds) PRIMARY KEY(key);
```

插入：

```sql title="Query"
INSERT INTO redis_table VALUES('1', 1, '1', 1.0), ('2', 2, '2', 2.0);
```

```sql title="Query"
SELECT COUNT(*) FROM redis_table;
```

```text title="Response"
┌─count()─┐
│       2 │
└─────────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE key='1';
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 1   │  1 │ 1  │  1 │
└─────┴────┴────┴────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE v1=2;
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 2   │  2 │ 2  │  2 │
└─────┴────┴────┴────┘
```

更新：

请注意，主键无法更新。

```sql title="Query"
ALTER TABLE redis_table UPDATE v1=2 WHERE key='1';
```

删除：

```sql title="Query"
ALTER TABLE redis_table DELETE WHERE key='1';
```

Truncate:

异步将数据刷写到 Redis 数据库。`Truncate` 也支持 SYNC 模式。

```sql title="Query"
TRUNCATE TABLE redis_table SYNC;
```

Join：

与其他表进行 Join。

```sql title="Query"
SELECT * FROM redis_table JOIN merge_tree_table ON merge_tree_table.key=redis_table.key;
```

<div id="limitations">
  ## 局限性
</div>

Redis 引擎也支持扫描查询，例如 `where k > xx`，但它也有一些局限性：

1. 在极少数 rehash 期间，扫描查询可能会产生重复的键。详见 [Redis Scan](https://github.com/redis/redis/blob/e4d183afd33e0b2e6e8d1c79a832f678a04a7886/src/dict.c#L1186-L1269)。
2. 在扫描过程中，键可能会被创建和删除，因此得到的数据集无法反映某个有效的时间点。