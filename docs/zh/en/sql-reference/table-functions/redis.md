---
description: '此表函数可将 ClickHouse 与 Redis 集成。'
sidebar_label: 'redis'
sidebar_position: 170
slug: /sql-reference/table-functions/redis
title: 'redis'
doc_type: 'reference'
---

此表函数可将 ClickHouse 与 [Redis](https://redis.io/) 集成。

<div id="syntax">
  ## 语法
</div>

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

<div id="arguments">
  ## 参数
</div>

| Argument    | Description                                    |
| ----------- | ---------------------------------------------- |
| `host:port` | Redis server 地址，可以省略端口，此时将使用默认的 Redis 端口 6379。 |
| `key`       | 列列表中的任意列名。                                     |
| `structure` | 此函数返回的 ClickHouse 表的 schema。                   |
| `db_index`  | Redis 数据库索引范围为 0 到 15，默认为 0。                   |
| `password`  | 用户密码，默认为空字符串。                                  |
| `pool_size` | Redis 最大连接池大小，默认为 16。                          |
| `primary`   | 必须指定，且仅支持主键包含一个列。主键将以二进制形式序列化为 Redis 键。        |

* 除主键外的其他列会按对应顺序以二进制形式序列化为 Redis 值。
* 对 key 使用 equals 或 in 进行筛选的 queries，会优化为从 Redis 执行多键 lookup。如果 queries 未按 key 进行筛选，则会发生 full table scan，这是一项开销很大的操作。

目前，`redis` 表函数暂不支持 [named collections](/zh/operations/named-collections.md)。

<div id="returned_value">
  ## 返回值
</div>

一个表对象，其中 key 作为 Redis 键，其他列共同组成 Redis 值。

<div id="usage-example">
  ## 使用示例
</div>

读取 Redis：

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

插入 Redis：

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

<div id="related">
  ## 相关
</div>

* [`Redis` 表引擎](/zh/engines/table-engines/integrations/redis.md)
* [将 Redis 用作字典源](/zh/sql-reference/statements/create/dictionary/sources/redis)