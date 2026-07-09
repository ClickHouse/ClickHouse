---
description: '允许访问集群的所有分片（在 `remote_servers` 部分中配置），
  无需创建 Distributed 表。'
sidebar_label: 'cluster'
sidebar_position: 30
slug: /sql-reference/table-functions/cluster
title: 'clusterAllReplicas'
doc_type: 'reference'
---

允许访问集群的所有分片 (在 `remote_servers` 部分中配置) ，而无需创建 [Distributed](../../engines/table-engines/special/distributed.md) 表。每个分片只查询一个副本。

`clusterAllReplicas` 函数——与 `cluster` 相同，但会查询所有副本。集群中的每个副本都会作为单独的分片/连接使用。

:::note
所有可用集群都列在 [system.clusters](../../operations/system-tables/clusters.md) 表中。
:::

<div id="syntax">
  ## 语法
</div>

```sql
cluster(['cluster_name', db.table, sharding_key])
cluster(['cluster_name', db, table, sharding_key])
clusterAllReplicas(['cluster_name', db.table, sharding_key])
clusterAllReplicas(['cluster_name', db, table, sharding_key])
```

<div id="arguments">
  ## 参数
</div>

| 参数                          | 类型                                            |
| --------------------------- | --------------------------------------------- |
| `cluster_name`              | 集群名称，用于构建远程和本地服务器的地址集合及连接参数；未指定时设为 `default`。 |
| `db.table` or `db`, `table` | 数据库名称和表名称。                                    |
| `sharding_key`              | 分片键。可选。如果集群有多个分片，则必须指定。                       |

<div id="returned_value">
  ## 返回值
</div>

来自各集群的数据集。

<div id="using_macros">
  ## 使用宏
</div>

`cluster_name` 可以包含宏，也就是 `{}` 中的替换内容。替换值取自服务器配置文件中的 [macros](../../operations/server-configuration-parameters/settings.md#macros) 部分。

示例：

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

<div id="usage_recommendations">
  ## 用法与建议
</div>

与创建 `Distributed` 表相比，使用 `cluster` 和 `clusterAllReplicas` 表函数的效率较低，因为在这种情况下，每次请求都会重新建立服务器连接。处理大量查询时，请务必提前创建 `Distributed` 表，不要使用 `cluster` 和 `clusterAllReplicas` 表函数。

在以下情况下，`cluster` 和 `clusterAllReplicas` 表函数可能会很有用：

* 访问特定集群以进行数据比较、调试和测试。
* 出于研究目的，查询不同的 ClickHouse 集群和副本。
* 手动发起的不频繁分布式请求。

诸如 `host`、`port`、`user`、`password`、`compression`、`secure` 等连接设置取自 `<remote_servers>` 配置部分。详见 [Distributed 引擎](../../engines/table-engines/special/distributed.md)。

<div id="related">
  ## 相关
</div>

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [load&#95;balancing](../../operations/settings/settings.md#load_balancing)