---
description: '用于显示当前由此 ClickHouse 服务器 注册的活动 ZooKeeper watch 的系统表。'
keywords: ['系统表', 'zookeeper_watches']
slug: /operations/system-tables/zookeeper_watches
title: 'system.zookeeper_watches'
doc_type: 'reference'
---

<div id="description">
  ## 描述
</div>

显示此 ClickHouse 服务器当前在 ZooKeeper 节点 (包括辅助 ZooKeeper 实例) 上注册的活动 [watch](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches)。每一行对应一个 watch。

<div id="columns">
  ## 列
</div>

* `zookeeper_name` ([String](../../sql-reference/data-types/string.md)) — ZooKeeper 连接名称 (主连接为 `default`，或为辅助连接名称) 。
* `create_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — 创建 watch 的时间。
* `create_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — 以微秒精度记录的 watch 创建时间。
* `path` ([String](../../sql-reference/data-types/string.md)) — 被监视的 ZooKeeper 路径。
* `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — 注册该 watch 的连接的会话 ID。
* `request_xid` ([Int64](../../sql-reference/data-types/int-uint.md)) — 创建该 watch 的请求的 XID。
* `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — 创建该 watch 的请求类型。
* `watch_type` ([Enum8](../../sql-reference/data-types/enum.md)) — watch 类型。可能的值：
  * `Children` — 监视子节点列表的变化 (由 `List` 操作设置) 。
  * `Exists` — 监视节点的创建或删除。
  * `Data` — 监视节点数据的变化 (由 `Get` 操作设置) 。

示例：

```sql
SELECT * FROM system.zookeeper_watches FORMAT Vertical;
```

```text
Row 1:
──────
zookeeper_name:           default
create_time:              2026-03-16 12:00:00
create_time_microseconds: 2026-03-16 12:00:00.123456
path:                     /clickhouse/task_queue/ddl
session_id:               106662742089334927
request_xid:              10858
op_num:                   List
watch_type:               Children
```

**另请参阅**

* [ZooKeeper](../../operations/tips.md#zookeeper)
* [ZooKeeper 指南](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)