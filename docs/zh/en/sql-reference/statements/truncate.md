---
description: 'TRUNCATE 语句文档'
sidebar_label: 'TRUNCATE'
sidebar_position: 52
slug: /sql-reference/statements/truncate
title: 'TRUNCATE 语句'
doc_type: 'reference'
---

ClickHouse 中的 `TRUNCATE` 语句用于在保留表或数据库结构的同时，快速清空其中的所有数据。

<div id="truncate-table">
  ## TRUNCATE TABLE
</div>

```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<br />

| 参数                   | 说明                                     |
| -------------------- | -------------------------------------- |
| `IF EXISTS`          | 如果表不存在，则不会报错。如果省略，查询会返回错误。             |
| `db.name`            | 可选的数据库名称。                              |
| `ON CLUSTER cluster` | 在指定集群上执行该命令。                           |
| `SYNC`               | 使用复制表时，使截断操作在各副本之间同步进行。如果省略，则默认异步执行截断。 |

你可以使用 [alter&#95;sync](/zh/operations/settings/settings#alter_sync) 设置等待副本执行操作的方式。

你可以使用 [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/zh/operations/settings/settings#replication_wait_for_inactive_replica_timeout) 设置等待非活动副本执行 `TRUNCATE` 查询的时长 (以秒为单位) 。

:::note
如果 `alter_sync` 设置为 `2`，且某些副本非活动状态持续的时间超过 `replication_wait_for_inactive_replica_timeout` 设置指定的时长，则会抛出 `UNFINISHED` 异常。
:::

`TRUNCATE TABLE` 查询**不支持**以下表引擎：

* [`View`](../../engines/table-engines/special/view.md)
* [`File`](../../engines/table-engines/special/file.md)
* [`URL`](../../engines/table-engines/special/url.md)
* [`Buffer`](../../engines/table-engines/special/buffer.md)
* [`Null`](../../engines/table-engines/special/null.md)

<div id="truncate-all-tables">
  ## TRUNCATE 所有表
</div>

```sql
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
```

<br />

| Parameter                               | Description     |
| --------------------------------------- | --------------- |
| `ALL`                                   | 删除数据库中所有表的数据。   |
| `IF EXISTS`                             | 如果数据库不存在，则避免报错。 |
| `db`                                    | 数据库名称。          |
| `LIKE \| ILIKE \| NOT LIKE '<pattern>'` | 按模式过滤表。         |
| `ON CLUSTER cluster`                    | 在整个集群上运行该命令。    |

删除数据库中所有表的数据。

<div id="truncate-database">
  ## TRUNCATE DATABASE
</div>

```sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

<br />

| 参数                   | 描述              |
| -------------------- | --------------- |
| `IF EXISTS`          | 如果数据库不存在，则不会报错。 |
| `db`                 | 数据库名称。          |
| `ON CLUSTER cluster` | 在指定集群上运行该命令。    |

删除数据库中的所有表，但保留数据库本身。省略 `IF EXISTS` 子句时，如果数据库不存在，查询将返回错误。

:::note
`TRUNCATE DATABASE` 不支持 `Replicated` 数据库。请改为直接对该数据库执行 `DROP` 和 `CREATE`。
:::