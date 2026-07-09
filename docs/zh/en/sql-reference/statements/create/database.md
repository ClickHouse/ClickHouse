---
description: 'CREATE DATABASE 文档'
sidebar_label: 'DATABASE'
sidebar_position: 35
slug: /sql-reference/statements/create/database
title: 'CREATE DATABASE'
doc_type: 'reference'
---

创建新数据库。

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [SETTINGS ...] [COMMENT 'Comment']
```

<div id="clauses">
  ## 子句
</div>

<div id="if-not-exists">
  ### IF NOT EXISTS
</div>

如果 `db_name` 数据库已存在，ClickHouse 将不会创建新数据库，并且：

* 如果指定了该子句，则不会抛出异常。
* 如果未指定该子句，则会抛出异常。

<div id="on-cluster">
  ### ON CLUSTER
</div>

ClickHouse 会在指定 cluster 的所有 server 上创建 `db_name` 数据库。更多详情请参阅 [Distributed DDL](../../../sql-reference/distributed-ddl.md) 一文。

<div id="engine">
  ### 引擎
</div>

默认情况下，ClickHouse 使用自己的 [Atomic](../../../engines/database-engines/atomic.md) 数据库引擎。此外，还有 [MySQL](../../../engines/database-engines/mysql.md)、[PostgresSQL](../../../engines/database-engines/postgresql.md)、[MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md)、[Replicated](../../../engines/database-engines/replicated.md)、[SQLite](../../../engines/database-engines/sqlite.md)。

<div id="comment">
  ### 注释
</div>

您可以在创建数据库时为其添加注释。

所有数据库引擎都支持注释。

**语法**

```sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**示例**

```sql title="Query"
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

```text title="Response"
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```

<div id="settings">
  ### 设置
</div>

<div id="lazy-load-tables">
  #### lazy_load_tables
</div>

启用后，数据库启动时不会完整加载各个表。系统会先为每个表创建一个轻量级代理，并在首次访问时将实际的表引擎 materialize。对于拥有大量表、但实际只会查询其中一部分的数据库，这可以减少启动时间和内存使用量。

```sql
CREATE DATABASE db_name ENGINE = Atomic SETTINGS lazy_load_tables = 1;
```

适用于将表元数据存储在磁盘上的数据库引擎 (例如 `Atomic`、`Ordinary`) 。无论此设置如何，视图、物化视图、字典以及基于表函数的表始终都会立即加载。

**何时使用：** 此设置适用于拥有大量表 (数百或数千张) ，但其中只有一部分会被频繁查询的数据库。它会将表引擎对象的创建、数据分区片段的扫描以及后台线程的初始化延后到首次访问时再进行，从而缩短服务器启动时间并降低内存使用量。

**对 `system.tables` 的影响：**

* 在表被访问之前，`system.tables` 会将其引擎显示为 `TableProxy`。首次访问后，则会显示真实的引擎名称 (例如 `MergeTree`) 。
* 对于未加载的表，`total_rows` 和 `total_bytes` 等列会返回 `NULL`，因为真实存储尚未创建。

**与 DDL 操作的交互：**

* `SELECT`、`INSERT`、`ALTER`、`DROP` 在首次使用时都会透明地触发真实表引擎的加载。
* `RENAME TABLE` 无需触发加载即可正常工作。
* 表一旦加载，就会在服务器进程的整个生命周期内保持已加载状态。

**限制：**

* 依赖 `system.tables` 元数据 (例如 `total_rows`、`engine`) 的监控工具，对于未加载的表可能只能看到不完整的信息。
* 对未加载表发起的第一次查询会带来一次性的加载开销 (解析已存储的 `CREATE TABLE` 语句并初始化引擎) 。

默认值：`0` (禁用) 。