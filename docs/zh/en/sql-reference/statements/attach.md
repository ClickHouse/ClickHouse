---
description: 'ATTACH 文档'
sidebar_label: 'ATTACH'
sidebar_position: 40
slug: /sql-reference/statements/attach
title: 'ATTACH 语句'
doc_type: 'reference'
---

将现有表或字典附加到服务器中，例如在将数据库迁移到另一台服务器时。

**语法**

```sql
ATTACH TABLE|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

该查询不会在磁盘上创建数据，而是假定数据已经位于适当的位置，只会将指定表、字典或数据库的信息添加到服务器中。执行 `ATTACH` 查询后，服务器就会识别该表、字典或数据库的存在。

如果某个表此前已被分离 ([DETACH](../../sql-reference/statements/detach.md) 查询) ，也就是说其结构已为服务器所知，则可以使用简写形式而无需定义结构。

<div id="attach-existing-table">
  ## 附加现有表
</div>

**语法**

```sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

此查询用于服务器启动时。服务器会将表的元数据存储为包含 `ATTACH` 查询的文件，并在启动时直接执行这些查询 (某些系统表除外，这些表会在服务器上显式创建) 。

如果该表已被永久分离，则不会在服务器启动时重新附加，因此你需要显式使用 `ATTACH` 查询。

<div id="create-new-table-and-attach-data">
  ## 创建新表并附加数据
</div>

<div id="with-specified-path-to-table-data">
  ### 使用指定的表数据路径
</div>

该查询会按给定的结构创建一个新表，并将 `user_files` 中指定目录里的表数据附加到该表。

**语法**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**示例**

```sql title="Query"
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```

```sql title="Response"
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

<div id="with-specified-table-uuid">
  ### 使用指定表 UUID
</div>

此查询会使用给定的结构创建一个新表，并挂载具有指定 UUID 的表中的数据。
[Atomic](../../engines/database-engines/atomic.md) 数据库引擎支持此功能。

**语法**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

<div id="attach-mergetree-table-as-replicatedmergetree">
  ## 将 MergeTree 表附加为 ReplicatedMergeTree
</div>

允许将非复制的 MergeTree 表附加为 ReplicatedMergeTree。系统会使用 `default_replica_path` 和 `default_replica_name` 设置的值来创建 ReplicatedMergeTree 表。也可以将复制表附加为普通的 MergeTree。

请注意，此查询不会影响 ZooKeeper 中该表的数据。这意味着，附加后你必须使用 `SYSTEM RESTORE REPLICA` 在 ZooKeeper 中添加元数据，或使用 `SYSTEM DROP REPLICA ... FROM ZKPATH ...` 将其清除。

如果你尝试为现有的 ReplicatedMergeTree 表添加副本，请注意，转换后的 MergeTree 表中的所有本地数据都会变为分离。

**语法**

```sql
ATTACH TABLE [db.]name AS [NOT] REPLICATED
```

**将表转换为启用复制的表**

```sql
DETACH TABLE test;
ATTACH TABLE test AS REPLICATED;
SYSTEM RESTORE REPLICA test;
```

**将表转换为非复制表**

获取该表的 ZooKeeper 路径和副本名称：

```sql title="Query"
SELECT replica_name, zookeeper_path FROM system.replicas WHERE table='test';
```

```sql title="Response"
┌─replica_name─┬─zookeeper_path─────────────────────────────────────────────┐
│ r1           │ /clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1 │
└──────────────┴────────────────────────────────────────────────────────────┘
```

以非复制方式附加表，并从 ZooKeeper 中删除该副本的数据：

```sql title="Query"
DETACH TABLE test;
ATTACH TABLE test AS NOT REPLICATED;
SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1';
```

<div id="attach-existing-dictionary">
  ## 附加现有字典
</div>

将先前已分离的字典重新附加。

**语法**

```sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

<div id="attach-existing-database">
  ## 附加现有数据库
</div>

将先前已分离的数据库重新附加。

**语法**

```sql
ATTACH DATABASE [IF NOT EXISTS] name [ENGINE=<database engine>] [ON CLUSTER cluster]
```