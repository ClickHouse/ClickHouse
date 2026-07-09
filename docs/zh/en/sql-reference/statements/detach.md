---
description: 'DETACH 文档'
sidebar_label: 'DETACH'
sidebar_position: 43
slug: /sql-reference/statements/detach
title: 'DETACH 语句'
doc_type: 'reference'
---

使服务器“忘记”表、materialized view、字典或数据库的存在。

**语法**

```sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

分离不会删除表、materialized view、字典或数据库的数据或元数据。如果某个实体不是以 `PERMANENTLY` 方式分离的，那么服务器下次启动时会读取元数据，并重新加载该表/视图/字典/数据库。如果某个实体是以 `PERMANENTLY` 方式分离的，则不会自动重新加载。

无论表、字典还是数据库是否被永久分离，在这两种情况下，你都可以使用 [ATTACH](../../sql-reference/statements/attach.md) 查询将其重新附加。系统日志表也可以重新附加 (例如 `query_log`、`text_log` 等) 。其他系统表不能重新附加。服务器下次启动时会再次加载这些表。

`ATTACH MATERIALIZED VIEW` 不支持简写语法 (不带 `SELECT`) ，但你可以使用 `ATTACH TABLE` 查询来附加它。

请注意，不能将已经处于分离 (临时) 状态的表再次永久分离。不过，你可以先将其重新附加，然后再以永久方式分离。

另外，不能对已分离的表执行 [DROP](../../sql-reference/statements/drop.md#drop-table)，也不能使用与已永久分离的表相同的名称执行 [CREATE TABLE](../../sql-reference/statements/create/table.md)，或者通过 [RENAME TABLE](../../sql-reference/statements/rename.md) 查询用另一张表替换它。

`SYNC` 修饰符会立即执行该操作，不会延迟。

**示例**

创建表：

```sql title="Query"
CREATE TABLE test ENGINE = MergeTree ORDER BY () AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

```text title="Response"
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

分离表：

```sql title="Query"
DETACH TABLE test;
SELECT * FROM test;
```

```text title="Response"
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

:::note
在 ClickHouse Cloud 中，用户应使用 `PERMANENTLY` 子句，例如 `DETACH TABLE <table> PERMANENTLY`。如果不使用该子句，表会在集群重启时被重新附加，例如在升级期间。
:::

**另请参见**

* [Materialized View](/zh/sql-reference/statements/create/view#materialized-view)
* [字典](./create/dictionary/overview.md)