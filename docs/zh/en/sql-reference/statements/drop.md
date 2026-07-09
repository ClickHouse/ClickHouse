---
description: 'DROP 语句文档'
sidebar_label: 'DROP'
sidebar_position: 44
slug: /sql-reference/statements/drop
title: 'DROP 语句'
doc_type: 'reference'
---

删除现有对象。如果指定了 `IF EXISTS` 子句，则即使对象不存在，这些查询也不会返回错误。如果指定了 `SYNC` 修饰符，则会立即删除该对象。

<div id="drop-database">
  ## DROP DATABASE
</div>

先删除 `db` 数据库中的所有表，然后删除 `db` 数据库本身。

语法：

```sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

<div id="drop-table">
  ## DROP TABLE
</div>

删除一个或多个表。

:::tip
要恢复已删除的表，请参阅 [UNDROP TABLE](/zh/sql-reference/statements/undrop.md)
:::

语法：

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

限制：

* 如果指定了 `IF EMPTY` 子句，服务器只会在接收到查询的副本上检查该表是否为空。
* 一次删除多个表不是原子操作，也就是说，如果某个表删除失败，后续的表将不会被删除。

<div id="drop-dictionary">
  ## DROP DICTIONARY
</div>

删除该字典。

语法：

```sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

<div id="drop-user">
  ## DROP USER
</div>

删除用户。

语法：

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-role">
  ## DROP ROLE
</div>

删除角色。已删除的角色会从所有被分配了该角色的实体中撤销。

语法：

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-row-policy">
  ## DROP ROW POLICY
</div>

删除 ROW POLICY。已删除的 ROW POLICY 会从所有被分配到该策略的实体中撤销。

语法：

```sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-masking-policy">
  ## DROP MASKING POLICY
</div>

删除数据脱敏策略。

语法：

```sql
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-quota">
  ## DROP QUOTA
</div>

删除 QUOTA。已删除的 QUOTA 会从所有已分配给相关实体的对象中撤销。

语法：

```sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-settings-profile">
  ## DROP SETTINGS PROFILE
</div>

删除设置 profile。已删除的设置 profile 会从所有已分配该 profile 的对象中移除。

语法：

```sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-view">
  ## DROP VIEW
</div>

删除视图。也可以使用 `DROP TABLE` 命令删除视图，但 `DROP VIEW` 会检查 `[db.]name` 是否是视图。

语法：

```sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<div id="drop-function">
  ## DROP FUNCTION
</div>

删除由 [CREATE FUNCTION](./create/function.md) 创建的用户自定义函数。
系统函数不可删除。

**语法**

```sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**示例**

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

<div id="drop-named-collection">
  ## DROP NAMED COLLECTION
</div>

删除命名集合。

**语法**

```sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**示例**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```