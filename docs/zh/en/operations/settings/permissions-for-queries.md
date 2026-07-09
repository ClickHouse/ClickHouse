---
description: '查询权限设置。'
sidebar_label: '查询权限'
sidebar_position: 58
slug: /operations/settings/permissions-for-queries
title: '查询权限'
doc_type: '参考'
---

ClickHouse 中的查询可分为以下几类：

1. 读取数据查询：`SELECT`、`SHOW`、`DESCRIBE`、`EXISTS`。
2. 写入数据查询：`INSERT`、`OPTIMIZE`。
3. 修改设置查询：`SET`、`USE`。
4. [DDL](https://en.wikipedia.org/wiki/Data_definition_language) 查询：`CREATE`、`ALTER`、`RENAME`、`ATTACH`、`DETACH`、`DROP`、`TRUNCATE`。
5. `KILL QUERY`。

以下设置用于按查询类型控制用户权限：

<div id="readonly">
  ## readonly
</div>

限制执行读取数据、写入数据和修改设置查询的权限。

当设置为 1 时，允许：

* 所有类型的读取查询 (如 SELECT 及其等效查询) 。
* 仅修改 session 上下文的查询 (如 USE) 。

当设置为 2 时，除上述之外，还允许：

* SET 和 CREATE TEMPORARY TABLE

  :::tip
  EXISTS、DESCRIBE、EXPLAIN、SHOW PROCESSLIST 等查询等同于 SELECT，因为它们本质上只是从系统表中进行 SELECT。
  :::

可选值：

* 0 — 允许读取数据、写入数据和修改设置查询。
* 1 — 仅允许读取数据查询。
* 2 — 允许读取数据和修改设置查询。

默认值：0

:::note
设置 `readonly = 1` 后，用户无法在当前 session 中更改 `readonly` 和 `allow_ddl` 设置。

在 [HTTP 接口](/zh/interfaces/http) 中使用 `GET` 方法时，会自动将 `readonly = 1`。要修改数据，请使用 `POST` 方法。

设置 `readonly = 1` 会禁止用户更改设置。不过，也可以只禁止用户更改特定设置；也可以在 `readonly = 1` 的限制下，仅允许更改特定设置。详情请参见[设置约束](../../operations/settings/constraints-on-settings.md)。
:::

<div id="allow_ddl">
  ## allow_ddl
</div>

允许或拒绝 [DDL](https://en.wikipedia.org/wiki/Data_definition_language) 查询。

可能的值：

* 0 — 不允许执行 DDL 查询。
* 1 — 允许执行 DDL 查询。

默认值：1

:::note
如果当前会话的 `allow_ddl = 0`，则无法运行 `SET allow_ddl = 1`。
:::

:::note KILL QUERY
无论 readonly 和 allow&#95;ddl 如何组合设置，都可以执行 `KILL QUERY`。
:::