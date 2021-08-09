---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "\u67E5\u8BE2\u6743\u9650"
---

# 查询权限 {#permissions_for_queries}

ClickHouse中的查询可以分为几种类型:

1.  读取数据查询: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  写入数据查询: `INSERT`, `OPTIMIZE`.
3.  更改设置查询: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) 查询: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

以下设置按查询类型规范用户权限:

-   [只读](#settings_readonly) — Restricts permissions for all types of queries except DDL queries.
-   [allow_ddl](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` 可以与任何设置进行。

## 只读 {#settings_readonly}

限制读取数据、写入数据和更改设置查询的权限。

查看查询如何划分为多种类型 [以上](#permissions_for_queries).

可能的值:

-   0 — All queries are allowed.
-   1 — Only read data queries are allowed.
-   2 — Read data and change settings queries are allowed.

设置后 `readonly = 1`，用户无法更改 `readonly` 和 `allow_ddl` 当前会话中的设置。

使用时 `GET` 方法中的 [HTTP接口](../../interfaces/http.md), `readonly = 1` 自动设置。 要修改数据，请使用 `POST` 方法。

设置 `readonly = 1` 禁止用户更改所有设置。 有一种方法可以禁止用户
从只更改特定设置，有关详细信息，请参阅 [对设置的限制](constraints-on-settings.md).

默认值：0

## allow_ddl {#settings_allow_ddl}

允许或拒绝 [DDL](https://en.wikipedia.org/wiki/Data_definition_language) 查询。

查看查询如何划分为多种类型 [以上](#permissions_for_queries).

可能的值:

-   0 — DDL queries are not allowed.
-   1 — DDL queries are allowed.

你不能执行 `SET allow_ddl = 1` 如果 `allow_ddl = 0` 对于当前会话。

默认值：1

[原始文章](https://clickhouse.tech/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
