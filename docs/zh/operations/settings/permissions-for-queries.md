---
toc_priority: 58
toc_title: "\u67E5\u8BE2\u6743\u9650"
---

# 查询权限 {#permissions_for_queries}

ClickHouse中的查询可以分为几种类型:

1.  读取数据的查询： `SELECT` ， `SHOW` ， `DESCRIBE` ， `EXISTS` 。
2.  写入数据的查询： `INSERT` ， `OPTIMIZE` 。
3.  更改设置的查询： `SET` ， `USE` 。
4.  [DDL](https://zh.wikipedia.org/zh-cn/数据定义语言) 查询: `CREATE` ， `ALTER` ， `RENAME` ， `ATTACH` ， `DETACH` ， `DROP` ， `TRUNCATE` 。
5.  `KILL QUERY` 。

以下设置按查询类型规范用户权限:

-   [readonly](#settings_readonly) — 对除 DDL 查询以外的所有类型限制权限。
-   [allow_ddl](#settings_allow_ddl) —  对 DDL 查询限制权限。

`KILL QUERY` 可以与任何设置进行。

## readonly {#settings_readonly}

限制读取数据、写入数据和更改设置查询的权限。

查看查询如何划分为 **[上述](#permissions_for_queries)** 的多种类型。

可能的值:

-   0 — 所有查询都被允许。
-   1 — 只有读取数据的查询被允许。
-   2 — 读取数据以及变更设置的查询被允许。

设置为 `readonly = 1` 后，用户无法在当前会话中更改 `readonly` 和 `allow_ddl` 设置。

当使用 [HTTP接口](../../interfaces/http.md) 中的 `GET` 方法时，将自动设置为 `readonly = 1` 。 要修改数据，请使用 `POST` 方法。

设置 `readonly = 1` 将禁止用户的更改任何设置。有一种方法可以只禁止用户更改特定的设置，有关详细信息，请参阅 [对设置的限制](constraints-on-settings.md)。

默认值：0

## allow_ddl {#settings_allow_ddl}

允许或拒绝 [DDL](https://zh.wikipedia.org/zh-cn/数据定义语言) 查询。

从 [上文](#permissions_for_queries) 查看查询是如何被划分为多种类型的。

可能的值:

-   0 — DDL 查询不被允许。
-   1 — DDL 查询被允许。

如果对当前会话 `allow_ddl = 0` ，你就不能执行 `SET allow_ddl = 1` 。

默认值：1

[原始文章](https://clickhouse.com/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
