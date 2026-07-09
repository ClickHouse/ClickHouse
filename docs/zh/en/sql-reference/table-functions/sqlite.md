---
description: '可对存储在 SQLite 数据库中的数据执行查询。'
sidebar_label: 'sqlite'
sidebar_position: 185
slug: /sql-reference/table-functions/sqlite
title: 'sqlite'
doc_type: 'reference'
---

可对存储在 [SQLite](../../engines/database-engines/sqlite.md) 数据库中的数据执行查询。

<div id="syntax">
  ## 语法
</div>

```sql
sqlite('db_path', 'table_name')
```

<div id="arguments">
  ## 参数
</div>

* `db_path` — SQLite 数据库文件的路径。[String](../../sql-reference/data-types/string.md)。
* `table_name` — SQLite 数据库中表的名称，或按原样传递给 SQLite 的查询语句 (参见[传递查询而不是表名](#passing-a-query)) 。[String](../../sql-reference/data-types/string.md)。

<div id="returned_value">
  ## 返回值
</div>

* 一个表对象，其列与原始 `SQLite` 表相同。

<div id="passing-a-query">
  ## 传入查询而不是表名
</div>

第二个参数可以不是表名，而是一个原样传递给 SQLite 的 `SELECT` 查询。结果表的结构会根据查询结果自动推断。该查询既可以写成子查询，也可以封装在 `query` 函数中：

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

这样的表是只读的：不允许对其执行 `INSERT`。[`SQLite`](/zh/engines/table-engines/integrations/sqlite) 表引擎也支持相同的语法。

:::note
子查询形式 `(SELECT ...)` 会先由 ClickHouse 解析，并在发送到 SQLite 之前重新序列化。因此，它必须是有效的 ClickHouse SQL。若要传递 ClickHouse 无法解析的 SQLite 特有语法，请使用 `query('...')` 形式，其文本会原样发送到 SQLite。

外围 ClickHouse 查询中的任何外层 `WHERE`、`LIMIT`、聚合等，**都不会**下推到传递的查询中——而是在拉取完整查询结果后由 ClickHouse 应用。要限制从 SQLite 读取的数据，请将过滤器放在传递的查询内部。启用 [`external_table_strict_query = 1`](/zh/operations/settings/settings#external_table_strict_query) 时，无法下推的外层过滤器会被直接拒绝并抛出异常，而不是在本地执行。
:::

<div id="example">
  ## 示例
</div>

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

<div id="related">
  ## 相关
</div>

* [SQLite](../../engines/table-engines/integrations/sqlite.md) 表引擎
* [SQLite 数据库引擎](../../engines/database-engines/sqlite.md) — 数据类型支持章节