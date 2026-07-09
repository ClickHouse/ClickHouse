---
description: '将子查询转换为表。该函数用于实现视图。'
sidebar_label: 'view'
sidebar_position: 210
slug: /sql-reference/table-functions/view
title: 'view'
doc_type: 'reference'
---

将子查询转换为表。该函数用于实现视图 (请参见 [CREATE VIEW](/zh/sql-reference/statements/create/view)) 。生成的表不存储数据，而只存储指定的 `SELECT` 查询。从该表读取数据时，ClickHouse 会执行该查询，并从结果中移除所有不必要的列。

<div id="syntax">
  ## 语法
</div>

```sql
view(subquery)
```

<div id="arguments">
  ## 参数
</div>

* `subquery` — `SELECT` 查询。

<div id="returned_value">
  ## 返回值
</div>

* 表。

<div id="examples">
  ## 示例
</div>

输入表：

```text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

```sql title="Query"
SELECT * FROM view(SELECT name FROM months);
```

```text title="Response"
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

你可以将 `view` 函数作为 [remote](/zh/sql-reference/table-functions/remote) 和 [cluster](/zh/sql-reference/table-functions/cluster) 表函数的参数使用：

```sql title="Query"
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

```sql title="Query"
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

<div id="related">
  ## 相关
</div>

* [View 表引擎](/zh/engines/table-engines/special/view/)