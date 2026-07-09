---
description: '一种表函数，可高效地将以给定结构发送到服务器的数据转换后插入到具有另一种结构的表中。'
sidebar_label: 'input'
sidebar_position: 95
slug: /sql-reference/table-functions/input
title: 'input'
doc_type: 'reference'
---

`input(structure)` - 一种表函数，可高效地将以给定结构发送到服务器的数据转换后插入到具有另一种结构的表中。

`structure` - 发送到服务器的数据结构，格式如下：`'column1_name column1_type, column2_name column2_type, ...'`。
例如：`'id UInt32, name String'`。

此函数只能在 `INSERT SELECT` 查询中使用一次，但除此之外，其行为与普通表函数相同
(例如，它可以在子查询中使用等) 。

数据可以像普通 `INSERT` 查询一样以任意方式发送，并以任何可用[格式](/zh/sql-reference/formats)传递，
且必须在查询末尾指定该格式 (这与普通 `INSERT SELECT` 不同) 。

此函数的主要特点是，服务器从客户端接收数据时，会同时根据 `SELECT` 子句中的表达式列表对其进行转换，
并将其插入目标表。不会创建包含所有已传输数据的临时表。

<div id="examples">
  ## 示例
</div>

* 假设 `test` 表的结构如下：`(a String, b String)`
  而 `data.csv` 中的数据结构不同，为 `(col1 String, col2 Date, col3 Int32)`。将
  `data.csv` 中的数据插入到 `test` 表并同时进行转换的查询如下：

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

* 如果 `data.csv` 中的数据结构 `test_structure` 与表 `test` 相同，那么这两个查询是等价的：

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```