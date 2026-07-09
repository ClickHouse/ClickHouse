---
description: 'Log 文档'
slug: /engines/table-engines/log-family/log
toc_priority: 33
toc_title: 'Log'
title: 'Log 表引擎'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine">
  # Log 表引擎
</div>

<CloudNotSupportedBadge />

该引擎属于 `Log` 引擎家族。有关 `Log` 引擎的通用属性及其差异，请参见 [Log Engine Family](../../../engines/table-engines/log-family/index.md) 一文。

`Log` 与 [TinyLog](../../../engines/table-engines/log-family/tinylog.md) 的区别在于，它会在列文件旁额外保存一个较小的“标记”文件。这些标记会在每个数据块写入时一并记录，其中包含偏移量，用于指示应从文件的哪个位置开始读取，以跳过指定数量的行。因此，可以使用多个线程读取表数据。
在并发数据访问场景下，读操作可以同时执行，而写操作会阻塞读操作以及彼此之间的执行。
`Log` 引擎不支持索引。同样，如果向表写入失败，表就会损坏，读取时会返回错误。`Log` 引擎适用于临时数据、只写入一次的表，以及测试或演示用途。

<div id="table_engines-log-creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Log
```

请参阅 [CREATE TABLE](/zh/sql-reference/statements/create/table) 查询的详细描述。

<div id="table_engines-log-writing-the-data">
  ## 写入数据
</div>

`Log` 引擎通过将每一列分别写入各自的文件来高效存储数据。对于每个表，Log 引擎都会将以下文件写入指定的存储路径：

* `<column>.bin`：每列对应的数据文件，包含经过序列化和压缩的数据。
  `__marks.mrk`：标记文件，用于存储每个已插入数据块的偏移量和行数。标记可帮助提高查询执行效率，使引擎在读取时能够跳过无关的数据块。

<div id="writing-process">
  ### 写入过程
</div>

当数据写入 `Log` 表时：

1. 数据会被序列化并压缩为块。
2. 对于每一列，压缩后的数据会追加到对应的 `<column>.bin` 文件中。
3. `__marks.mrk` 文件中会添加相应条目，用于记录新插入数据的偏移量和行数。

<div id="table_engines-log-reading-the-data">
  ## 读取数据
</div>

标记文件使 ClickHouse 能够并行读取数据。这意味着 `SELECT` 查询返回的行顺序不可预测。请使用 `ORDER BY` 子句对行进行排序。

<div id="table_engines-log-example-of-use">
  ## 使用示例
</div>

创建表：

```sql
CREATE TABLE log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = Log
```

插入数据：

```sql
INSERT INTO log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

我们使用了两条 `INSERT` 查询，在 `<column>.bin` 文件中创建了两个数据块。

ClickHouse 在查询数据时会使用多个线程。每个线程都会读取一个独立的数据块，并在完成后独立返回结果行。因此，输出中各个行块的顺序可能与输入中对应行块的顺序不一致。例如：

```sql
SELECT * FROM log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

对结果排序 (默认按升序) ：

```sql
SELECT * FROM log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```