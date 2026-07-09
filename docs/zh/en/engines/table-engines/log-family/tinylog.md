---
description: 'TinyLog 表引擎文档'
slug: /engines/table-engines/log-family/tinylog
toc_priority: 34
toc_title: 'TinyLog'
title: 'TinyLog 表引擎'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="tinylog-table-engine">
  # TinyLog 表引擎
</div>

<CloudNotSupportedBadge />

该引擎属于日志引擎家族。有关日志引擎的通用属性及其差异，请参见 [Log Engine Family](../../../engines/table-engines/log-family/index.md)。

这种表引擎通常采用一次写入的方式：数据写入一次，然后根据需要读取多次。例如，`TinyLog` 类型的表可用于存放以小批次处理的中间数据。请注意，将数据存储在大量小表中效率较低。

查询在单个 stream 中执行。换句话说，这种引擎适用于相对较小的表 (最多约 1,000,000 行) 。如果你有许多小表，使用这种表引擎是合理的，因为它比 [Log](../../../engines/table-engines/log-family/log.md) 引擎更简单 (需要打开的文件更少) 。

<div id="characteristics">
  ## 特性
</div>

* **结构更简单**：与 Log 引擎不同，TinyLog 不使用标记文件。这降低了复杂度，但也限制了针对大型数据集的性能优化能力。
* **单个 stream 查询**：TinyLog 表上的查询以单个 stream 执行，因此适用于相对较小的表，通常最多可达 1,000,000 行。
* **更适合小型表**：TinyLog 引擎结构简单，因此在管理大量小型表时更具优势，因为与 Log 引擎相比，它所需的文件操作更少。

与 Log 引擎不同，TinyLog 不使用标记文件。这降低了复杂度，但也限制了针对更大数据集的性能优化能力。

<div id="table_engines-tinylog-creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = TinyLog
```

请参阅 [CREATE TABLE](/zh/sql-reference/statements/create/table) 查询的详细描述。

<div id="table_engines-tinylog-writing-the-data">
  ## 写入数据
</div>

`TinyLog` 引擎将所有列存储在同一个文件中。对于每个 `INSERT` 查询，ClickHouse 都会将数据块追加到表文件末尾，并按列逐一写入。

对于每个表，ClickHouse 会写入以下文件：

* `<column>.bin`：每列对应的数据文件，包含序列化并压缩后的数据。

`TinyLog` 引擎不支持 `ALTER UPDATE` 和 `ALTER DELETE` 操作。

<div id="table_engines-tinylog-example-of-use">
  ## 使用示例
</div>

创建表：

```sql
CREATE TABLE tiny_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = TinyLog
```

插入数据：

```sql
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

我们使用了两条 `INSERT` 查询，在 `<column>.bin` 文件中创建了两个数据块。

ClickHouse 以单个 stream 读取数据。因此，输出中各行块的顺序与输入中对应块的顺序一致。例如：

```sql
SELECT * FROM tiny_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2024-12-10 13:11:58 │ REGULAR      │ The first regular message  │
│ 2024-12-10 13:12:12 │ REGULAR      │ The second regular message │
│ 2024-12-10 13:12:12 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```