---
description: 'StripeLog 表引擎的文档'
slug: /engines/table-engines/log-family/stripelog
toc_priority: 32
toc_title: 'StripeLog'
title: 'StripeLog 表引擎'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="stripelog-table-engine">
  # StripeLog 表引擎
</div>

<CloudNotSupportedBadge />

该引擎属于日志引擎家族。有关日志引擎的通用属性及其差异，请参阅[Log 引擎家族](../../../engines/table-engines/log-family/index.md)一文。

当您需要写入大量仅包含少量数据 (少于 100 万行) 的表时，可使用此引擎。例如，该表可用于存储待转换的传入数据批次，并且要求对这些批次进行原子处理。ClickHouse server 可以支持 10 万个这种类型的表实例。当需要大量表时，相比 [Log](./log.md)，应优先使用此表引擎，但代价是读取效率会降低。

<div id="table_engines-stripelog-creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

请参阅 [CREATE TABLE](/zh/sql-reference/statements/create/table) 查询的详细说明。

<div id="table_engines-stripelog-writing-the-data">
  ## 写入数据
</div>

`StripeLog` 引擎将所有列存储在一个文件中。对于每个 `INSERT` 查询，ClickHouse 都会将数据块追加到表文件末尾，并按列逐一写入。

对于每个表，ClickHouse 会写入以下文件：

* `data.bin` — 数据文件。
* `index.mrk` — 标记文件。标记包含每个已插入数据块中每一列的偏移量。

`StripeLog` 引擎不支持 `ALTER UPDATE` 和 `ALTER DELETE` 操作。

<div id="table_engines-stripelog-reading-the-data">
  ## 读取数据
</div>

标记文件使 ClickHouse 能够并行读取数据。这意味着 `SELECT` 查询返回的行顺序是不可预测的。使用 `ORDER BY` 子句对行进行排序。

<div id="table_engines-stripelog-example-of-use">
  ## 使用示例
</div>

创建表：

```sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

插入数据：

```sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

我们使用了两条 `INSERT` 查询，在 `data.bin` 文件中创建了两个数据块。

ClickHouse 在查询数据时会使用多个线程。每个线程都会读取一个独立的数据块，并在完成后分别返回结果行。因此，在大多数情况下，输出中各个数据块的行顺序与输入中相应数据块的行顺序并不一致。例如：

```sql
SELECT * FROM stripe_log_table
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

对结果排序 (默认升序) ：

```sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```