---
description: '用于 JOIN 操作的可选预先准备的数据结构。'
sidebar_label: 'Join'
sidebar_position: 70
slug: /engines/table-engines/special/join
title: 'Join 表引擎'
doc_type: 'reference'
---

用于 [JOIN](/zh/sql-reference/statements/select/join) 操作的可选预先准备的数据结构。

:::note
在 ClickHouse Cloud 中，如果您的服务是在早于 25.4 的版本中创建的，则需要使用 `SET compatibility=25.4` 将兼容性设置为至少 25.4。
:::

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

有关 [CREATE TABLE](/zh/sql-reference/statements/create/table) 查询的详细说明，请参阅相应文档。

<div id="engine-parameters">
  ## 引擎参数
</div>

<div id="join_strictness">
  ### `join_strictness`
</div>

`join_strictness` – [JOIN 严格度](/zh/sql-reference/statements/select/join#supported-types-of-join).

<div id="join_type">
  ### `join_type`
</div>

`join_type` – [JOIN 类型](/zh/sql-reference/statements/select/join#supported-types-of-join).

<div id="key-columns">
  ### 键列
</div>

`k1[, k2, ...]` – `JOIN` 操作所使用的 `USING` 子句中的键列。

输入 `join_strictness` 和 `join_type` 参数时不要加引号，例如 `Join(ANY, LEFT, col1)`。它们必须与该表要用于的 `JOIN` 操作相匹配。如果这些参数不匹配，ClickHouse 不会抛出异常，但可能会返回错误数据。

<div id="specifics-and-recommendations">
  ## 具体说明与建议
</div>

<div id="data-storage">
  ### 数据存储
</div>

`Join` 表的数据始终位于 RAM 中。向表中插入行时，ClickHouse 会将数据块写入磁盘上的目录，以便在服务器重启后恢复这些数据。

如果服务器异常重启，磁盘上的数据块可能会丢失或损坏。此时，您可能需要手动删除包含损坏数据的文件。

<div id="selecting-and-inserting-data">
  ### 选择和插入数据
</div>

你可以使用 `INSERT` 查询向 `Join` 引擎 表中添加数据。如果表在创建时使用的是 `ANY` 严格度，则会忽略重复键的数据。使用 `ALL` 严格度 时，则会添加所有行。

`Join` 引擎 表的主要用例如下：

* 将该表放在 `JOIN` 子句的右侧。
* 调用 [joinGet](/zh/sql-reference/functions/other-functions.md/#joinGet) 函数，这样你就可以像从字典中提取数据一样，从该表中提取数据。

<div id="deleting-data">
  ### 删除数据
</div>

对于使用 `Join` 引擎的表，`ALTER DELETE` 查询是作为 [变更](/zh/sql-reference/statements/alter/index.md#mutations) 实现的。`DELETE` 变更会读取过滤后的数据，并覆盖内存和磁盘中的数据。

<div id="join-limitations-and-settings">
  ### 限制和设置
</div>

创建表时，将应用以下设置：

<div id="join_use_nulls">
  #### `join_use_nulls`
</div>

[join&#95;use&#95;nulls](/zh/operations/settings/settings.md/#join_use_nulls)

<div id="max_rows_in_join">
  #### `max_rows_in_join`
</div>

[max&#95;rows&#95;in&#95;join](/zh/operations/settings/settings#max_rows_in_join)

<div id="max_bytes_in_join">
  #### `max_bytes_in_join`
</div>

[max&#95;bytes&#95;in&#95;join](/zh/operations/settings/settings#max_bytes_in_join)

<div id="join_overflow_mode">
  #### `join_overflow_mode`
</div>

[join&#95;overflow&#95;mode](/zh/operations/settings/settings#join_overflow_mode)

<div id="join_any_take_last_row">
  #### `join_any_take_last_row`
</div>

[join&#95;any&#95;take&#95;last&#95;row](/zh/operations/settings/settings.md/#join_any_take_last_row)

<div id="join_use_nulls">
  #### `join_use_nulls`
</div>

<div id="persistent">
  #### 持久化
</div>

禁用 Join 和 [Set](/zh/engines/table-engines/special/set.md) 表引擎的持久化功能。

可减少 I/O 开销。适用于追求性能且不需要持久化的场景。

可选值：

* 1 — 启用。
* 0 — 禁用。

默认值：`1`。

`Join` 引擎表不能用于 `GLOBAL JOIN` 操作。

`Join` 引擎允许在 `CREATE TABLE` 语句中指定 [join&#95;use&#95;nulls](/zh/operations/settings/settings.md/#join_use_nulls) 设置。[SELECT](/zh/sql-reference/statements/select/index.md) 查询应使用相同的 `join_use_nulls` 值。

<div id="example">
  ## 使用示例
</div>

创建左侧表：

```sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog;
```

```sql
INSERT INTO id_val VALUES (1,11), (2,12), (3,13);
```

创建右侧的 `Join` 表：

```sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);
```

```sql
INSERT INTO id_val_join VALUES (1,21), (1,22), (3,23);
```

连接表：

```sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id);
```

```text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │               0 │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

或者，你可以通过指定连接键值从 `Join` 表中检索数据：

```sql
SELECT joinGet('id_val_join', 'val', toUInt32(1));
```

```text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

删除 `Join` 表中的一行：

```sql
ALTER TABLE id_val_join DELETE WHERE id = 3;
```

```text
┌─id─┬─val─┐
│  1 │  21 │
└────┴─────┘
```