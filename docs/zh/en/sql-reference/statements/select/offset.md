---
description: 'OFFSET 文档'
sidebar_label: 'OFFSET'
slug: /sql-reference/statements/select/offset
title: 'OFFSET FETCH 子句'
doc_type: 'reference'
---

`OFFSET` 和 `FETCH` 允许你分批检索数据。它们指定了希望通过单个查询获取的一个行块。

```sql
-- SQL Standard style:
[OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]

-- MySQL/PostgreSQL style:
[LIMIT [n, ]m] [OFFSET offset_row_count]
```

`offset_row_count` 或 `fetch_row_count` 的值可以是数字或字面量常量。你可以省略 `fetch_row_count`；默认值为 1。

`OFFSET` 指定在开始返回查询结果集中的行之前要跳过的行数。`OFFSET n` 会跳过结果中的前 `n` 行。

支持负数 `OFFSET`：`OFFSET -n` 会跳过结果中的最后 `n` 行。

也支持小数 `OFFSET`：`OFFSET n` - 如果 0 &lt; n &lt; 1，则会跳过结果中前 n * 100% 的内容。

示例：
• `OFFSET 0.1` - 跳过结果中的前 10%。

> **注意**
> • 该小数必须是小于 1 且大于 0 的 [Float64](../../data-types/float.md) 数字。
> • 如果计算后得到的行数不是整数，则会向上取整到下一个整数。

`FETCH` 指定查询结果中最多可包含的行数。

`ONLY` 选项用于返回紧接在 `OFFSET` 省略的那些行之后的行。在这种情况下，`FETCH` 可作为 [LIMIT](../../../sql-reference/statements/select/limit.md) 子句的替代方案。例如，以下查询

```sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

与该查询完全相同

```sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

`WITH TIES` 选项用于返回结果集中根据 `ORDER BY` 子句并列最后一位的所有额外行。例如，如果 `fetch_row_count` 设为 5，但还有另外两行与第 5 行在 `ORDER BY` 列上的值相同，则结果集将包含 7 行。

:::note
根据标准，如果 `OFFSET` 子句和 `FETCH` 子句同时存在，则 `OFFSET` 子句必须位于 `FETCH` 子句之前。
:::

:::note
实际偏移量还可能取决于 [offset](../../../operations/settings/settings.md#offset) 设置。
:::

<div id="examples">
  ## 示例
</div>

输入表：

```text
┌─a─┬─b─┐
│ 1 │ 1 │
│ 2 │ 1 │
│ 3 │ 4 │
│ 1 │ 3 │
│ 5 │ 4 │
│ 0 │ 6 │
│ 5 │ 7 │
└───┴───┘
```

`ONLY` 选项的用法：

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

`WITH TIES` 选项的用法：

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```