---
description: 'LIMIT 子句文档'
sidebar_label: 'LIMIT'
slug: /sql-reference/statements/select/limit
title: 'LIMIT 子句'
doc_type: 'reference'
---

`LIMIT` 子句用于控制查询结果返回的行数。

<div id="basic-syntax">
  ## 基本语法
</div>

**查询前几行：**

```sql
LIMIT m
```

返回结果中的前 `m` 行；如果记录数少于 `m`，则返回所有记录。

**TOP 的替代语法 (与 MS SQL Server 兼容) ：**

```sql
-- SELECT TOP number|percent column_name(s) FROM table_name
SELECT TOP 10 * FROM numbers(100);
SELECT TOP 0.1 * FROM numbers(100);
```

这相当于 `LIMIT m`，可用于兼容 Microsoft SQL Server 的查询。

**带 OFFSET 的 SELECT：**

```sql
LIMIT m OFFSET n
-- or equivalently:
LIMIT n, m
```

跳过前 `n` 行，然后返回后续 `m` 行。

在这两种形式下，`n` 和 `m` 都必须是非负整数。

<div id="negative-limits">
  ## 负数LIMIT
</div>

使用负值从结果集的*末尾*选择行：

| 语法                   | 结果                   |
| -------------------- | -------------------- |
| `LIMIT -m`           | 最后 `m` 行             |
| `LIMIT -m OFFSET -n` | 跳过最后 `n` 行后取最后 `m` 行 |
| `LIMIT m OFFSET -n`  | 跳过最后 `n` 行后取前 `m` 行  |
| `LIMIT -m OFFSET n`  | 跳过前 `n` 行后取最后 `m` 行  |

`LIMIT -n, -m` 语法等同于 `LIMIT -m OFFSET -n`。

<div id="fractional-limits">
  ## 小数 LIMIT
</div>

使用 0 到 1 之间的小数值来选择一定比例的行：

| 语法                      | 结果                          |
| ----------------------- | --------------------------- |
| `LIMIT 0.1`             | 前 10% 的行                    |
| `LIMIT 1 OFFSET 0.5`    | 中间那一行                       |
| `LIMIT 0.25 OFFSET 0.5` | 第三四分位区间 (跳过前 50% 后的 25% 行)  |

:::note

* 这些小数必须是大于 0 且小于 1 的 [Float64](../../data-types/float.md) 值。
* 按小数计算得到的行数会向上取整为下一个整数。
  :::

<div id="combining-limit-types">
  ## 组合使用不同的 LIMIT 类型
</div>

你可以将标准整数与小数偏移量或负偏移量混合使用：

```sql
LIMIT 10 OFFSET 0.5    -- 10 rows starting from the halfway point
LIMIT 10 OFFSET -20    -- 10 rows after skipping the last 20
```

<div id="limit--with-ties-modifier">
  ## LIMIT ... WITH TIES
</div>

`WITH TIES` 修饰符会将与限定结果中最后一行具有相同 `ORDER BY` 值的其他行一并包含在内。

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

使用 `WITH TIES` 时，将包含所有与最后一个值相同的行：

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5 WITH TIES
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

第 6 行也会包含在结果中，因为它与第 5 行的值相同 (`2`) 。

使用 `OFFSET` 关键字指定偏移量时，情况也是一样：

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 3 OFFSET 2 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

跳过前 2 行并取 3 行通常会返回 `1, 1, 2`，但由于第二个 `2` 与最后一行并列，因此也会被包含在结果中。

`WITH TIES` 也适用于负 LIMIT 和偏移量。它会包含与所选第一行具有相同 `ORDER BY` 值的其他行：

```sql
SELECT number % 3 AS n FROM numbers(15)
ORDER BY n LIMIT -4 OFFSET -3 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

如果不使用 `WITH TIES`，结果将是 `1, 1, 2, 2`。使用 `WITH TIES` 时，会额外包含三个值为 `1` 的行，因为它们与第一条选中的行并列。

此修饰符可与 [`ORDER BY ... WITH FILL`](/zh/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier) 修饰符结合使用。

<div id="considerations">
  ## 注意事项
</div>

**非确定性结果：**如果没有 [`ORDER BY`](../../../sql-reference/statements/select/order-by.md) 子句，返回的行可能是任意的，并且不同次查询执行的结果可能会有所不同。

**服务端限制：**返回的行数还可能受到 [limit](../../../operations/settings/settings.md#limit) 设置的影响。

<div id="see-also">
  ## 另请参阅
</div>

* [LIMIT BY](/zh/sql-reference/statements/select/limit-by) — 限制每组值中的行数，适合获取每个类别中的前 N 个结果。