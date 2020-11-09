---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: DISTINCT
---

# DISTINCT子句 {#select-distinct}

如果 `SELECT DISTINCT` 如果指定，则查询结果中只保留唯一行。 因此，在结果中所有完全匹配的行集合中，只有一行将保留。

## 空处理 {#null-processing}

`DISTINCT` 适用于 [NULL](../../../sql-reference/syntax.md#null-literal) 就好像 `NULL` 是一个特定的值，并且 `NULL==NULL`. 换句话说，在 `DISTINCT` 结果，不同的组合 `NULL` 仅发生一次。 它不同于 `NULL` 在大多数其他上下文中进行处理。

## 替代办法 {#alternatives}

通过应用可以获得相同的结果 [GROUP BY](../../../sql-reference/statements/select/group-by.md) 在同一组值指定为 `SELECT` 子句，而不使用任何聚合函数。 但有几个区别 `GROUP BY` 方法:

-   `DISTINCT` 可以一起应用 `GROUP BY`.
-   当 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 省略和 [LIMIT](../../../sql-reference/statements/select/limit.md) 定义时，查询在读取所需数量的不同行后立即停止运行。
-   数据块在处理时输出，而无需等待整个查询完成运行。

## 限制 {#limitations}

`DISTINCT` 如果不支持 `SELECT` 具有至少一个数组列。

## 例 {#examples}

ClickHouse支持使用 `DISTINCT` 和 `ORDER BY` 一个查询中不同列的子句。 该 `DISTINCT` 子句之前执行 `ORDER BY` 条款

示例表:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

当与选择数据 `SELECT DISTINCT a FROM t1 ORDER BY b ASC` 查询，我们得到以下结果:

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

如果我们改变排序方向 `SELECT DISTINCT a FROM t1 ORDER BY b DESC`，我们得到以下结果:

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

行 `2, 4` 分拣前被切割。

在编程查询时考虑这种实现特异性。
