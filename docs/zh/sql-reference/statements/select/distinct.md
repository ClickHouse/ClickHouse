---
toc_title: DISTINCT
---

# DISTINCT子句 {#select-distinct}

如果 `SELECT DISTINCT` 被声明，则查询结果中只保留唯一行。 因此，在结果中所有完全匹配的行集合中，只有一行被保留。

## 空处理 {#null-processing}

`DISTINCT` 适用于 [NULL](../../../sql-reference/syntax.md#null-literal) 就好像 `NULL` 是一个特定的值，并且 `NULL==NULL`. 换句话说，在 `DISTINCT` 结果，不同的组合 `NULL` 仅发生一次。 它不同于 `NULL` 在大多数其他情况中的处理方式。

## 替代办法 {#alternatives}

通过应用可以获得相同的结果 [GROUP BY](../../../sql-reference/statements/select/group-by.md) 在同一组值指定为 `SELECT` 子句，并且不使用任何聚合函数。 但与 `GROUP BY` 有几个不同的地方:

-   `DISTINCT` 可以与 `GROUP BY` 一起使用.
-   当 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 被省略并且 [LIMIT](../../../sql-reference/statements/select/limit.md) 被定义时，在读取所需数量的不同行后立即停止运行。
-   数据块在处理时输出，而无需等待整个查询完成运行。

## 限制 {#limitations}

`DISTINCT` 不支持当 `SELECT` 包含有数组的列。

## 例子 {#examples}

ClickHouse支持使用 `DISTINCT` 和 `ORDER BY` 在一个查询中的不同的列。 `DISTINCT` 子句在 `ORDER BY` 子句前被执行。

示例表:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

当执行 `SELECT DISTINCT a FROM t1 ORDER BY b ASC` 来查询数据，我们得到以下结果:

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

行 `2, 4` 排序前被切割。

在编程查询时考虑这种实现特性。
