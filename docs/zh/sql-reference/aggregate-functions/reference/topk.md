---
slug: /zh/sql-reference/aggregate-functions/reference/topk
sidebar_position: 108
---

# topK {#topk}

返回指定列中近似最常见值的数组。 生成的数组按值的近似频率降序排序（而不是值本身）。

实现了[过滤节省空间](https://doi.org/10.1016/j.ins.2010.08.024)算法， 使用基于reduce-and-combine的算法，借鉴[并行节省空间](https://doi.org/10.1016/j.ins.2015.09.003)。

**语法**

``` sql
topK(N)(x)
```
此函数不提供保证的结果。 在某些情况下，可能会发生错误，并且可能会返回不是最高频的值。

我们建议使用 `N < 10` 值，`N` 值越大，性能越低。最大值 `N = 65536`。

**参数**

-   `N` — 要返回的元素数。

如果省略该参数，则使用默认值10。

**参数**

-   `x` – (要计算频次的)值。

**示例**

就拿 [OnTime](../../../getting-started/example-datasets/ontime.md) 数据集来说，选择`AirlineID` 列中出现最频繁的三个。

``` sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```

``` text
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```
