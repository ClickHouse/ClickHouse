---
toc_priority: 103
---

# anyHeavy {#anyheavyx}

选择一个频繁出现的值，使用[heavy hitters](http://www.cs.umd.edu/~samir/498/karp.pdf) 算法。 如果某个值在查询的每个执行线程中出现的情况超过一半，则返回此值。 通常情况下，结果是不确定的。

``` sql
anyHeavy(column)
```

**参数**

-   `column` – The column name。

**示例**

使用 [OnTime](../../../getting-started/example-datasets/ontime.md) 数据集，并选择在 `AirlineID` 列任何频繁出现的值。

查询:

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime;
```

结果:

``` text
┌───res─┐
│ 19690 │
└───────┘
```
