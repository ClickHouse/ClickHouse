---
sidebar_position: 220
---

# simpleLinearRegression {#simplelinearregression}

执行简单（一维）线性回归。

**语法**

``` sql
simpleLinearRegression(x, y)
```

**参数**

-   `x` — x轴。
-   `y` — y轴。

**返回值**

符合`y = a*x + b`的常量 `(a, b)` 。

**示例**

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])─┐
│ (1,0)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])─┐
│ (1,3)                                                             │
└───────────────────────────────────────────────────────────────────┘
```
