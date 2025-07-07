---
slug: /zh/sql-reference/aggregate-functions/reference/stddevsamp
sidebar_position: 31
---

# stddevSamp {#stddevsamp}

结果等于 [varSamp](../../../sql-reference/aggregate-functions/reference/varsamp.md) 的平方根。

:::note
该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `stddevSampStable` 函数。 它的工作速度较慢，但提供较低的计算错误。
:::
