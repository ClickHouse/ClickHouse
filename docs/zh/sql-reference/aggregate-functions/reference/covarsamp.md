---
toc_priority: 37
---

# covarSamp {#covarsamp}

**语法**
``` sql
covarSamp(x, y)
```

计算 `Σ((x - x̅)(y - y̅)) / (n - 1)` 的值。

返回Float64。 当 `n <= 1`, 返回 +∞。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `covarSampStable` 函数。 它的工作速度较慢，但提供较低的计算错误。
