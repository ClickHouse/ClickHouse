---
toc_priority: 107
---

# corr {#corrx-y}

**语法**
``` sql
`corr(x, y)`
```

计算Pearson相关系数: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `corrStable` 函数。 它的工作速度较慢，但提供较低的计算错误。
