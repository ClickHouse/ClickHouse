---
toc_priority: 36
---

# covarPop {#covarpop}

**语法**
``` sql
covarPop(x, y)
```

计算 `Σ((x - x̅)(y - y̅)) / n` 的值。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `covarPopStable` 函数。 它的工作速度较慢，但提供了较低的计算错误。
