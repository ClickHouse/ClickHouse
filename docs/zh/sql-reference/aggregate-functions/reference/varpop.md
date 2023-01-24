---
toc_priority: 32
---

# varPop(x) {#varpopx}

计算 `Σ((x - x̅)^2) / n`，这里 `n` 是样本大小， `x̅` 是 `x` 的平均值。

换句话说，计算一组数据的离差。 返回 `Float64`。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `varPopStable` 函数。 它的工作速度较慢，但提供较低的计算错误。
