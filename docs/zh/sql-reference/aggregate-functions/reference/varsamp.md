---
toc_priority: 33
---

# varSamp {#varsamp}

计算 `Σ((x - x̅)^2) / (n - 1)`，这里 `n` 是样本大小， `x̅`是`x`的平均值。

它表示随机变量的方差的无偏估计，如果传递的值形成其样本。

返回 `Float64`。 当 `n <= 1`，返回 `+∞`。

!!! note "注"
该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `varSampStable` 函数。 它的工作速度较慢，但提供较低的计算错误。

