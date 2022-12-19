---
sidebar_position: 32
---

# varPop(x)

Calculates the amount `Σ((x - x̅)^2) / n`, where `n` is the sample size and `x̅`is the average value of `x`.

In other words, dispersion for a set of values. Returns `Float64`.

:::note    
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the `varPopStable` function. It works slower but provides a lower computational error.
:::