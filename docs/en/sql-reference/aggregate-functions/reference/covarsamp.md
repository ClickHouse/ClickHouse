---
sidebar_position: 37
---

# covarSamp {#covarsamp}

Calculates the value of `Σ((x - x̅)(y - y̅)) / (n - 1)`.

Returns Float64. When `n <= 1`, returns +∞.

:::note    
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the `covarSampStable` function. It works slower but provides a lower computational error.
:::