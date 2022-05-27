---
sidebar_position: 33
---

# varSamp {#varsamp}

Calculates the amount `Σ((x - x̅)^2) / (n - 1)`, where `n` is the sample size and `x̅`is the average value of `x`.

It represents an unbiased estimate of the variance of a random variable if passed values form its sample.

Returns `Float64`. When `n <= 1`, returns `+∞`.

:::note    
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the `varSampStable` function. It works slower but provides a lower computational error.
:::