---
toc_priority: 31
---

# stddevSamp {#stddevsamp}

The result is equal to the square root of [varSamp](../../../sql-reference/aggregate-functions/reference/varsamp.md).

!!! note "Note"
    This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the `stddevSampStable` function. It works slower but provides a lower computational error.
