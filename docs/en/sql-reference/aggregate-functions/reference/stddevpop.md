---
toc_priority: 30
---

# stddevPop {#stddevpop}

The result is equal to the square root of [varPop](../../../sql-reference/aggregate-functions/reference/varpop.md).

!!! note "Note"
    This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the `stddevPopStable` function. It works slower but provides a lower computational error.
