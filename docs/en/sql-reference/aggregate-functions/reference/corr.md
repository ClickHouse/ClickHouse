---
toc_priority: 107
---

# corr {#corrx-y}

Syntax: `corr(x, y)`

Calculates the Pearson correlation coefficient: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`.

!!! note "Note"
    This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the `corrStable` function. It works slower but provides a lower computational error.
