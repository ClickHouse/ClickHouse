---
sidebar_position: 36
---

# covarPop {#covarpop}

Syntax: `covarPop(x, y)`

Calculates the value of `Σ((x - x̅)(y - y̅)) / n`.

:::note    
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the `covarPopStable` function. It works slower but provides a lower computational error.
:::