---
sidebar_position: 220
---

# simpleLinearRegression {#simplelinearregression}

Performs simple (unidimensional) linear regression.

``` sql
simpleLinearRegression(x, y)
```

Parameters:

-   `x` — Column with dependent variable values.
-   `y` — Column with explanatory variable values.

Returned values:

Constants `(a, b)` of the resulting line `y = a*x + b`.

**Examples**

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])─┐
│ (1,0)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])─┐
│ (1,3)                                                             │
└───────────────────────────────────────────────────────────────────┘
```
