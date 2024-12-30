---
slug: /en/sql-reference/aggregate-functions/reference/simplelinearregression
sidebar_position: 183
---

# simpleLinearRegression

Performs simple (unidimensional) linear regression.

``` sql
simpleLinearRegression(x, y)
```

Parameters:

- `x` — Column with explanatory variable values.
- `y` — Column with dependent variable values.

Returned values:

Constants `(k, b)` of the resulting line `y = k*x + b`.

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
