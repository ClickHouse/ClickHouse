---
slug: /sql-reference/aggregate-functions/reference/simplelinearregression
sidebar_position: 183
title: "simpleLinearRegression"
description: "Performs simple (unidimensional) linear regression."
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

## Combinators

The following combinators can be applied to the `simpleLinearRegression` function:

### simpleLinearRegressionIf
Performs simple linear regression only for rows that match the given condition.

### simpleLinearRegressionArray
Performs simple linear regression between elements in two arrays.

### simpleLinearRegressionMap
Performs simple linear regression for each key in the map separately.

### simpleLinearRegressionSimpleState
Returns the regression coefficients with SimpleAggregateFunction type.

### simpleLinearRegressionState
Returns the intermediate state of regression calculation.

### simpleLinearRegressionMerge
Combines intermediate regression states to get the final coefficients.

### simpleLinearRegressionMergeState
Combines intermediate regression states but returns an intermediate state.

### simpleLinearRegressionForEach
Performs simple linear regression for corresponding elements in multiple arrays.

### simpleLinearRegressionDistinct
Performs simple linear regression using distinct value pairs only.

### simpleLinearRegressionOrDefault
Returns (0,0) if there are not enough rows to calculate regression.

### simpleLinearRegressionOrNull
Returns NULL if there are not enough rows to calculate regression.
