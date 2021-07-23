---
toc_priority: 201
---

# quantiles Functions {#quantiles-functions}

## quantiles {#quantiles}

Syntax: `quantiles(level1, level2, …)(x)`

All the quantile functions also have corresponding quantiles functions: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`, `quantilesBFloat16`. These functions calculate all the quantiles of the listed levels in one pass, and return an array of the resulting values.

## quantilesExactExclusive {#quantilesexactexclusive}

Exactly computes the [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed. However, for a small number of values, the function is very effective.

This function is equivalent to [PERCENTILE.EXC](https://support.microsoft.com/en-us/office/percentile-exc-function-bbaa7204-e9e1-4010-85bf-c31dc5dce4ba) Excel function, ([type R6](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)). 

Works more efficiently with sets of levels than [quantileExactExclusive](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexactexclusive).

**Syntax**

``` sql
quantilesExactExclusive(level1, level2, ...)(expr)
```

**Arguments**

-   `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Parameters**

-   `level` — Levels of quantiles. Possible values: (0, 1) — bounds not included. [Float](../../../sql-reference/data-types/float.md).

**Returned value**

-   [Array](../../../sql-reference/data-types/array.md) of quantiles of the specified levels.

Type of array values:

-   [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantilesExactExclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
```

Result:

``` text
┌─quantilesExactExclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x)─┐
│ [249.25,499.5,749.75,899.9,949.9499999999999,989.99,998.999]        │
└─────────────────────────────────────────────────────────────────────┘
```

## quantilesExactInclusive {#quantilesexactinclusive}

Exactly computes the [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed. However, for a small number of values, the function is very effective.

This function is equivalent to [PERCENTILE.INC](https://support.microsoft.com/en-us/office/percentile-inc-function-680f9539-45eb-410b-9a5e-c1355e5fe2ed) Excel function, ([type R7](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)).

Works more efficiently with sets of levels than [quantileExactInclusive](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexactinclusive).

**Syntax**

``` sql
quantilesExactInclusive(level1, level2, ...)(expr)
```

**Arguments**

-   `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Parameters**

-   `level` — Levels of quantiles. Possible values: [0, 1] — bounds included. [Float](../../../sql-reference/data-types/float.md).

**Returned value**

-   [Array](../../../sql-reference/data-types/array.md) of quantiles of the specified levels.

Type of array values:

-   [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
```

Result:

``` text
┌─quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x)─┐
│ [249.75,499.5,749.25,899.1,949.05,989.01,998.001]                   │
└─────────────────────────────────────────────────────────────────────┘
```
