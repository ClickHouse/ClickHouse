---
slug: /en/sql-reference/aggregate-functions/reference/quantileddsketch
sidebar_position: 171
title: quantileDD
---

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a sample with relative-error guarantees. It works by building a [DD](https://www.vldb.org/pvldb/vol12/p2195-masson.pdf).

**Syntax**

``` sql
quantileDD(relative_accuracy, [level])(expr)
```

**Arguments**

- `expr` — Column with numeric data. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md).

**Parameters**

- `relative_accuracy` — Relative accuracy of the quantile. Possible values are in the range from 0 to 1. [Float](../../../sql-reference/data-types/float.md). The size of the sketch depends on the range of the data and the relative accuracy. The larger the range and the smaller the relative accuracy, the larger the sketch. The rough memory size of the of the sketch is `log(max_value/min_value)/relative_accuracy`. The recommended value is 0.001 or higher.

- `level` — Level of quantile. Optional. Possible values are in the range from 0 to 1. Default value: 0.5. [Float](../../../sql-reference/data-types/float.md).

**Returned value**

- Approximate quantile of the specified level.

Type: [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Example**

Input table has an integer and a float columns:

``` text
┌─a─┬─────b─┐
│ 1 │ 1.001 │
│ 2 │ 1.002 │
│ 3 │ 1.003 │
│ 4 │ 1.004 │
└───┴───────┘
```

Query to calculate 0.75-quantile (third quartile):

``` sql
SELECT quantileDD(0.01, 0.75)(a), quantileDD(0.01, 0.75)(b) FROM example_table;
```

Result:

``` text
┌─quantileDD(0.01, 0.75)(a)─┬─quantileDD(0.01, 0.75)(b)─┐
│               2.974233423476717 │                            1.01 │
└─────────────────────────────────┴─────────────────────────────────┘
```

**See Also**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
