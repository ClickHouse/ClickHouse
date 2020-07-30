---
toc_folder_title: Aggregate Functions
toc_priority: 33
toc_title: Introduction
---

# Aggregate Functions {#aggregate-functions}

Aggregate functions work in the [normal](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) way as expected by database experts.

ClickHouse also supports:

-   [Parametric aggregate functions](../../sql-reference/aggregate-functions/parametric-functions.md#aggregate_functions_parametric), which accept other parameters in addition to columns.
-   [Combinators](../../sql-reference/aggregate-functions/combinators.md#aggregate_functions_combinators), which change the behavior of aggregate functions.

## NULL Processing {#null-processing}

During aggregation, all `NULL`s are skipped.

**Examples:**

Consider this table:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Let’s say you need to total the values in the `y` column:

``` sql
SELECT sum(y) FROM t_null_big
```

```text
┌─sum(y)─┐
│      7 │
└────────┘
```

The `sum` function interprets `NULL` as `0`. In particular, this means that if the function receives input of a selection where all the values are `NULL`, then the result will be `0`, not `NULL`.

Now you can use the `groupArray` function to create an array from the `y` column:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` does not include `NULL` in the resulting array.

## List of Aggregate Functions {#aggregate-functions-reference}

Standard aggregate functions:

-   [count](../../sql-reference/aggregate-functions/reference/count.md)
-   [min](../../sql-reference/aggregate-functions/reference/min.md)
-   [max](../../sql-reference/aggregate-functions/reference/max.md)
-   [sum](../../sql-reference/aggregate-functions/reference/sum.md)
-   [avg](../../sql-reference/aggregate-functions/reference/avg.md)
-   [any](../../sql-reference/aggregate-functions/reference/any.md)
-   [stddevPop](../../sql-reference/aggregate-functions/reference/stddevpop.md)
-   [stddevSamp](../../sql-reference/aggregate-functions/reference/stddevsamp.md)
-   [varPop](../../sql-reference/aggregate-functions/reference/varpop.md)
-   [varSamp](../../sql-reference/aggregate-functions/reference/varsamp.md)
-   [covarPop](../../sql-reference/aggregate-functions/reference/covarpop.md)
-   [covarSamp](../../sql-reference/aggregate-functions/reference/covarsamp.md)

ClickHouse-specific aggregate functions:

-   [anyHeavy](../../sql-reference/aggregate-functions/reference/anyheavy.md)
-   [anyLast](../../sql-reference/aggregate-functions/reference/anylast.md)
-   [argMin](../../sql-reference/aggregate-functions/reference/argmin.md)
-   [argMax](../../sql-reference/aggregate-functions/reference/argmax.md)
-   [avgWeighted](../../sql-reference/aggregate-functions/reference/avgweighted.md)
-   [topK](../../sql-reference/aggregate-functions/reference/topkweighted.md)
-   [topKWeighted](../../sql-reference/aggregate-functions/reference/topkweighted.md)
-   [groupArray](../../sql-reference/aggregate-functions/reference/grouparray.md)
-   [groupUniqArray](../../sql-reference/aggregate-functions/reference/groupuniqarray.md)
-   [groupArrayInsertAt](../../sql-reference/aggregate-functions/reference/grouparrayinsertat.md)
-   [groupArrayMovingAvg](../../sql-reference/aggregate-functions/reference/grouparraymovingavg.md)
-   [groupArrayMovingSum](../../sql-reference/aggregate-functions/reference/grouparraymovingsum.md)
-   [groupBitAnd](../../sql-reference/aggregate-functions/reference/groupbitand.md)
-   [groupBitOr](../../sql-reference/aggregate-functions/reference/groupbitor.md)
-   [groupBitXor](../../sql-reference/aggregate-functions/reference/groupbitxor.md)
-   [groupBitmap](../../sql-reference/aggregate-functions/reference/groupbitmap.md)
-   [groupBitmapAnd](../../sql-reference/aggregate-functions/reference/groupbitmapand.md)
-   [groupBitmapOr](../../sql-reference/aggregate-functions/reference/groupbitmapor.md)
-   [groupBitmapXor](../../sql-reference/aggregate-functions/reference/groupbitmapxor.md)
-   [sumWithOverflow](../../sql-reference/aggregate-functions/reference/sumwithoverflow.md)
-   [sumMap](../../sql-reference/aggregate-functions/reference/summap.md)
-   [minMap](../../sql-reference/aggregate-functions/reference/minmap.md)
-   [maxMap](../../sql-reference/aggregate-functions/reference/maxmap.md)
-   [skewSamp](../../sql-reference/aggregate-functions/reference/skewsamp.md)
-   [skewPop](../../sql-reference/aggregate-functions/reference/skewpop.md)
-   [kurtSamp](../../sql-reference/aggregate-functions/reference/kurtsamp.md)
-   [kurtPop](../../sql-reference/aggregate-functions/reference/kurtpop.md)
-   [timeSeriesGroupSum](../../sql-reference/aggregate-functions/reference/timeseriesgroupsum.md)
-   [timeSeriesGroupRateSum](../../sql-reference/aggregate-functions/reference/timeseriesgroupratesum.md)
-   [uniq](../../sql-reference/aggregate-functions/reference/uniq.md)
-   [uniqExact](../../sql-reference/aggregate-functions/reference/uniqexact.md)
-   [uniqCombined](../../sql-reference/aggregate-functions/reference/uniqcombined.md)
-   [uniqCombined64](../../sql-reference/aggregate-functions/reference/uniqcombined64.md)
-   [uniqHLL12](../../sql-reference/aggregate-functions/reference/uniqhll12.md)
-   [quantile](../../sql-reference/aggregate-functions/reference/quantile.md)
-   [quantiles](../../sql-reference/aggregate-functions/reference/quantiles.md)
-   [quantileExact](../../sql-reference/aggregate-functions/reference/quantileexact.md)
-   [quantileExactWeighted](../../sql-reference/aggregate-functions/reference/quantileexactweighted.md)
-   [quantileTiming](../../sql-reference/aggregate-functions/reference/quantiletiming.md)
-   [quantileTimingWeighted](../../sql-reference/aggregate-functions/reference/quantiletimingweighted.md)
-   [quantileDeterministic](../../sql-reference/aggregate-functions/reference/quantiledeterministic.md)
-   [quantileTDigest](../../sql-reference/aggregate-functions/reference/quantiletdigest.md)
-   [quantileTDigestWeighted](../../sql-reference/aggregate-functions/reference/quantiletdigestweighted.md)
-   [simpleLinearRegression](../../sql-reference/aggregate-functions/reference/simplelinearregression.md)
-   [stochasticLinearRegression](../../sql-reference/aggregate-functions/reference/stochasticlinearregression.md)
-   [stochasticLogisticRegression](../../sql-reference/aggregate-functions/reference/stochasticlogisticregression.md)
-   [categoricalInformationValue](../../sql-reference/aggregate-functions/reference/categoricalinformationvalue.md)

[Original article](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
