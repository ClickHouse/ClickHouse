---
toc_folder_title: "\u0410\u0433\u0440\u0435\u0433\u0430\u0442\u043D\u044B\u0435 \u0444\
  \u0443\u043D\u043A\u0446\u0438\u0438"
toc_priority: 33
toc_title: "\u0412\u0432\u0435\u0434\u0435\u043D\u0438\u0435"
---

# Агрегатные функции {#aggregate-functions}

Агрегатные функции работают в [привычном](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) для специалистов по базам данных смысле.

ClickHouse поддерживает также:

-   [Параметрические агрегатные функции](parametric-functions.md#aggregate_functions_parametric), которые помимо столбцов принимаю и другие параметры.
-   [Комбинаторы](combinators.md#aggregate_functions_combinators), которые изменяют поведение агрегатных функций.

## Обработка NULL {#obrabotka-null}

При агрегации все `NULL` пропускаются.

**Примеры**

Рассмотрим таблицу:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Выполним суммирование значений в столбце `y`:

``` sql
SELECT sum(y) FROM t_null_big
```

``` text
┌─sum(y)─┐
│      7 │
└────────┘
```

Функция `sum` работает с `NULL` как с `0`. В частности, это означает, что если на вход в функцию подать выборку, где все значения `NULL`, то результат будет `0`, а не `NULL`.

Теперь с помощью функции `groupArray` сформируем массив из столбца `y`:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` не включает `NULL` в результирующий массив.


## Перечень агрегатных функций {#aggregate-functions-list}

Стандартные агрегатные функции:

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

Агрегатные функции, специфичные для ClickHouse:

-   [anyHeavy](../../sql-reference/aggregate-functions/reference/anyheavy.md)
-   [anyLast](../../sql-reference/aggregate-functions/reference/anylast.md)
-   [argMin](../../sql-reference/aggregate-functions/reference/argmin.md)
-   [argMax](../../sql-reference/aggregate-functions/reference/argmax.md)
-   [avgWeighted](../../sql-reference/aggregate-functions/reference/avgweighted.md)
-   [topK](../../sql-reference/aggregate-functions/reference/topk.md)
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
-   [sumWithOverflow](../../sql-reference/aggregate-functions/reference/sumwithoverflow.md)
-   [sumMap](../../sql-reference/aggregate-functions/reference/summap.md)
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


[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/agg_functions/) <!--hide-->
