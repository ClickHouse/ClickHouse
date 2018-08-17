<a name="aggregate_functions_reference"></a>

# Function reference

## count()

Counts the number of rows. Accepts zero arguments and returns UInt64.
The syntax `COUNT(DISTINCT x)` is not supported. The separate `uniq` aggregate function exists for this purpose.

A `SELECT count() FROM table` query is not optimized, because the number of entries in the table is not stored separately. It will select some small column from the table and count the number of values in it.

## any(x)

Selects the first encountered value.
The query can be executed in any order and even in a different order each time, so the result of this function is indeterminate.
To get a determinate result, you can use the 'min' or 'max' function instead of 'any'.

In some cases, you can rely on the order of execution. This applies to cases when SELECT comes from a subquery that uses ORDER BY.

When a `SELECT` query has the `GROUP BY` clause or at least one aggregate function, ClickHouse (in contrast to MySQL) requires that all expressions in the `SELECT`, `HAVING`, and `ORDER BY` clauses be calculated from keys or from aggregate functions. In other words, each column selected from the table must be used either in keys or inside aggregate functions. To get behavior like in MySQL, you can put the other columns in the `any` aggregate function.

## anyHeavy(x)

Selects a frequently occurring value using the [heavy hitters](http://www.cs.umd.edu/~samir/498/karp.pdf) algorithm. If there is a value that occurs more than in half the cases in each of the query's execution threads, this value is returned. Normally, the result is nondeterministic.

```
anyHeavy(column)
```

**Arguments**
- `column` – The column name.

**Example**

Take the [OnTime](../../getting_started/example_datasets/ontime.md#example_datasets-ontime) data set and select any frequently occurring value in the `AirlineID` column.

```sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

```
┌───res─┐
│ 19690 │
└───────┘
```

## anyLast(x)

Selects the last value encountered.
The result is just as indeterminate as for the `any` function.

## min(x)

Calculates the minimum.

## max(x)

Calculates the maximum.

## argMin(arg, val)

Calculates the 'arg' value for a minimal 'val' value. If there are several different values of 'arg' for minimal values of 'val', the first of these values encountered is output.

## argMax(arg, val)

Calculates the 'arg' value for a maximum 'val' value. If there are several different values of 'arg' for maximum values of 'val', the first of these values encountered is output.

## sum(x)

Calculates the sum.
Only works for numbers.

## sumWithOverflow(x)

Computes the sum of the numbers, using the same data type for the result as for the input parameters. If the sum exceeds the maximum value for this data type, the function returns an error.

Only works for numbers.

## sumMap(key, value)

Totals the 'value' array according to the keys specified in the 'key' array.
The number of elements in 'key' and 'value' must be the same for each row that is totaled.
Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

Example:

```sql
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    )
) ENGINE = Log;
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests)
FROM sum_map
GROUP BY timeslot
```

```text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │
└─────────────────────┴──────────────────────────────────────────────┘
```

## avg(x)

Calculates the average.
Only works for numbers.
The result is always Float64.

## uniq(x)

Calculates the approximate number of different values of the argument. Works for numbers, strings, dates, date-with-time, and for multiple arguments and tuple arguments.

Uses an adaptive sampling algorithm: for the calculation state, it uses a sample of element hash values with a size up to 65536.
This algorithm is also very accurate for data sets with low cardinality (up to 65536) and very efficient on CPU (when computing not too many of these functions, using `uniq` is almost as fast as using other aggregate functions).

The result is determinate (it doesn't depend on the order of query processing).

This function provides excellent accuracy even for data sets with extremely high cardinality (over 10 billion elements). It is recommended for default use.

## uniqCombined(x)

Calculates the approximate number of different values of the argument. Works for numbers, strings, dates, date-with-time, and for multiple arguments and tuple arguments.

A combination of three algorithms is used: array, hash table and [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) with an error correction table. The memory consumption is several times smaller than for the `uniq` function, and the accuracy is several times higher. Performance is slightly lower than for the `uniq` function, but sometimes it can be even higher than it, such as with distributed queries that transmit a large number of aggregation states over the network. The maximum state size is 96 KiB (HyperLogLog of 217 6-bit cells).

The result is determinate (it doesn't depend on the order of query processing).

The `uniqCombined` function is a good default choice for calculating the number of different values, but keep in mind that the estimation error will increase for high-cardinality data sets (200M+ elements), and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

## uniqHLL12(x)

Uses the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm to approximate the number of different values of the argument.
212 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

The result is determinate (it doesn't depend on the order of query processing).

We don't recommend using this function. In most cases, use the  `uniq` or `uniqCombined` function.

## uniqExact(x)

Calculates the number of different values of the argument, exactly.
There is no reason to fear approximations. It's better to use the `uniq` function.
Use the `uniqExact` function if you definitely need an exact result.

The `uniqExact` function uses more memory than the `uniq` function, because the size of the state has unbounded growth as the number of different values increases.

## groupArray(x), groupArray(max_size)(x)

Creates an array of argument values.
Values can be added to the array in any (indeterminate) order.

The second version (with the `max_size` parameter) limits the size of the resulting array to `max_size` elements.
For example, `groupArray (1) (x)` is equivalent to `[any (x)]`.

In some cases, you can still rely on the order of execution. This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY`.

<a name="agg_functions_groupArrayInsertAt"></a>

## groupArrayInsertAt(x)

Inserts a value into the array in the specified position.

Accepts the value and position as input. If several values ​​are inserted into the same position, any of them might end up in the resulting array (the first one will be used in the case of single-threaded execution). If no value is inserted into a position, the position is assigned the default value.

Optional parameters:

- The default value for substituting in empty positions.
- The length of the resulting array. This allows you to receive arrays of the same size for all the aggregate keys. When using this parameter, the default value must be specified.

## groupUniqArray(x)

Creates an array from different argument values. Memory consumption is the same as for the `uniqExact` function.

## quantile(level)(x)

Approximates the 'level' quantile. 'level' is a constant, a floating-point number from 0 to 1.
We recommend using a 'level' value in the range of 0.01..0.99
Don't use a 'level' value equal to 0 or 1 – use the 'min' and 'max' functions for these cases.

In this function, as well as in all functions for calculating quantiles, the 'level' parameter can be omitted. In this case, it is assumed to be equal to 0.5 (in other words, the function will calculate the median).

Works for numbers, dates, and dates with times.
Returns: for numbers – Float64; for dates – a date; for dates with times – a date with time.

Uses [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) with a reservoir size up to 8192.
If necessary, the result is output with linear approximation from the two neighboring values.
This algorithm provides very low accuracy. See also: `quantileTiming`, `quantileTDigest`, `quantileExact`.

The result depends on the order of running the query, and is nondeterministic.

When using multiple `quantile` (and similar) functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the `quantiles` (and similar) functions.

## quantileDeterministic(level)(x, determinator)

Works the same way as the `quantile` function, but the result is deterministic and does not depend on the order of query execution.

To achieve this, the function takes a second argument – the "determinator". This is a number whose hash is used instead of a random number generator in the reservoir sampling algorithm. For the function to work correctly, the same determinator value should not occur too often. For the determinator, you can use an event ID, user ID, and so on.

Don't use this function for calculating timings. There is a more suitable function for this purpose: `quantileTiming`.

## quantileTiming(level)(x)

Computes the quantile of 'level' with a fixed precision.
Works for numbers. Intended for calculating quantiles of page loading time in milliseconds.

If the value is greater than 30,000 (a page loading time of more than 30 seconds), the result is equated to 30,000.

If the total value is not more than about 5670, then the calculation is accurate.

Otherwise:

- if the time is less than 1024 ms, then the calculation is accurate.
- otherwise the calculation is rounded to a multiple of 16 ms.

When passing negative values to the function, the behavior is undefined.

The returned value has the Float32 type. If no values were passed to the function (when using `quantileTimingIf`), 'nan' is returned. The purpose of this is to differentiate these instances from zeros. See the note on sorting NaNs in "ORDER BY clause".

The result is determinate (it doesn't depend on the order of query processing).

For its purpose (calculating quantiles of page loading times), using this function is more effective and the result is more accurate than for the `quantile` function.

## quantileTimingWeighted(level)(x, weight)

Differs from the `quantileTiming`  function in that it has a second argument, "weights". Weight is a non-negative integer.
The result is calculated as if the `x`  value were passed `weight` number of times to the `quantileTiming` function.

## quantileExact(level)(x)

Computes the quantile of 'level' exactly. To do this, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes O(n) memory, where 'n' is the number of values that were passed. However, for a small number of values, the function is very effective.

## quantileExactWeighted(level)(x, weight)

Computes the quantile of 'level' exactly. In addition, each value is counted with its weight, as if it is present 'weight' times. The arguments of the function can be considered as histograms, where the value 'x' corresponds to a histogram "column" of the height 'weight', and the function itself can be considered as a summation of histograms.

A hash table is used as the algorithm. Because of this, if the passed values ​​are frequently repeated, the function consumes less RAM than `quantileExact`. You can use this function instead of `quantileExact` and specify the weight as 1.

## quantileTDigest(level)(x)

Approximates the quantile level using the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithm. The maximum error is 1%. Memory consumption by State is proportional to the logarithm of the number of passed values.

The performance of the function is lower than for ` quantile`, ` quantileTiming`. In terms of the ratio of State size to precision, this function is much better than `quantile`.

The result depends on the order of running the query, and is nondeterministic.

## median(x)

All the quantile functions have corresponding median functions: `median`, `medianDeterministic`, `medianTiming`, `medianTimingWeighted`, `medianExact`, `medianExactWeighted`, `medianTDigest`. They are synonyms and their behavior is identical.

## quantiles(level1, level2, ...)(x)

All the quantile functions also have corresponding quantiles functions: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`. These functions calculate all the quantiles of the listed levels in one pass, and return an array of the resulting values.

## varSamp(x)

Calculates the amount `Σ((x - x̅)^2) / (n - 1)`, where `n` is the sample size and `x̅`is the average value of `x`.

It represents an unbiased estimate of the variance of a random variable, if the values passed to the function are a sample of this random amount.

Returns `Float64`. When `n <= 1`, returns `+∞`.

## varPop(x)

Calculates the amount `Σ((x - x̅)^2) / (n - 1)`, where `n` is the sample size and `x̅`is the average value of `x`.

In other words, dispersion for a set of values. Returns `Float64`.

## stddevSamp(x)

The result is equal to the square root of `varSamp(x)`.

## stddevPop(x)

The result is equal to the square root of `varPop(x)`.

## topK(N)(column)

Returns an array of the most frequent values in the specified column. The resulting array is sorted in descending order of frequency of values (not by the values themselves).

Implements the [Filtered Space-Saving](http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf)  algorithm for analyzing TopK, based on the reduce-and-combine algorithm from [Parallel Space Saving](https://arxiv.org/pdf/1401.0702.pdf).

```
topK(N)(column)
```

This function doesn't provide a guaranteed result. In certain situations, errors might occur and it might return frequent values that aren't the most frequent values.

We recommend using the `N < 10 ` value; performance is reduced with large `N` values. Maximum value of ` N = 65536`.

**Arguments**
- 'N' is the number of values.
- ' x ' – The column.

**Example**

Take the [OnTime](../../getting_started/example_datasets/ontime.md#example_datasets-ontime) data set and select the three most frequently occurring values in the `AirlineID` column.

```sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```
```
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```

## covarSamp(x, y)

Calculates the value of `Σ((x - x̅)(y - y̅)) / (n - 1)`.

Returns Float64. When `n <= 1`, returns +∞.

## covarPop(x, y)

Calculates the value of `Σ((x - x̅)(y - y̅)) / n`.

## corr(x, y)

Calculates the Pearson correlation coefficient: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`.

