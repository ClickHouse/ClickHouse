---
toc_priority: 36
toc_title: Reference
---

# Function Reference {#function-reference}

## count {#agg_function-count}

Counts the number of rows or not-NULL values.

ClickHouse supports the following syntaxes for `count`:
- `count(expr)` or `COUNT(DISTINCT expr)`.
- `count()` or `COUNT(*)`. The `count()` syntax is ClickHouse-specific.

**Parameters**

The function can take:

-   Zero parameters.
-   One [expression](../syntax.md#syntax-expressions).

**Returned value**

-   If the function is called without parameters it counts the number of rows.
-   If the [expression](../syntax.md#syntax-expressions) is passed, then the function counts how many times this expression returned not null. If the expression returns a [Nullable](../../sql-reference/data-types/nullable.md)-type value, then the result of `count` stays not `Nullable`. The function returns 0 if the expression returned `NULL` for all the rows.

In both cases the type of the returned value is [UInt64](../../sql-reference/data-types/int-uint.md).

**Details**

ClickHouse supports the `COUNT(DISTINCT ...)` syntax. The behavior of this construction depends on the [count\_distinct\_implementation](../../operations/settings/settings.md#settings-count_distinct_implementation) setting. It defines which of the [uniq\*](#agg_function-uniq) functions is used to perform the operation. The default is the [uniqExact](#agg_function-uniqexact) function.

The `SELECT count() FROM table` query is not optimized, because the number of entries in the table is not stored separately. It chooses a small column from the table and counts the number of values in it.

**Examples**

Example 1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

Example 2:

``` sql
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation'
```

``` text
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘
```

``` sql
SELECT count(DISTINCT num) FROM t
```

``` text
┌─uniqExact(num)─┐
│              3 │
└────────────────┘
```

This example shows that `count(DISTINCT num)` is performed by the `uniqExact` function according to the `count_distinct_implementation` setting value.

## any(x) {#agg_function-any}

Selects the first encountered value.
The query can be executed in any order and even in a different order each time, so the result of this function is indeterminate.
To get a determinate result, you can use the ‘min’ or ‘max’ function instead of ‘any’.

In some cases, you can rely on the order of execution. This applies to cases when SELECT comes from a subquery that uses ORDER BY.

When a `SELECT` query has the `GROUP BY` clause or at least one aggregate function, ClickHouse (in contrast to MySQL) requires that all expressions in the `SELECT`, `HAVING`, and `ORDER BY` clauses be calculated from keys or from aggregate functions. In other words, each column selected from the table must be used either in keys or inside aggregate functions. To get behavior like in MySQL, you can put the other columns in the `any` aggregate function.

## anyHeavy(x) {#anyheavyx}

Selects a frequently occurring value using the [heavy hitters](http://www.cs.umd.edu/~samir/498/karp.pdf) algorithm. If there is a value that occurs more than in half the cases in each of the query’s execution threads, this value is returned. Normally, the result is nondeterministic.

``` sql
anyHeavy(column)
```

**Arguments**

-   `column` – The column name.

**Example**

Take the [OnTime](../../getting-started/example-datasets/ontime.md) data set and select any frequently occurring value in the `AirlineID` column.

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```

## anyLast(x) {#anylastx}

Selects the last value encountered.
The result is just as indeterminate as for the `any` function.

## groupBitAnd {#groupbitand}

Applies bitwise `AND` for series of numbers.

``` sql
groupBitAnd(expr)
```

**Parameters**

`expr` – An expression that results in `UInt*` type.

**Return value**

Value of the `UInt*` type.

**Example**

Test data:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Query:

``` sql
SELECT groupBitAnd(num) FROM t
```

Where `num` is the column with the test data.

Result:

``` text
binary     decimal
00000100 = 4
```

## groupBitOr {#groupbitor}

Applies bitwise `OR` for series of numbers.

``` sql
groupBitOr(expr)
```

**Parameters**

`expr` – An expression that results in `UInt*` type.

**Return value**

Value of the `UInt*` type.

**Example**

Test data:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Query:

``` sql
SELECT groupBitOr(num) FROM t
```

Where `num` is the column with the test data.

Result:

``` text
binary     decimal
01111101 = 125
```

## groupBitXor {#groupbitxor}

Applies bitwise `XOR` for series of numbers.

``` sql
groupBitXor(expr)
```

**Parameters**

`expr` – An expression that results in `UInt*` type.

**Return value**

Value of the `UInt*` type.

**Example**

Test data:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Query:

``` sql
SELECT groupBitXor(num) FROM t
```

Where `num` is the column with the test data.

Result:

``` text
binary     decimal
01101000 = 104
```

## groupBitmap {#groupbitmap}

Bitmap or Aggregate calculations from a unsigned integer column, return cardinality of type UInt64, if add suffix -State, then return [bitmap object](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**Parameters**

`expr` – An expression that results in `UInt*` type.

**Return value**

Value of the `UInt64` type.

**Example**

Test data:

``` text
UserID
1
1
2
3
```

Query:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

Result:

``` text
num
3
```

## min(x) {#agg_function-min}

Calculates the minimum.

## max(x) {#agg_function-max}

Calculates the maximum.

## argMin(arg, val) {#agg-function-argmin}

Calculates the ‘arg’ value for a minimal ‘val’ value. If there are several different values of ‘arg’ for minimal values of ‘val’, the first of these values encountered is output.

**Example:**

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

``` sql
SELECT argMin(user, salary) FROM salary
```

``` text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```

## argMax(arg, val) {#agg-function-argmax}

Calculates the ‘arg’ value for a maximum ‘val’ value. If there are several different values of ‘arg’ for maximum values of ‘val’, the first of these values encountered is output.

## sum(x) {#agg_function-sum}

Calculates the sum.
Only works for numbers.

## sumWithOverflow(x) {#sumwithoverflowx}

Computes the sum of the numbers, using the same data type for the result as for the input parameters. If the sum exceeds the maximum value for this data type, the function returns an error.

Only works for numbers.

## sumMap(key, value), sumMap(Tuple(key, value)) {#agg_functions-summap}

Totals the ‘value’ array according to the keys specified in the ‘key’ array.
Passing tuple of keys and values arrays is synonymical to passing two arrays of keys and values.
The number of elements in ‘key’ and ‘value’ must be the same for each row that is totaled.
Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

Example:

``` sql
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    ),
    statusMapTuple Tuple(Array(Int32), Array(Int32))
) ENGINE = Log;
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests),
    sumMap(statusMapTuple)
FROM sum_map
GROUP BY timeslot
```

``` text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┬─sumMap(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │ ([1,2,3,4,5],[10,10,20,10,10]) │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │ ([4,5,6,7,8],[10,10,20,10,10]) │
└─────────────────────┴──────────────────────────────────────────────┴────────────────────────────────┘
```

## skewPop {#skewpop}

Computes the [skewness](https://en.wikipedia.org/wiki/Skewness) of a sequence.

``` sql
skewPop(expr)
```

**Parameters**

`expr` — [Expression](../syntax.md#syntax-expressions) returning a number.

**Returned value**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**Example**

``` sql
SELECT skewPop(value) FROM series_with_value_column
```

## skewSamp {#skewsamp}

Computes the [sample skewness](https://en.wikipedia.org/wiki/Skewness) of a sequence.

It represents an unbiased estimate of the skewness of a random variable if passed values form its sample.

``` sql
skewSamp(expr)
```

**Parameters**

`expr` — [Expression](../syntax.md#syntax-expressions) returning a number.

**Returned value**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). If `n <= 1` (`n` is the size of the sample), then the function returns `nan`.

**Example**

``` sql
SELECT skewSamp(value) FROM series_with_value_column
```

## kurtPop {#kurtpop}

Computes the [kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of a sequence.

``` sql
kurtPop(expr)
```

**Parameters**

`expr` — [Expression](../syntax.md#syntax-expressions) returning a number.

**Returned value**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**Example**

``` sql
SELECT kurtPop(value) FROM series_with_value_column
```

## kurtSamp {#kurtsamp}

Computes the [sample kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of a sequence.

It represents an unbiased estimate of the kurtosis of a random variable if passed values form its sample.

``` sql
kurtSamp(expr)
```

**Parameters**

`expr` — [Expression](../syntax.md#syntax-expressions) returning a number.

**Returned value**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). If `n <= 1` (`n` is a size of the sample), then the function returns `nan`.

**Example**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column
```

## timeSeriesGroupSum(uid, timestamp, value) {#agg-function-timeseriesgroupsum}

`timeSeriesGroupSum` can aggregate different time series that sample timestamp not alignment.
It will use linear interpolation between two sample timestamp and then sum time-series together.

-   `uid` is the time series unique id, `UInt64`.
-   `timestamp` is Int64 type in order to support millisecond or microsecond.
-   `value` is the metric.

The function returns array of tuples with `(timestamp, aggregated_value)` pairs.

Before using this function make sure `timestamp` is in ascending order.

Example:

``` text
┌─uid─┬─timestamp─┬─value─┐
│ 1   │     2     │   0.2 │
│ 1   │     7     │   0.7 │
│ 1   │    12     │   1.2 │
│ 1   │    17     │   1.7 │
│ 1   │    25     │   2.5 │
│ 2   │     3     │   0.6 │
│ 2   │     8     │   1.6 │
│ 2   │    12     │   2.4 │
│ 2   │    18     │   3.6 │
│ 2   │    24     │   4.8 │
└─────┴───────────┴───────┘
```

``` sql
CREATE TABLE time_series(
    uid       UInt64,
    timestamp Int64,
    value     Float64
) ENGINE = Memory;
INSERT INTO time_series VALUES
    (1,2,0.2),(1,7,0.7),(1,12,1.2),(1,17,1.7),(1,25,2.5),
    (2,3,0.6),(2,8,1.6),(2,12,2.4),(2,18,3.6),(2,24,4.8);

SELECT timeSeriesGroupSum(uid, timestamp, value)
FROM (
    SELECT * FROM time_series order by timestamp ASC
);
```

And the result will be:

``` text
[(2,0.2),(3,0.9),(7,2.1),(8,2.4),(12,3.6),(17,5.1),(18,5.4),(24,7.2),(25,2.5)]
```

## timeSeriesGroupRateSum(uid, ts, val) {#agg-function-timeseriesgroupratesum}

Similarly timeSeriesGroupRateSum, timeSeriesGroupRateSum will Calculate the rate of time-series and then sum rates together.
Also, timestamp should be in ascend order before use this function.

Use this function, the result above case will be:

``` text
[(2,0),(3,0.1),(7,0.3),(8,0.3),(12,0.3),(17,0.3),(18,0.3),(24,0.3),(25,0.1)]
```

## avg(x) {#agg_function-avg}

Calculates the average.
Only works for numbers.
The result is always Float64.

## avgWeighted {#avgweighted}

Calculates the [weighted arithmetic mean](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean).

**Syntax**

``` sql
avgWeighted(x, weight)
```

**Parameters**

-   `x` — Values. [Integer](../data-types/int-uint.md) or [floating-point](../data-types/float.md).
-   `weight` — Weights of the values. [Integer](../data-types/int-uint.md) or [floating-point](../data-types/float.md).

Type of `x` and `weight` must be the same.

**Returned value**

-   Weighted mean.
-   `NaN`. If all the weights are equal to 0.

Type: [Float64](../data-types/float.md).

**Example**

Query:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

Result:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

## uniq {#agg_function-uniq}

Calculates the approximate number of different values of the argument.

``` sql
uniq(x[, ...])
```

**Parameters**

The function takes a variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

**Returned value**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-type number.

**Implementation details**

Function:

-   Calculates a hash for all parameters in the aggregate, then uses it in calculations.

-   Uses an adaptive sampling algorithm. For the calculation state, the function uses a sample of element hash values up to 65536.

        This algorithm is very accurate and very efficient on the CPU. When the query contains several of these functions, using `uniq` is almost as fast as using other aggregate functions.

-   Provides the result deterministically (it doesn’t depend on the query processing order).

We recommend using this function in almost all scenarios.

**See Also**

-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined {#agg_function-uniqcombined}

Calculates the approximate number of different argument values.

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

The `uniqCombined` function is a good choice for calculating the number of different values.

**Parameters**

The function takes a variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

`HLL_precision` is the base-2 logarithm of the number of cells in [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Optional, you can use the function as `uniqCombined(x[, ...])`. The default value for `HLL_precision` is 17, which is effectively 96 KiB of space (2^17 cells, 6 bits each).

**Returned value**

-   A number [UInt64](../../sql-reference/data-types/int-uint.md)-type number.

**Implementation details**

Function:

-   Calculates a hash (64-bit hash for `String` and 32-bit otherwise) for all parameters in the aggregate, then uses it in calculations.

-   Uses a combination of three algorithms: array, hash table, and HyperLogLog with an error correction table.

        For a small number of distinct elements, an array is used. When the set size is larger, a hash table is used. For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.

-   Provides the result deterministically (it doesn’t depend on the query processing order).

!!! note "Note"
    Since it uses 32-bit hash for non-`String` type, the result will have very high error for cardinalities significantly larger than `UINT_MAX` (error will raise quickly after a few tens of billions of distinct values), hence in this case you should use [uniqCombined64](#agg_function-uniqcombined64)

Compared to the [uniq](#agg_function-uniq) function, the `uniqCombined`:

-   Consumes several times less memory.
-   Calculates with several times higher accuracy.
-   Usually has slightly lower performance. In some scenarios, `uniqCombined` can perform better than `uniq`, for example, with distributed queries that transmit a large number of aggregation states over the network.

**See Also**

-   [uniq](#agg_function-uniq)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined64 {#agg_function-uniqcombined64}

Same as [uniqCombined](#agg_function-uniqcombined), but uses 64-bit hash for all data types.

## uniqHLL12 {#agg_function-uniqhll12}

Calculates the approximate number of different argument values, using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm.

``` sql
uniqHLL12(x[, ...])
```

**Parameters**

The function takes a variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

**Returned value**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-type number.

**Implementation details**

Function:

-   Calculates a hash for all parameters in the aggregate, then uses it in calculations.

-   Uses the HyperLogLog algorithm to approximate the number of different argument values.

        212 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

-   Provides the determinate result (it doesn’t depend on the query processing order).

We don’t recommend using this function. In most cases, use the [uniq](#agg_function-uniq) or [uniqCombined](#agg_function-uniqcombined) function.

**See Also**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqExact](#agg_function-uniqexact)

## uniqExact {#agg_function-uniqexact}

Calculates the exact number of different argument values.

``` sql
uniqExact(x[, ...])
```

Use the `uniqExact` function if you absolutely need an exact result. Otherwise use the [uniq](#agg_function-uniq) function.

The `uniqExact` function uses more memory than `uniq`, because the size of the state has unbounded growth as the number of different values increases.

**Parameters**

The function takes a variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

**See Also**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqHLL12](#agg_function-uniqhll12)

## groupArray(x), groupArray(max\_size)(x) {#agg_function-grouparray}

Creates an array of argument values.
Values can be added to the array in any (indeterminate) order.

The second version (with the `max_size` parameter) limits the size of the resulting array to `max_size` elements.
For example, `groupArray (1) (x)` is equivalent to `[any (x)]`.

In some cases, you can still rely on the order of execution. This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY`.

## groupArrayInsertAt {#grouparrayinsertat}

Inserts a value into the array at the specified position.

**Syntax**

```sql
groupArrayInsertAt(default_x, size)(x, pos);
```

If in one query several values are inserted into the same position, the function behaves in the following ways:

- If a query is executed in a single thread, the first one of the inserted values is used.
- If a query is executed in multiple threads, the resulting value is an undetermined one of the inserted values.

**Parameters**

- `x` — Value to be inserted. [Expression](../syntax.md#syntax-expressions) resulting in one of the [supported data types](../../sql-reference/data-types/index.md).
- `pos` — Position at which the specified element `x` is to be inserted. Index numbering in the array starts from zero. [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).
- `default_x`— Default value for substituting in empty positions. Optional parameter. [Expression](../syntax.md#syntax-expressions) resulting in the data type configured for the `x` parameter. If `default_x` is not defined, the [default values](../../sql-reference/statements/create.md#create-default-values) are used.
- `size`— Length of the resulting array. Optional parameter. When using this parameter, the default value must be specified. [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Returned value**

- Array with inserted values.

Type: [Array](../../sql-reference/data-types/array.md#data-type-array).

**Example**

Query:

```sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

Result:

```text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐
│ ['0','','1','','2','','3','','4']                         │
└───────────────────────────────────────────────────────────┘
```

Query:

```sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

Result:

```text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

Query:

```sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

Result:

```text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

Multi-threaded insertion of elements into one position.

Query:

```sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

Result:

```text
┌─groupArrayInsertAt(number, 0)─┐
│ [0]                           │
└───────────────────────────────┘
```

Query:

```sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

Result:

```text
┌─groupArrayInsertAt(number, 0)─┐
│ [2]                           │
└───────────────────────────────┘
```

Query:

```sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

Result:

```text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
```


## groupArrayMovingSum {#agg_function-grouparraymovingsum}

Calculates the moving sum of input values.

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

The function can take the window size as a parameter. If left unspecified, the function takes the window size equal to the number of rows in the column.

**Parameters**

-   `numbers_for_summing` — [Expression](../syntax.md#syntax-expressions) resulting in a numeric data type value.
-   `window_size` — Size of the calculation window.

**Returned values**

-   Array of the same size and type as the input data.

**Example**

The sample table:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

The queries:

``` sql
SELECT
    groupArrayMovingSum(int) AS I,
    groupArrayMovingSum(float) AS F,
    groupArrayMovingSum(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,7,14] │ [1.1,3.3000002,7.7000003,15.47] │ [1.10,3.30,7.70,15.47] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

``` sql
SELECT
    groupArrayMovingSum(2)(int) AS I,
    groupArrayMovingSum(2)(float) AS F,
    groupArrayMovingSum(2)(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,6,11] │ [1.1,3.3000002,6.6000004,12.17] │ [1.10,3.30,6.60,12.17] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

## groupArrayMovingAvg {#agg_function-grouparraymovingavg}

Calculates the moving average of input values.

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

The function can take the window size as a parameter. If left unspecified, the function takes the window size equal to the number of rows in the column.

**Parameters**

-   `numbers_for_summing` — [Expression](../syntax.md#syntax-expressions) resulting in a numeric data type value.
-   `window_size` — Size of the calculation window.

**Returned values**

-   Array of the same size and type as the input data.

The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). It truncates the decimal places insignificant for the resulting data type.

**Example**

The sample table `b`:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

The queries:

``` sql
SELECT
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F───────────────────────────────────┬─D─────────────────────┐
│ [0,0,1,3] │ [0.275,0.82500005,1.9250001,3.8675] │ [0.27,0.82,1.92,3.86] │
└───────────┴─────────────────────────────────────┴───────────────────────┘
```

``` sql
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F────────────────────────────────┬─D─────────────────────┐
│ [0,1,3,5] │ [0.55,1.6500001,3.3000002,6.085] │ [0.55,1.65,3.30,6.08] │
└───────────┴──────────────────────────────────┴───────────────────────┘
```

## groupUniqArray(x), groupUniqArray(max\_size)(x) {#groupuniqarrayx-groupuniqarraymax-sizex}

Creates an array from different argument values. Memory consumption is the same as for the `uniqExact` function.

The second version (with the `max_size` parameter) limits the size of the resulting array to `max_size` elements.
For example, `groupUniqArray(1)(x)` is equivalent to `[any(x)]`.

## quantile {#quantile}

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

This function applies [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) with a reservoir size up to 8192 and a random number generator for sampling. The result is non-deterministic. To get an exact quantile, use the [quantileExact](#quantileexact) function.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](#quantiles) function.

**Syntax**

``` sql
quantile(level)(expr)
```

Alias: `median`.

**Parameters**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [data types](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

-   Approximate quantile of the specified level.

Type:

-   [Float64](../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Input table:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Query:

``` sql
SELECT quantile(val) FROM t
```

Result:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**See Also**

-   [median](#median)
-   [quantiles](#quantiles)

## quantileDeterministic {#quantiledeterministic}

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

This function applies [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) with a reservoir size up to 8192 and deterministic algorithm of sampling. The result is deterministic. To get an exact quantile, use the [quantileExact](#quantileexact) function.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](#quantiles) function.

**Syntax**

``` sql
quantileDeterministic(level)(expr, determinator)
```

Alias: `medianDeterministic`.

**Parameters**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [data types](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).
-   `determinator` — Number whose hash is used instead of a random number generator in the reservoir sampling algorithm to make the result of sampling deterministic. As a determinator you can use any deterministic positive number, for example, a user id or an event id. If the same determinator value occures too often, the function works incorrectly.

**Returned value**

-   Approximate quantile of the specified level.

Type:

-   [Float64](../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Input table:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Query:

``` sql
SELECT quantileDeterministic(val, 1) FROM t
```

Result:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**See Also**

-   [median](#median)
-   [quantiles](#quantiles)

## quantileExact {#quantileexact}

Exactly computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed. However, for a small number of values, the function is very effective.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](#quantiles) function.

**Syntax**

``` sql
quantileExact(level)(expr)
```

Alias: `medianExact`.

**Parameters**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [data types](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

-   Quantile of the specified level.

Type:

-   [Float64](../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

Result:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

**See Also**

-   [median](#median)
-   [quantiles](#quantiles)

## quantileExactWeighted {#quantileexactweighted}

Exactly computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence, taking into account the weight of each element.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Each value is counted with its weight, as if it is present `weight` times. A hash table is used in the algorithm. Because of this, if the passed values ​​are frequently repeated, the function consumes less RAM than [quantileExact](#quantileexact). You can use this function instead of `quantileExact` and specify the weight 1.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](#quantiles) function.

**Syntax**

``` sql
quantileExactWeighted(level)(expr, weight)
```

Alias: `medianExactWeighted`.

**Parameters**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [data types](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence members. Weight is a number of value occurrences.

**Returned value**

-   Quantile of the specified level.

Type:

-   [Float64](../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Input table:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

Query:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

Result:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**See Also**

-   [median](#median)
-   [quantiles](#quantiles)

## quantileTiming {#quantiletiming}

With the determined precision computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

The result is deterministic (it doesn’t depend on the query processing order). The function is optimized for working with sequences which describe distributions like loading web pages times or backend response times.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](#quantiles) function.

**Syntax**

``` sql
quantileTiming(level)(expr)
```

Alias: `medianTiming`.

**Parameters**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).

-   `expr` — [Expression](../syntax.md#syntax-expressions) over a column values returning a [Float\*](../../sql-reference/data-types/float.md)-type number.

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

**Accuracy**

The calculation is accurate if:

-   Total number of values doesn’t exceed 5670.
-   Total number of values exceeds 5670, but the page loading time is less than 1024ms.

Otherwise, the result of the calculation is rounded to the nearest multiple of 16 ms.

!!! note "Note"
    For calculating page loading time quantiles, this function is more effective and accurate than [quantile](#quantile).

**Returned value**

-   Quantile of the specified level.

Type: `Float32`.

!!! note "Note"
    If no values are passed to the function (when using `quantileTimingIf`), [NaN](../../sql-reference/data-types/float.md#data_type-float-nan-inf) is returned. The purpose of this is to differentiate these cases from cases that result in zero. See [ORDER BY clause](../statements/select.md#select-order-by) for notes on sorting `NaN` values.

**Example**

Input table:

``` text
┌─response_time─┐
│            72 │
│           112 │
│           126 │
│           145 │
│           104 │
│           242 │
│           313 │
│           168 │
│           108 │
└───────────────┘
```

Query:

``` sql
SELECT quantileTiming(response_time) FROM t
```

Result:

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**See Also**

-   [median](#median)
-   [quantiles](#quantiles)

## quantileTimingWeighted {#quantiletimingweighted}

With the determined precision computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence according to the weight of each sequence member.

The result is deterministic (it doesn’t depend on the query processing order). The function is optimized for working with sequences which describe distributions like loading web pages times or backend response times.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](#quantiles) function.

**Syntax**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

Alias: `medianTimingWeighted`.

**Parameters**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).

-   `expr` — [Expression](../syntax.md#syntax-expressions) over a column values returning a [Float\*](../../sql-reference/data-types/float.md)-type number.

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**Accuracy**

The calculation is accurate if:

-   Total number of values doesn’t exceed 5670.
-   Total number of values exceeds 5670, but the page loading time is less than 1024ms.

Otherwise, the result of the calculation is rounded to the nearest multiple of 16 ms.

!!! note "Note"
    For calculating page loading time quantiles, this function is more effective and accurate than [quantile](#quantile).

**Returned value**

-   Quantile of the specified level.

Type: `Float32`.

!!! note "Note"
    If no values are passed to the function (when using `quantileTimingIf`), [NaN](../../sql-reference/data-types/float.md#data_type-float-nan-inf) is returned. The purpose of this is to differentiate these cases from cases that result in zero. See [ORDER BY clause](../statements/select.md#select-order-by) for notes on sorting `NaN` values.

**Example**

Input table:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

Query:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

Result:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

**See Also**

-   [median](#median)
-   [quantiles](#quantiles)

## quantileTDigest {#quantiletdigest}

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithm.

The maximum error is 1%. Memory consumption is `log(n)`, where `n` is a number of values. The result depends on the order of running the query, and is nondeterministic.

The performance of the function is lower than performance of [quantile](#quantile) or [quantileTiming](#quantiletiming). In terms of the ratio of State size to precision, this function is much better than `quantile`.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](#quantiles) function.

**Syntax**

``` sql
quantileTDigest(level)(expr)
```

Alias: `medianTDigest`.

**Parameters**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [data types](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

-   Approximate quantile of the specified level.

Type:

-   [Float64](../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

Result:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**See Also**

-   [median](#median)
-   [quantiles](#quantiles)

## quantileTDigestWeighted {#quantiletdigestweighted}

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithm. The function takes into account the weight of each sequence member. The maximum error is 1%. Memory consumption is `log(n)`, where `n` is a number of values.

The performance of the function is lower than performance of [quantile](#quantile) or [quantileTiming](#quantiletiming). In terms of the ratio of State size to precision, this function is much better than `quantile`.

The result depends on the order of running the query, and is nondeterministic.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](#quantiles) function.

**Syntax**

``` sql
quantileTDigest(level)(expr)
```

Alias: `medianTDigest`.

**Parameters**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [data types](../../sql-reference/data-types/index.md#data_types), [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**Returned value**

-   Approximate quantile of the specified level.

Type:

-   [Float64](../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

Result:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**See Also**

-   [median](#median)
-   [quantiles](#quantiles)

## median {#median}

The `median*` functions are the aliases for the corresponding `quantile*` functions. They calculate median of a numeric data sample.

Functions:

-   `median` — Alias for [quantile](#quantile).
-   `medianDeterministic` — Alias for [quantileDeterministic](#quantiledeterministic).
-   `medianExact` — Alias for [quantileExact](#quantileexact).
-   `medianExactWeighted` — Alias for [quantileExactWeighted](#quantileexactweighted).
-   `medianTiming` — Alias for [quantileTiming](#quantiletiming).
-   `medianTimingWeighted` — Alias for [quantileTimingWeighted](#quantiletimingweighted).
-   `medianTDigest` — Alias for [quantileTDigest](#quantiletdigest).
-   `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](#quantiletdigestweighted).

**Example**

Input table:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Query:

``` sql
SELECT medianDeterministic(val, 1) FROM t
```

Result:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```

## quantiles(level1, level2, …)(x) {#quantiles}

All the quantile functions also have corresponding quantiles functions: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`. These functions calculate all the quantiles of the listed levels in one pass, and return an array of the resulting values.

## varSamp(x) {#varsampx}

Calculates the amount `Σ((x - x̅)^2) / (n - 1)`, where `n` is the sample size and `x̅`is the average value of `x`.

It represents an unbiased estimate of the variance of a random variable if passed values form its sample.

Returns `Float64`. When `n <= 1`, returns `+∞`.

## varPop(x) {#varpopx}

Calculates the amount `Σ((x - x̅)^2) / n`, where `n` is the sample size and `x̅`is the average value of `x`.

In other words, dispersion for a set of values. Returns `Float64`.

## stddevSamp(x) {#stddevsampx}

The result is equal to the square root of `varSamp(x)`.

## stddevPop(x) {#stddevpopx}

The result is equal to the square root of `varPop(x)`.

## topK(N)(x) {#topknx}

Returns an array of the approximately most frequent values in the specified column. The resulting array is sorted in descending order of approximate frequency of values (not by the values themselves).

Implements the [Filtered Space-Saving](http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf) algorithm for analyzing TopK, based on the reduce-and-combine algorithm from [Parallel Space Saving](https://arxiv.org/pdf/1401.0702.pdf).

``` sql
topK(N)(column)
```

This function doesn’t provide a guaranteed result. In certain situations, errors might occur and it might return frequent values that aren’t the most frequent values.

We recommend using the `N < 10` value; performance is reduced with large `N` values. Maximum value of `N = 65536`.

**Parameters**

-   ‘N’ is the number of elements to return.

If the parameter is omitted, default value 10 is used.

**Arguments**

-   ’ x ’ – The value to calculate frequency.

**Example**

Take the [OnTime](../../getting-started/example-datasets/ontime.md) data set and select the three most frequently occurring values in the `AirlineID` column.

``` sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```

``` text
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```

## topKWeighted {#topkweighted}

Similar to `topK` but takes one additional argument of integer type - `weight`. Every value is accounted `weight` times for frequency calculation.

**Syntax**

``` sql
topKWeighted(N)(x, weight)
```

**Parameters**

-   `N` — The number of elements to return.

**Arguments**

-   `x` – The value.
-   `weight` — The weight. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

Returns an array of the values with maximum approximate sum of weights.

**Example**

Query:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

Result:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```

## covarSamp(x, y) {#covarsampx-y}

Calculates the value of `Σ((x - x̅)(y - y̅)) / (n - 1)`.

Returns Float64. When `n <= 1`, returns +∞.

## covarPop(x, y) {#covarpopx-y}

Calculates the value of `Σ((x - x̅)(y - y̅)) / n`.

## corr(x, y) {#corrx-y}

Calculates the Pearson correlation coefficient: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`.

## categoricalInformationValue {#categoricalinformationvalue}

Calculates the value of `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` for each category.

``` sql
categoricalInformationValue(category1, category2, ..., tag)
```

The result indicates how a discrete (categorical) feature `[category1, category2, ...]` contribute to a learning model which predicting the value of `tag`.

## simpleLinearRegression {#simplelinearregression}

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

## stochasticLinearRegression {#agg_functions-stochasticlinearregression}

This function implements stochastic linear regression. It supports custom parameters for learning rate, L2 regularization coefficient, mini-batch size and has few methods for updating weights ([Adam](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) (used by default), [simple SGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent), [Momentum](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum), [Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)).

### Parameters {#agg_functions-stochasticlinearregression-parameters}

There are 4 customizable parameters. They are passed to the function sequentially, but there is no need to pass all four - default values will be used, however good model required some parameter tuning.

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` is the coefficient on step length, when gradient descent step is performed. Too big learning rate may cause infinite weights of the model. Default is `0.00001`.
2.  `l2 regularization coefficient` which may help to prevent overfitting. Default is `0.1`.
3.  `mini-batch size` sets the number of elements, which gradients will be computed and summed to perform one step of gradient descent. Pure stochastic descent uses one element, however having small batches(about 10 elements) make gradient steps more stable. Default is `15`.
4.  `method for updating weights`, they are: `Adam` (by default), `SGD`, `Momentum`, `Nesterov`. `Momentum` and `Nesterov` require little bit more computations and memory, however they happen to be useful in terms of speed of convergance and stability of stochastic gradient methods.

### Usage {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` is used in two steps: fitting the model and predicting on new data. In order to fit the model and save its state for later usage we use `-State` combinator, which basically saves the state (model weights, etc).
To predict we use function [evalMLMethod](../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod), which takes a state as an argument as well as features to predict on.

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** Fitting

Such query may be used.

``` sql
CREATE TABLE IF NOT EXISTS train_data
(
    param1 Float64,
    param2 Float64,
    target Float64
) ENGINE = Memory;

CREATE TABLE your_model ENGINE = Memory AS SELECT
stochasticLinearRegressionState(0.1, 0.0, 5, 'SGD')(target, param1, param2)
AS state FROM train_data;
```

Here we also need to insert data into `train_data` table. The number of parameters is not fixed, it depends only on number of arguments, passed into `linearRegressionState`. They all must be numeric values.
Note that the column with target value(which we would like to learn to predict) is inserted as the first argument.

**2.** Predicting

After saving a state into the table, we may use it multiple times for prediction, or even merge with other states and create new even better models.

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

The query will return a column of predicted values. Note that first argument of `evalMLMethod` is `AggregateFunctionState` object, next are columns of features.

`test_data` is a table like `train_data` but may not contain target value.

### Notes {#agg_functions-stochasticlinearregression-notes}

1.  To merge two models user may create such query:
    `sql  SELECT state1 + state2 FROM your_models`
    where `your_models` table contains both models. This query will return new `AggregateFunctionState` object.

2.  User may fetch weights of the created model for its own purposes without saving the model if no `-State` combinator is used.
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    Such query will fit the model and return its weights - first are weights, which correspond to the parameters of the model, the last one is bias. So in the example above the query will return a column with 3 values.

**See Also**

-   [stochasticLogisticRegression](#agg_functions-stochasticlogisticregression)
-   [Difference between linear and logistic regressions](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

This function implements stochastic logistic regression. It can be used for binary classification problem, supports the same custom parameters as stochasticLinearRegression and works the same way.

### Parameters {#agg_functions-stochasticlogisticregression-parameters}

Parameters are exactly the same as in stochasticLinearRegression:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
For more information see [parameters](#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

1.  Fitting

<!-- -->

    See the `Fitting` section in the [stochasticLinearRegression](#stochasticlinearregression-usage-fitting) description.

    Predicted labels have to be in \[-1, 1\].

1.  Predicting

<!-- -->

    Using saved state we can predict probability of object having label `1`.

    ``` sql
    WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) FROM test_data
    ```

    The query will return a column of probabilities. Note that first argument of `evalMLMethod` is `AggregateFunctionState` object, next are columns of features.

    We can also set a bound of probability, which assigns elements to different labels.

    ``` sql
    SELECT ans < 1.1 AND ans > 0.5 FROM
    (WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) AS ans FROM test_data)
    ```

    Then the result will be labels.

    `test_data` is a table like `train_data` but may not contain target value.

**See Also**

-   [stochasticLinearRegression](#agg_functions-stochasticlinearregression)
-   [Difference between linear and logistic regressions.](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## groupBitmapAnd {#groupbitmapand}

Calculations the AND of a bitmap column, return cardinality of type UInt64, if add suffix -State, then return [bitmap object](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapAnd(expr)
```

**Parameters**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` type.

**Return value**

Value of the `UInt64` type.

**Example**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapAnd(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapAnd(z)─┐
│               3   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapAndState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapAndState(z)))─┐
│ [6,8,10]                                         │
└──────────────────────────────────────────────────┘
```

## groupBitmapOr {#groupbitmapor}

Calculations the OR of a bitmap column, return cardinality of type UInt64, if add suffix -State, then return [bitmap object](../../sql-reference/functions/bitmap-functions.md). This is equivalent to `groupBitmapMerge`.

``` sql
groupBitmapOr(expr)
```

**Parameters**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` type.

**Return value**

Value of the `UInt64` type.

**Example**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapOr(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapOr(z)─┐
│             15   │
└──────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapOrState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapOrState(z)))─┐
│ [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]           │
└─────────────────────────────────────────────────┘
```

## groupBitmapXor {#groupbitmapxor}

Calculations the XOR of a bitmap column, return cardinality of type UInt64, if add suffix -State, then return [bitmap object](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapOr(expr)
```

**Parameters**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` type.

**Return value**

Value of the `UInt64` type.

**Example**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapXor(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapXor(z)─┐
│              10   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapXorState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapXorState(z)))─┐
│ [1,3,5,6,8,10,11,13,14,15]                       │
└──────────────────────────────────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/query_language/agg_functions/reference/) <!--hide-->
