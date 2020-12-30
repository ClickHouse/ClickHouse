---
toc_priority: 36
toc_title: 聚合函数
---

# 聚合函数引用 {#aggregate-functions-reference}

## count {#agg_function-count}

计数行数或非空值。

ClickHouse支持以下语法 `count`:
- `count(expr)` 或 `COUNT(DISTINCT expr)`.
- `count()` 或 `COUNT(*)`. 该 `count()` 语法是ClickHouse特定的。

**参数**

该功能可以采取:

-   零参数。
-   一 [表达式](../syntax.md#syntax-expressions).

**返回值**

-   如果没有参数调用函数，它会计算行数。
-   如果 [表达式](../syntax.md#syntax-expressions) 被传递，则该函数计数此表达式返回的次数非null。 如果表达式返回 [可为空](../../sql-reference/data-types/nullable.md)-键入值，然后结果 `count` 保持不 `Nullable`. 如果返回表达式，则该函数返回0 `NULL` 对于所有的行。

在这两种情况下，返回值的类型为 [UInt64](../../sql-reference/data-types/int-uint.md).

**详细信息**

ClickHouse支持 `COUNT(DISTINCT ...)` 语法 这种结构的行为取决于 [count_distinct_implementation](../../operations/settings/settings.md#settings-count_distinct_implementation) 设置。 它定义了其中的 [uniq\*](#agg_function-uniq) 函数用于执行操作。 默认值为 [uniqExact](#agg_function-uniqexact) 功能。

该 `SELECT count() FROM table` 查询未被优化，因为表中的条目数没有单独存储。 它从表中选择一个小列并计算其中的值数。

**例**

示例1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

示例2:

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

这个例子表明 `count(DISTINCT num)` 由执行 `uniqExact` 根据功能 `count_distinct_implementation` 设定值。

## any(x) {#agg_function-any}

选择第一个遇到的值。
查询可以以任何顺序执行，甚至每次都以不同的顺序执行，因此此函数的结果是不确定的。
要获得确定的结果，您可以使用 ‘min’ 或 ‘max’ 功能，而不是 ‘any’.

在某些情况下，可以依靠执行的顺序。 这适用于SELECT来自使用ORDER BY的子查询的情况。

当一个 `SELECT` 查询具有 `GROUP BY` 子句或至少一个聚合函数，ClickHouse（相对于MySQL）要求在所有表达式 `SELECT`, `HAVING`，和 `ORDER BY` 子句可以从键或聚合函数计算。 换句话说，从表中选择的每个列必须在键或聚合函数内使用。 要获得像MySQL这样的行为，您可以将其他列放在 `any` 聚合函数。

## anyHeavy(x) {#anyheavyx}

使用选择一个频繁出现的值 [重打者](http://www.cs.umd.edu/~samir/498/karp.pdf) 算法。 如果某个值在查询的每个执行线程中出现的情况超过一半，则返回此值。 通常情况下，结果是不确定的。

``` sql
anyHeavy(column)
```

**参数**

-   `column` – The column name.

**示例**

就拿 [时间](../../getting-started/example-datasets/ontime.md) 数据集，并选择在任何频繁出现的值 `AirlineID` 列。

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

选择遇到的最后一个值。
其结果是一样不确定的 `any` 功能。

## groupBitAnd {#groupbitand}

按位应用 `AND` 对于一系列的数字。

``` sql
groupBitAnd(expr)
```

**参数**

`expr` – An expression that results in `UInt*` 类型。

**返回值**

的价值 `UInt*` 类型。

**示例**

测试数据:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

查询:

``` sql
SELECT groupBitAnd(num) FROM t
```

哪里 `num` 是包含测试数据的列。

结果:

``` text
binary     decimal
00000100 = 4
```

## groupBitOr {#groupbitor}

按位应用 `OR` 对于一系列的数字。

``` sql
groupBitOr(expr)
```

**参数**

`expr` – An expression that results in `UInt*` 类型。

**返回值**

的价值 `UInt*` 类型。

**示例**

测试数据:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

查询:

``` sql
SELECT groupBitOr(num) FROM t
```

哪里 `num` 是包含测试数据的列。

结果:

``` text
binary     decimal
01111101 = 125
```

## groupBitXor {#groupbitxor}

按位应用 `XOR` 对于一系列的数字。

``` sql
groupBitXor(expr)
```

**参数**

`expr` – An expression that results in `UInt*` 类型。

**返回值**

的价值 `UInt*` 类型。

**示例**

测试数据:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

查询:

``` sql
SELECT groupBitXor(num) FROM t
```

哪里 `num` 是包含测试数据的列。

结果:

``` text
binary     decimal
01101000 = 104
```

## groupBitmap {#groupbitmap}

从无符号整数列的位图或聚合计算，返回UInt64类型的基数，如果添加后缀状态，则返回 [位图对象](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**参数**

`expr` – An expression that results in `UInt*` 类型。

**返回值**

的价值 `UInt64` 类型。

**示例**

测试数据:

``` text
UserID
1
1
2
3
```

查询:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

结果:

``` text
num
3
```

## min(x) {#agg_function-min}

计算最小值。

## max(x) {#agg_function-max}

计算最大值。

## argMin(arg,val) {#agg-function-argmin}

计算 ‘arg’ 最小值的值 ‘val’ 价值。 如果有几个不同的值 ‘arg’ 对于最小值 ‘val’，遇到的第一个值是输出。

**示例:**

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

## argMax(arg,val) {#agg-function-argmax}

计算 ‘arg’ 最大值 ‘val’ 价值。 如果有几个不同的值 ‘arg’ 对于最大值 ‘val’，遇到的第一个值是输出。

## sum(x) {#agg_function-sum}

计算总和。
只适用于数字。

## sumWithOverflow(x) {#sumwithoverflowx}

使用与输入参数相同的数据类型计算数字的总和。 如果总和超过此数据类型的最大值，则函数返回错误。

只适用于数字。

## sumMap(key,value),sumMap(Tuple(key,value)) {#agg_functions-summap}

总计 ‘value’ 数组根据在指定的键 ‘key’ 阵列。
传递键和值数组的元组与传递两个键和值数组是同义的。
元素的数量 ‘key’ 和 ‘value’ 总计的每一行必须相同。
返回两个数组的一个二元组： key是排好序的，value是对应key的求和。

示例:

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

计算的序列[偏度](https://en.wikipedia.org/wiki/Skewness)。

``` sql
skewPop(expr)
```

**参数**

`expr` — [表达式](../syntax.md#syntax-expressions) 返回一个数字。

**返回值**

给定序列的偏度。类型 — [Float64](../../sql-reference/data-types/float.md)

**示例**

``` sql
SELECT skewPop(value) FROM series_with_value_column
```

## skewSamp {#skewsamp}

计算 [样品偏度](https://en.wikipedia.org/wiki/Skewness) 的序列。

它表示随机变量的偏度的无偏估计，如果传递的值形成其样本。

``` sql
skewSamp(expr)
```

**参数**

`expr` — [表达式](../syntax.md#syntax-expressions) 返回一个数字。

**返回值**

给定序列的偏度。 类型 — [Float64](../../sql-reference/data-types/float.md). 如果 `n <= 1` (`n` 是样本的大小），则该函数返回 `nan`.

**示例**

``` sql
SELECT skewSamp(value) FROM series_with_value_column
```

## kurtPop {#kurtpop}

计算 [峰度](https://en.wikipedia.org/wiki/Kurtosis) 的序列。

``` sql
kurtPop(expr)
```

**参数**

`expr` — [表达式](../syntax.md#syntax-expressions) 返回一个数字。

**返回值**

给定序列的峰度。 类型 — [Float64](../../sql-reference/data-types/float.md)

**示例**

``` sql
SELECT kurtPop(value) FROM series_with_value_column
```

## kurtSamp {#kurtsamp}

计算 [峰度样本](https://en.wikipedia.org/wiki/Kurtosis) 的序列。

它表示随机变量峰度的无偏估计，如果传递的值形成其样本。

``` sql
kurtSamp(expr)
```

**参数**

`expr` — [表达式](../syntax.md#syntax-expressions) 返回一个数字。

**返回值**

给定序列的峰度。类型 — [Float64](../../sql-reference/data-types/float.md). 如果 `n <= 1` (`n` 是样本的大小），则该函数返回 `nan`.

**示例**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column
```

## avg(x) {#agg_function-avg}

计算平均值。
只适用于数字。
结果总是Float64。

## avgWeighted {#avgweighted}

计算 [加权算术平均值](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean).

**语法**

``` sql
avgWeighted(x, weight)
```

**参数**

-   `x` — 值。 [整数](../data-types/int-uint.md) 或 [浮点](../data-types/float.md).
-   `weight` — 值的加权。 [整数](../data-types/int-uint.md) 或 [浮点](../data-types/float.md).

`x` 和 `weight` 的类型一定是一样的

**返回值**

-   加权平均值。
-   `NaN`. 如果所有的权重都等于0。

类型: [Float64](../data-types/float.md).

**示例**

查询:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

结果:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

## uniq {#agg_function-uniq}

计算参数的不同值的近似数量。

``` sql
uniq(x[, ...])
```

**参数**

该函数采用可变数量的参数。 参数可以是 `Tuple`, `Array`, `Date`, `DateTime`, `String`，或数字类型。

**返回值**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-键入号码。

**实现细节**

功能:

-   计算聚合中所有参数的哈希值，然后在计算中使用它。

-   使用自适应采样算法。 对于计算状态，该函数使用最多65536个元素哈希值的样本。

        这个算法是非常精确的，并且对于CPU来说非常高效。如果查询包含一些这样的函数，那和其他聚合函数相比 `uniq` 将是几乎一样快。

-   确定性地提供结果（它不依赖于查询处理顺序）。

我们建议在几乎所有情况下使用此功能。

**另请参阅**

-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined {#agg_function-uniqcombined}

计算不同参数值的近似数量。

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

该 `uniqCombined` 函数是计算不同数值数量的不错选择。

**参数**

该函数采用可变数量的参数。 参数可以是 `Tuple`, `Array`, `Date`, `DateTime`, `String`，或数字类型。

`HLL_precision` 是以2为底的单元格数的对数 [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). 可选，您可以将该函数用作 `uniqCombined(x[, ...])`. 默认值 `HLL_precision` 是17，这是有效的96KiB的空间（2^17个单元，每个6比特）。

**返回值**

-   一个[UInt64](../../sql-reference/data-types/int-uint.md)类型的数字。

**实现细节**

功能:

-   计算散列（64位散列 `String` 否则32位）对于聚合中的所有参数，然后在计算中使用它。

-   使用三种算法的组合：数组、哈希表和包含错误修正表的HyperLogLog。

        少量的不同的值，使用数组。 值再多一些，使用哈希表。对于大量的数据来说，使用HyperLogLog，HyperLogLog占用一个固定的内存空间。

-   确定性地提供结果（它不依赖于查询处理顺序）。

!!! note "注"
    因为它使用32位散列非-`String` 类型，结果将有非常高的误差基数显着大于 `UINT_MAX` （错误将在几百亿不同值之后迅速提高），因此在这种情况下，您应该使用 [uniqCombined64](#agg_function-uniqcombined64)

相比于 [uniq](#agg_function-uniq) 功能，该 `uniqCombined`:

-   消耗少几倍的内存。
-   计算精度高出几倍。
-   通常具有略低的性能。 在某些情况下, `uniqCombined` 可以表现得比 `uniq` 好，例如，使用通过网络传输大量聚合状态的分布式查询。

**另请参阅**

-   [uniq](#agg_function-uniq)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined64 {#agg_function-uniqcombined64}

和 [uniqCombined](#agg_function-uniqcombined)，但对所有数据类型使用64位哈希。

## uniqHLL12 {#agg_function-uniqhll12}

计算不同参数值的近似数量，使用 [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) 算法。

``` sql
uniqHLL12(x[, ...])
```

**参数**

该函数采用可变数量的参数。 参数可以是 `Tuple`, `Array`, `Date`, `DateTime`, `String`，或数字类型。

**返回值**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-键入号码。

**实现细节**

功能:

-   计算聚合中所有参数的哈希值，然后在计算中使用它。

-   使用HyperLogLog算法来近似不同参数值的数量。

        212 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

-   提供确定结果（它不依赖于查询处理顺序）。

我们不建议使用此功能。 在大多数情况下，使用 [uniq](#agg_function-uniq) 或 [uniqCombined](#agg_function-uniqcombined) 功能。

**另请参阅**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqExact](#agg_function-uniqexact)

## uniqExact {#agg_function-uniqexact}

计算不同参数值的准确数目。

``` sql
uniqExact(x[, ...])
```

如果你绝对需要一个确切的结果，使用 `uniqExact` 功能。 否则使用 [uniq](#agg_function-uniq) 功能。

`uniqExact` 比 `uniq` 使用更多的内存，因为状态的大小随着不同值的数量的增加而无界增长。

**参数**

该函数采用可变数量的参数。 参数可以是 `Tuple`, `Array`, `Date`, `DateTime`, `String`，或数字类型。

**另请参阅**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqHLL12](#agg_function-uniqhll12)

## groupArray(x), groupArray(max_size)(x) {#agg_function-grouparray}

创建参数值的数组。
值可以按任何（不确定）顺序添加到数组中。

第二个版本（与 `max_size` 参数）将结果数组的大小限制为 `max_size` 元素。
例如, `groupArray (1) (x)` 相当于 `[any (x)]`.

在某些情况下，您仍然可以依靠执行的顺序。 这适用于以下情况 `SELECT` 来自使用 `ORDER BY`.

## groupArrayInsertAt {#grouparrayinsertat}

在指定位置向数组中插入一个值。

**语法**

``` sql
groupArrayInsertAt(default_x, size)(x, pos);
```

如果在一个查询中将多个值插入到同一位置，则该函数的行为方式如下:

-   如果在单个线程中执行查询，则使用第一个插入的值。
-   如果在多个线程中执行查询，则结果值是未确定的插入值之一。

**参数**

-   `x` — 被插入的值。[表达式](../syntax.md#syntax-expressions) 导致的一个 [支持的数据类型](../../sql-reference/data-types/index.md).
-   `pos` — `x` 将被插入的位置。 数组中的索引编号从零开始。 [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).
-   `default_x`— 如果代入值为空，则使用默认值。可选参数。[表达式](../syntax.md#syntax-expressions) 为 `x` 数据类型的数据。 如果 `default_x` 未定义，则 [默认值](../../sql-reference/statements/create.md#create-default-values) 被使用。
-   `size`— 结果数组的长度。可选参数。如果使用该参数，`default_x` 必须指定。 [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).

**返回值**

-   具有插入值的数组。

类型: [阵列](../../sql-reference/data-types/array.md#data-type-array).

**示例**

查询:

``` sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

结果:

``` text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐
│ ['0','','1','','2','','3','','4']                         │
└───────────────────────────────────────────────────────────┘
```

查询:

``` sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

结果:

``` text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

查询:

``` sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

结果:

``` text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

在一个位置多线程插入数据。

查询:

``` sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

作为这个查询的结果，你会得到随机整数 `[0,9]` 范围。 例如:

``` text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
```

## groupArrayMovingSum {#agg_function-grouparraymovingsum}

计算输入值的移动和。

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

该函数可以将窗口大小作为参数。 如果未指定，则该函数的窗口大小等于列中的行数。

**参数**

-   `numbers_for_summing` — [表达式](../syntax.md#syntax-expressions) 为数值数据类型值。
-   `window_size` — 窗口大小。

**返回值**

-   与输入数据大小和类型相同的数组。

**示例**

样品表:

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

查询:

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

计算输入值的移动平均值。

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

该函数可以将窗口大小作为参数。 如果未指定，则该函数的窗口大小等于列中的行数。

**参数**

-   `numbers_for_summing` — [表达式](../syntax.md#syntax-expressions) 生成数值数据类型值。
-   `window_size` — 窗口大小。

**返回值**

-   与输入数据大小和类型相同的数组。

该函数使用 [四舍五入到零](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). 它截断无意义的小数位来保证结果的数据类型。

**示例**

样品表 `b`:

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

查询:

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

## groupUniqArray(x), groupUniqArray(max_size)(x) {#groupuniqarrayx-groupuniqarraymax-sizex}

从不同的参数值创建一个数组。 内存消耗是一样的 `uniqExact` 功能。

第二个版本（`max_size` 参数）将结果数组的大小限制为 `max_size` 元素。
例如, `groupUniqArray(1)(x)` 相当于 `[any(x)]`.

## quantile {#quantile}

计算数字序列的近似[分位数](https://en.wikipedia.org/wiki/Quantile)。

此功能适用 [水塘抽样(](https://en.wikipedia.org/wiki/Reservoir_sampling)，使用储存器最大到8192和随机数发生器进行采样。 结果是非确定性的。 要获得精确的分位数，请使用 [quantileExact](#quantileexact) 功能。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantile(level)(expr)
```

别名: `median`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。

**返回值**

-   指定层次的近似分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

输入表:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

查询:

``` sql
SELECT quantile(val) FROM t
```

结果:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileDeterministic {#quantiledeterministic}

计算数字序列的近似[分位数](https://en.wikipedia.org/wiki/Quantile)。

此功能适用 [水塘抽样(](https://en.wikipedia.org/wiki/Reservoir_sampling)，使用储存器最大到8192和随机数发生器进行采样。 结果是非确定性的。 要获得精确的分位数，请使用 [quantileExact](#quantileexact) 功能。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileDeterministic(level)(expr, determinator)
```

别名: `medianDeterministic`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。
-   `determinator` — 一个数字，其hash被用来代替在水塘抽样中随机生成的数字，这样可以保证取样的确定性。你可以使用用户ID或者事件ID等任何正数，但是如果相同的 `determinator` 出现多次，那结果很可能不正确。

**返回值**

-   指定层次的近似分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

输入表:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

查询:

``` sql
SELECT quantileDeterministic(val, 1) FROM t
```

结果:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileExact {#quantileexact}

准确计算数字序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

为了准确计算，所有输入的数据被合并为一个数组，并且部分的排序。因此该函数需要 `O(n)` 的内存，n为输入数据的个数。但是对于少量数据来说，该函数还是非常有效的。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileExact(level)(expr)
```

别名: `medianExact`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

结果:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileExactWeighted {#quantileexactweighted}

考虑到每个元素的权重，然后准确计算数值序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

为了准确计算，所有输入的数据被合并为一个数组，并且部分的排序。每个输入值需要根据 `weight` 计算求和。该算法使用哈希表。正因为如此，在数据重复较多的时候使用的内存是少于[quantileExact](#quantileexact)的。 您可以使用此函数代替 `quantileExact` 并指定重量1。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileExactWeighted(level)(expr, weight)
```

别名: `medianExactWeighted`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。
-   `weight` — 权重序列。 权重是一个数据出现的数值。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

输入表:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

查询:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

结果:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileTiming {#quantiletiming}

使用确定的精度计算数字数据序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

结果是确定性的（它不依赖于查询处理顺序）。 该函数针对描述加载网页时间或后端响应时间等分布的序列进行了优化。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileTiming(level)(expr)
```

别名: `medianTiming`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).

-   `expr` — [表达式](../syntax.md#syntax-expressions)，返回 [浮动\*](../../sql-reference/data-types/float.md)类型数据。

        - 如果输入负值，那结果是不可预期的。
        - 如果输入值大于30000（页面加载时间大于30s），那我们假设为30000。

**精度**

计算是准确的，如果:

-   值的总数不超过5670。
-   总数值超过5670，但页面加载时间小于1024ms。

否则，计算结果将四舍五入到16毫秒的最接近倍数。

!!! note "注"
    对于计算页面加载时间分位数，此函数比 [分位数](#quantile)更有效和准确。

**返回值**

-   指定层次的分位数。

类型: `Float32`.

!!! note "注"
    如果没有值传递给函数（当使用 `quantileTimingIf`), [NaN](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 被返回。 这样做的目的是将这些案例与导致零的案例区分开来。 看 [ORDER BY clause](../statements/select/order-by.md#select-order-by) 对于 `NaN` 值排序注意事项。

**示例**

输入表:

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

查询:

``` sql
SELECT quantileTiming(response_time) FROM t
```

结果:

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileTimingWeighted {#quantiletimingweighted}

根据每个序列成员的权重，使用确定的精度计算数字序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

结果是确定性的（它不依赖于查询处理顺序）。 该函数针对描述加载网页时间或后端响应时间等分布的序列进行了优化。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

别名: `medianTimingWeighted`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).

-   `expr` — [表达式](../syntax.md#syntax-expressions)，返回 [浮动\*](../../sql-reference/data-types/float.md)类型数据。

        - 如果输入负值，那结果是不可预期的。
        - 如果输入值大于30000（页面加载时间大于30s），那我们假设为30000。

-   `weight` — 权重序列。 权重是一个数据出现的数值。

**精度**

计算是准确的，如果:

-   值的总数不超过5670。
-   总数值超过5670，但页面加载时间小于1024ms。

否则，计算结果将四舍五入到16毫秒的最接近倍数。

!!! note "注"
    对于计算页面加载时间分位数，此函数比 [分位数](#quantile)更高效和准确。

**返回值**

-   指定层次的分位数。

类型: `Float32`.

!!! note "注"
    如果没有值传递给函数（当使用 `quantileTimingIf`), [NaN](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 被返回。 这样做的目的是将这些案例与导致零的案例区分开来。看 [ORDER BY clause](../statements/select/order-by.md#select-order-by) 对于 `NaN` 值排序注意事项。

**示例**

输入表:

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

查询:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

结果:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileTDigest {#quantiletdigest}

使用[t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) 算法计算近似[分位数](https://en.wikipedia.org/wiki/Quantile)。

最大误差为1%。 内存消耗 `log(n)`，这里 `n` 是值的个数。 结果取决于运行查询的顺序，并且是不确定的。

该功能的性能低于性能 [分位数](#quantile) 或 [时间分位](#quantiletiming). 在状态大小与精度的比率方面，这个函数比 `quantile`更优秀。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileTDigest(level)(expr)
```

别名: `medianTDigest`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。

**回值**

-   指定层次的分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

结果:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileTDigestWeighted {#quantiletdigestweighted}

使用[t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) 算法计算近似[分位数](https://en.wikipedia.org/wiki/Quantile)。 该函数考虑了每个序列成员的权重。最大误差为1%。 内存消耗 `log(n)`，这里 `n` 是值的个数。

该功能的性能低于性能 [分位数](#quantile) 或 [时间分位](#quantiletiming). 在状态大小与精度的比率方面，这个函数比 `quantile`更优秀。

结果取决于运行查询的顺序，并且是不确定的。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能

**语法**

``` sql
quantileTDigest(level)(expr)
```

别名: `medianTDigest`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。
-   `weight` — 权重序列。 权重是一个数据出现的数值。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

结果:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## median {#median}

`median*` 函数是 `quantile*` 函数的别名。 它们计算数字数据样本的中位数。

函数:

-   `median` — [quantile](#quantile)别名。
-   `medianDeterministic` — [quantileDeterministic](#quantiledeterministic)别名。
-   `medianExact` — [quantileExact](#quantileexact)别名。
-   `medianExactWeighted` — [quantileExactWeighted](#quantileexactweighted)别名。
-   `medianTiming` — [quantileTiming](#quantiletiming)别名。
-   `medianTimingWeighted` — [quantileTimingWeighted](#quantiletimingweighted)别名。
-   `medianTDigest` — [quantileTDigest](#quantiletdigest)别名。
-   `medianTDigestWeighted` — [quantileTDigestWeighted](#quantiletdigestweighted)别名。

**示例**

输入表:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

查询:

``` sql
SELECT medianDeterministic(val, 1) FROM t
```

结果:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```

## quantiles(level1, level2, …)(x) {#quantiles}

所有分位数函数也有相应的函数: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`。这些函数一次计算所列层次的所有分位数，并返回结果值的数组。

## varSamp(x) {#varsampx}

计算 `Σ((x - x̅)^2) / (n - 1)`，这里 `n` 是样本大小， `x̅`是`x`的平均值。

它表示随机变量的方差的无偏估计，如果传递的值形成其样本。

返回 `Float64`. 当 `n <= 1`，返回 `+∞`.

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `varSampStable` 功能。 它的工作速度较慢，但提供较低的计算错误。

## varPop(x) {#varpopx}

计算 `Σ((x - x̅)^2) / n`，这里 `n` 是样本大小， `x̅`是`x`的平均值。

换句话说，计算一组数据的离差。 返回 `Float64`。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `varPopStable` 功能。 它的工作速度较慢，但提供较低的计算错误。

## stddevSamp(x) {#stddevsampx}

结果等于平方根 `varSamp(x)`。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `stddevSampStable` 功能。 它的工作速度较慢，但提供较低的计算错误。

## stddevPop(x) {#stddevpopx}

结果等于平方根 `varPop(x)`。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `stddevPopStable` 功能。 它的工作速度较慢，但提供较低的计算错误。

## topK(N)(x) {#topknx}

返回指定列中近似最常见值的数组。 生成的数组按值的近似频率降序排序（而不是值本身）。

实现了[过滤节省空间](http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf)算法， 使用基于reduce-and-combine的算法，借鉴[并行节省空间](https://arxiv.org/pdf/1401.0702.pdf).

``` sql
topK(N)(column)
```

此函数不提供保证的结果。 在某些情况下，可能会发生错误，并且可能会返回不是最高频的值。

我们建议使用 `N < 10` 值，`N` 值越大，性能越低。最大值 `N = 65536`。

**参数**

-   ‘N’ 是要返回的元素数。

如果省略该参数，则使用默认值10。

**参数**

-   ' x ' – 计算的频率值。

**示例**

就拿 [OnTime](../../getting-started/example-datasets/ontime.md) 数据集来说，选择`AirlineID` 列中出现最频繁的三个。

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

类似于 `topK` 但需要一个整数类型的附加参数 - `weight`. 每个输入都被记入 `weight` 次频率计算。

**语法**

``` sql
topKWeighted(N)(x, weight)
```

**参数**

-   `N` — 返回值个数。

**参数**

-   `x` – 输入值。
-   `weight` — 权重。 [UInt8](../../sql-reference/data-types/int-uint.md)类型。

**返回值**

返回具有最大近似权重总和的值数组。

**示例**

查询:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

结果:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```

## covarSamp(x,y) {#covarsampx-y}

计算 `Σ((x - x̅)(y - y̅)) / (n - 1)`。

返回Float64。 当 `n <= 1`, returns +∞。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `covarSampStable` 功能。 它的工作速度较慢，但提供较低的计算错误。

## covarPop(x,y) {#covarpopx-y}

计算 `Σ((x - x̅)(y - y̅)) / n`。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `covarPopStable` 功能。 它的工作速度较慢，但提供了较低的计算错误。

## corr(x,y) {#corrx-y}

计算Pearson相关系数: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`。

!!! note "注"
    该函数使用数值不稳定的算法。 如果你需要 [数值稳定性](https://en.wikipedia.org/wiki/Numerical_stability) 在计算中，使用 `corrStable` 功能。 它的工作速度较慢，但提供较低的计算错误。

## categoricalInformationValue {#categoricalinformationvalue}

对于每个类别计算 `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` 。

``` sql
categoricalInformationValue(category1, category2, ..., tag)
```

结果指示离散（分类）要素如何使用 `[category1, category2, ...]` 有助于使用学习模型预测`tag`的值。

## simpleLinearRegression {#simplelinearregression}

执行简单（一维）线性回归。

``` sql
simpleLinearRegression(x, y)
```

参数:

-   `x` — x轴。
-   `y` — y轴。

返回值:

符合`y = a*x + b`的常量 `(a, b)` 。

**例**

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

该函数实现随机线性回归。 它支持自定义参数的学习率、L2正则化系数、微批，并且具有少量更新权重的方法（[Adam](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) （默认）， [simple SGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent)， [Momentum](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum)， [Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)）。

### 参数 {#agg_functions-stochasticlinearregression-parameters}

有4个可自定义的参数。 它们按顺序传递给函数，但是没有必要传递所有四个默认值将被使用，但是好的模型需要一些参数调整。

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` 当执行梯度下降步骤时，步长上的系数。 过大的学习率可能会导致模型的权重无限大。 默认值为 `0.00001`.
2.  `l2 regularization coefficient` 这可能有助于防止过度拟合。 默认值为 `0.1`.
3.  `mini-batch size` 设置元素的数量，这些元素将被计算和求和以执行梯度下降的一个步骤。 纯随机下降使用一个元素，但是具有小批量（约10个元素）使梯度步骤更稳定。 默认值为 `15`.
4.  `method for updating weights` 他们是: `Adam` （默认情况下), `SGD`, `Momentum`, `Nesterov`. `Momentum` 和 `Nesterov` 需要更多的计算和内存，但是它们恰好在收敛速度和随机梯度方法的稳定性方面是有用的。

### 用法 {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` 用于两个步骤：拟合模型和预测新数据。 为了拟合模型并保存其状态以供以后使用，我们使用 `-State` combinator，它基本上保存了状态（模型权重等）。
为了预测我们使用函数 [evalMLMethod](../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod)，这需要一个状态作为参数以及特征来预测。

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** 安装

可以使用这种查询。

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

在这里，我们还需要将数据插入到 `train_data` 桌子 参数的数量不是固定的，它只取决于参数的数量，传递到 `linearRegressionState`. 它们都必须是数值。
请注意，带有目标值的列（我们想要学习预测）被插入作为第一个参数。

**2.** 预测

在将状态保存到表中之后，我们可以多次使用它进行预测，甚至与其他状态合并并创建新的更好的模型。

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

查询将返回一列预测值。 请注意，第一个参数 `evalMLMethod` 是 `AggregateFunctionState` 对象，接下来是要素列。

`test_data` 是一个像表 `train_data` 但可能不包含目标值。

### 注 {#agg_functions-stochasticlinearregression-notes}

1.  要合并两个模型，用户可以创建这样的查询:
    `sql  SELECT state1 + state2 FROM your_models`
    哪里 `your_models` 表包含这两个模型。 此查询将返回new `AggregateFunctionState` 对象。

2.  如果没有，用户可以获取创建的模型的权重用于自己的目的，而不保存模型 `-State` 使用combinator。
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    这种查询将拟合模型并返回其权重-首先是权重，它对应于模型的参数，最后一个是偏差。 所以在上面的例子中，查询将返回一个具有3个值的列。

**另请参阅**

-   [stochasticLogisticRegression](#agg_functions-stochasticlogisticregression)
-   [线性回归和逻辑回归之间的区别](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

该函数实现随机逻辑回归。 它可以用于二进制分类问题，支持与stochasticLinearRegression相同的自定义参数，并以相同的方式工作。

### 参数 {#agg_functions-stochasticlogisticregression-parameters}

参数与stochasticLinearRegression中的参数完全相同:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
欲了解更多信息，请参阅 [参数](#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

**1.** 安装

<!-- -->

    参考stochasticLinearRegression相关文档

    预测标签的取值范围为[-1, 1]

**2.** 预测

<!-- -->

    使用已经保存的state我们可以预测标签为 `1` 的对象的概率。

    ``` sql
    WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) FROM test_data
    ```

    查询结果返回一个列的概率。注意 `evalMLMethod` 的第一个参数是 `AggregateFunctionState` 对象，接下来的参数是列的特性。

    我们也可以设置概率的范围， 这样需要给元素指定不同的标签。

    ``` sql
    SELECT ans < 1.1 AND ans > 0.5 FROM
    (WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) AS ans FROM test_data)
    ```

    结果是标签。

    `test_data` 是一个像 `train_data` 一样的表，但是不包含目标值。

**另请参阅**

-   [随机指标线上回归](#agg_functions-stochasticlinearregression)
-   [线性回归和逻辑回归之间的差异](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## groupBitmapAnd {#groupbitmapand}

计算位图列的AND，返回UInt64类型的基数，如果添加后缀状态，则返回 [位图对象](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapAnd(expr)
```

**参数**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` 类型。

**返回值**

的价值 `UInt64` 类型。

**示例**

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

计算位图列的OR，返回UInt64类型的基数，如果添加后缀状态，则返回 [位图对象](../../sql-reference/functions/bitmap-functions.md). 这相当于 `groupBitmapMerge`.

``` sql
groupBitmapOr(expr)
```

**参数**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` 类型。

**返回值**

的价值 `UInt64` 类型。

**示例**

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

计算位图列的XOR，返回UInt64类型的基数，如果添加后缀状态，则返回 [位图对象](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapOr(expr)
```

**参数**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` 类型。

**返回值**

的价值 `UInt64` 类型。

**示例**

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

[原始文章](https://clickhouse.tech/docs/en/query_language/agg_functions/reference/) <!--hide-->
