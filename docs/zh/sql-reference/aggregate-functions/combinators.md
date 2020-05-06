---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_priority: 37
toc_title: "\u805A\u5408\u51FD\u6570\u7EC4\u5408\u5668"
---

# 聚合函数组合器 {#aggregate_functions_combinators}

聚合函数的名称可以附加一个后缀。 这改变了聚合函数的工作方式。

## -如果 {#agg-functions-combinator-if}

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

例: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` 等等。

使用条件聚合函数，您可以一次计算多个条件的聚合，而无需使用子查询和 `JOIN`例如，在Yandex的。Metrica，条件聚合函数用于实现段比较功能。

## -阵列 {#agg-functions-combinator-array}

-Array后缀可以附加到任何聚合函数。 在这种情况下，聚合函数采用的参数 ‘Array(T)’ 类型（数组）而不是 ‘T’ 类型参数。 如果聚合函数接受多个参数，则它必须是长度相等的数组。 在处理数组时，聚合函数的工作方式与所有数组元素的原始聚合函数类似。

示例1: `sumArray(arr)` -总计所有的所有元素 ‘arr’ 阵列。 在这个例子中，它可以更简单地编写: `sum(arraySum(arr))`.

示例2: `uniqArray(arr)` – Counts the number of unique elements in all ‘arr’ 阵列。 这可以做一个更简单的方法: `uniq(arrayJoin(arr))`，但它并不总是可以添加 ‘arrayJoin’ 到查询。

-如果和-阵列可以组合。 然而, ‘Array’ 必须先来，然后 ‘If’. 例: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. 由于这个顺序，该 ‘cond’ 参数不会是数组。

## -州 {#agg-functions-combinator-state}

如果应用此combinator，则聚合函数不会返回结果值（例如唯一值的数量 [uniq](reference.md#agg_function-uniq) 函数），但聚合的中间状态（用于 `uniq`，这是用于计算唯一值的数量的散列表）。 这是一个 `AggregateFunction(...)` 可用于进一步处理或存储在表中以完成聚合。

要使用这些状态，请使用:

-   [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) 表引擎。
-   [最后聚会](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) 功能。
-   [跑累积](../../sql-reference/functions/other-functions.md#function-runningaccumulate) 功能。
-   [-合并](#aggregate_functions_combinators-merge) combinator
-   [-MergeState](#aggregate_functions_combinators-mergestate) combinator

## -合并 {#aggregate_functions_combinators-merge}

如果应用此组合器，则聚合函数将中间聚合状态作为参数，组合状态以完成聚合，并返回结果值。

## -MergeState {#aggregate_functions_combinators-mergestate}

以与-Merge combinator相同的方式合并中间聚合状态。 但是，它不会返回结果值，而是返回中间聚合状态，类似于-State combinator。

## -ForEach {#agg-functions-combinator-foreach}

将表的聚合函数转换为聚合相应数组项并返回结果数组的数组的聚合函数。 例如, `sumForEach` 对于数组 `[1, 2]`, `[3, 4, 5]`和`[6, 7]`返回结果 `[10, 13, 5]` 之后将相应的数组项添加在一起。

## -OrDefault {#agg-functions-combinator-ordefault}

如果没有要聚合的内容，则填充聚合函数的返回类型的默认值。

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

## -OrNull {#agg-functions-combinator-ornull}

填充 `null` 如果没有什么聚合。 返回列将为空。

``` sql
SELECT avg(number), avgOrNull(number) FROM numbers(0)
```

``` text
┌─avg(number)─┬─avgOrNull(number)─┐
│         nan │              ᴺᵁᴸᴸ │
└─────────────┴───────────────────┘
```

-OrDefault和-OrNull可以与其他组合器相结合。 当聚合函数不接受空输入时，它很有用。

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

``` text
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

## -重新采样 {#agg-functions-combinator-resample}

允许您将数据划分为组，然后单独聚合这些组中的数据。 通过将一列中的值拆分为间隔来创建组。

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**参数**

-   `start` — Starting value of the whole required interval for `resampling_key` 值。
-   `stop` — Ending value of the whole required interval for `resampling_key` 值。 整个时间间隔不包括 `stop` 价值 `[start, stop)`.
-   `step` — Step for separating the whole interval into subintervals. The `aggFunction` 在每个子区间上独立执行。
-   `resampling_key` — Column whose values are used for separating data into intervals.
-   `aggFunction_params` — `aggFunction` 参数。

**返回值**

-   阵列 `aggFunction` 每个子区间的结果。

**示例**

考虑一下 `people` 具有以下数据的表:

``` text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

让我们得到的人的名字，他们的年龄在于的时间间隔 `[30,60)` 和 `[60,75)`. 由于我们使用整数表示的年龄，我们得到的年龄 `[30, 59]` 和 `[60,74]` 间隔。

要在数组中聚合名称，我们使用 [groupArray](reference.md#agg_function-grouparray) 聚合函数。 这需要一个参数。 在我们的例子中，它是 `name` 列。 该 `groupArrayResample` 函数应该使用 `age` 按年龄聚合名称的列。 要定义所需的时间间隔，我们通过 `30, 75, 30` 参数到 `groupArrayResample` 功能。

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

考虑结果。

`Jonh` 是因为他太年轻了 其他人按照指定的年龄间隔进行分配。

现在让我们计算指定年龄间隔内的总人数和平均工资。

``` sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

``` text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

[原始文章](https://clickhouse.tech/docs/en/query_language/agg_functions/combinators/) <!--hide-->
