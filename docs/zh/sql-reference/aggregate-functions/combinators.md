---
toc_priority: 37
toc_title: 聚合函数组合器
---

# 聚合函数组合器 {#aggregate_functions_combinators}

聚合函数的名称可以附加一个后缀。 这改变了聚合函数的工作方式。

## -If {#agg-functions-combinator-if}

-If可以加到任何聚合函数之后。加了-If之后聚合函数需要接受一个额外的参数，一个条件（Uint8类型），如果条件满足，那聚合函数处理当前的行数据，如果不满足，那返回默认值（通常是0或者空字符串）。

例： `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` 等等。

使用条件聚合函数，您可以一次计算多个条件的聚合，而无需使用子查询和 `JOIN`例如，在Yandex.Metrica，条件聚合函数用于实现段比较功能。

## -Array {#agg-functions-combinator-array}

-Array后缀可以附加到任何聚合函数。 在这种情况下，聚合函数采用的参数 ‘Array(T)’ 类型（数组）而不是 ‘T’ 类型参数。 如果聚合函数接受多个参数，则它必须是长度相等的数组。 在处理数组时，聚合函数的工作方式与所有数组元素的原始聚合函数类似。

示例1： `sumArray(arr)` -总计所有的所有元素 ‘arr’ 阵列。在这个例子中，它可以更简单地编写: `sum(arraySum(arr))`.

示例2： `uniqArray(arr)` – 计算‘arr’中唯一元素的个数。这可以是一个更简单的方法： `uniq(arrayJoin(arr))`，但它并不总是可以添加 ‘arrayJoin’ 到查询。

如果和-If组合，‘Array’ 必须先来，然后 ‘If’. 例： `uniqArrayIf(arr, cond)`， `quantilesTimingArrayIf(level1, level2)(arr, cond)`。由于这个顺序，该 ‘cond’ 参数不会是数组。

## -State {#agg-functions-combinator-state}

如果应用此combinator，则聚合函数不会返回结果值（例如唯一值的数量 [uniq](./reference/uniq.md#agg_function-uniq) 函数），但是返回聚合的中间状态（对于 `uniq`，返回的是计算唯一值的数量的哈希表）。 这是一个 `AggregateFunction(...)` 可用于进一步处理或存储在表中以完成稍后的聚合。

要使用这些状态，请使用:

-   [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) 表引擎。
-   [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) 功能。
-   [runningAccumulate](../../sql-reference/functions/other-functions.md#function-runningaccumulate) 功能。
-   [-Merge](#aggregate_functions_combinators-merge) combinator
-   [-MergeState](#aggregate_functions_combinators-mergestate) combinator

## -Merge {#aggregate_functions_combinators-merge}

如果应用此组合器，则聚合函数将中间聚合状态作为参数，组合状态以完成聚合，并返回结果值。

## -MergeState {#aggregate_functions_combinators-mergestate}

以与-Merge 相同的方式合并中间聚合状态。 但是，它不会返回结果值，而是返回中间聚合状态，类似于-State。

## -ForEach {#agg-functions-combinator-foreach}

将表的聚合函数转换为聚合相应数组项并返回结果数组的数组的聚合函数。 例如, `sumForEach` 对于数组 `[1, 2]`, `[3, 4, 5]`和`[6, 7]`返回结果 `[10, 13, 5]` 之后将相应的数组项添加在一起。

## -OrDefault {#agg-functions-combinator-ordefault}

更改聚合函数的行为。

如果聚合函数没有输入值，则使用此组合器它返回其返回数据类型的默认值。 适用于可以采用空输入数据的聚合函数。

`-OrDefault` 可与其他组合器一起使用。

**语法**

``` sql
<aggFunction>OrDefault(x)
```

**参数**

-   `x` — 聚合函数参数。

**返回值**

如果没有要聚合的内容，则返回聚合函数返回类型的默认值。

类型取决于所使用的聚合函数。

**示例**

查询:

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

结果:

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

还有 `-OrDefault` 可与其他组合器一起使用。 当聚合函数不接受空输入时，它很有用。

查询:

``` sql
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

结果:

``` text
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

## -OrNull {#agg-functions-combinator-ornull}

更改聚合函数的行为。

此组合器将聚合函数的结果转换为 [可为空](../data-types/nullable.md) 数据类型。 如果聚合函数没有值来计算它返回 [NULL](../syntax.md#null-literal).

`-OrNull` 可与其他组合器一起使用。

**语法**

``` sql
<aggFunction>OrNull(x)
```

**参数**

-   `x` — Aggregate function parameters.

**返回值**

-   聚合函数的结果，转换为 `Nullable` 数据类型。
-   `NULL`，如果没有什么聚合。

类型: `Nullable(aggregate function return type)`.

**示例**

添加 `-orNull` 到聚合函数的末尾。

查询:

``` sql
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

结果:

``` text
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

还有 `-OrNull` 可与其他组合器一起使用。 当聚合函数不接受空输入时，它很有用。

查询:

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

结果:

``` text
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

## -Resample {#agg-functions-combinator-resample}

允许您将数据划分为组，然后单独聚合这些组中的数据。 通过将一列中的值拆分为间隔来创建组。

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**参数**

-   `start` — `resampling_key` 开始值。
-   `stop` — `resampling_key` 结束边界。 区间内部不包含 `stop` 值，即 `[start, stop)`.
-   `step` — 分组的步长。 The `aggFunction` 在每个子区间上独立执行。
-   `resampling_key` — 取样列，被用来分组.
-   `aggFunction_params` — `aggFunction` 参数。

**返回值**

-  `aggFunction` 每个子区间的结果，结果为数组。

**示例**

考虑一下 `people` 表具有以下数据的表结构：

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

让我们得到的人的名字，他们的年龄在于的时间间隔 `[30,60)` 和 `[60,75)`。 由于我们使用整数表示的年龄，我们得到的年龄 `[30, 59]` 和 `[60,74]` 间隔。

要在数组中聚合名称，我们使用 [groupArray](./reference/grouparray.md#agg_function-grouparray) 聚合函数。 这需要一个参数。 在我们的例子中，它是 `name` 列。 `groupArrayResample` 函数应该使用 `age` 按年龄聚合名称， 要定义所需的时间间隔，我们传入 `30, 75, 30` 参数给 `groupArrayResample` 函数。

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

考虑结果。

`Jonh` 没有被选中，因为他太年轻了。 其他人按照指定的年龄间隔进行分配。

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

[原始文章](https://clickhouse.com/docs/en/query_language/agg_functions/combinators/) <!--hide-->
