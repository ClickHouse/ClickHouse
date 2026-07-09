---
description: '聚合函数组合器文档'
sidebar_label: '组合器'
sidebar_position: 37
slug: /sql-reference/aggregate-functions/combinators
title: '聚合函数组合器'
doc_type: 'reference'
---

聚合函数名称后可以附加后缀，从而改变该聚合函数的工作方式。

<div id="-if">
  ## -If
</div>

后缀 -If 可以附加到任何聚合函数的名称后。在这种情况下，聚合函数会接受一个额外的参数——条件 (Uint8 类型) 。聚合函数只处理满足该条件的行。如果该条件一次都未触发，则返回默认值 (通常为零或空字符串) 。

示例：`sumIf(column, cond)`、`countIf(cond)`、`avgIf(x, cond)`、`quantilesTimingIf(level1, level2)(x, cond)`、`argMinIf(arg, val, cond)` 等。

使用条件聚合函数，你可以一次为多个条件计算聚合结果，而无需使用子查询和 `JOIN`。例如，条件聚合函数可用于实现分段比较功能。

<div id="-array">
  ## -Array
</div>

可以将 -Array 后缀附加到任何聚合函数上。在这种情况下，聚合函数接受的参数类型是 &#39;Array(T)&#39; (数组) ，而不是 &#39;T&#39;。如果聚合函数接受多个参数，则这些参数必须是长度相同的数组。处理数组时，聚合函数会像原始聚合函数一样，对所有数组元素进行聚合。

示例 1：`sumArray(arr)` - 对所有 &#39;arr&#39; 数组中的全部元素求和。在这个示例中，也可以写成更简单的形式：`sum(arraySum(arr))`。

示例 2：`uniqArray(arr)` – 统计所有 &#39;arr&#39; 数组中唯一元素的数量。也可以用更简单的方式实现：`uniq(arrayJoin(arr))`，但并不总能在查询中添加 &#39;arrayJoin&#39;。

-If 和 -Array 可以组合使用。不过，&#39;Array&#39; 必须在前，&#39;If&#39; 必须在后。示例：`uniqArrayIf(arr, cond)`、`quantilesTimingArrayIf(level1, level2)(arr, cond)`。由于这一顺序，&#39;cond&#39; 参数不会是数组。

<div id="-map">
  ## -Map
</div>

`-Map` 后缀可附加到任何聚合函数上。这会创建一个以 Map 类型作为参数的聚合函数，并使用指定的聚合函数分别对映射中每个键对应的值进行聚合。结果也将是 Map 类型。

**示例**

```sql
CREATE TABLE map_map(
    date Date,
    timeslot DateTime,
    status Map(String, UInt64)
) ENGINE = MergeTree
ORDER BY ();

INSERT INTO map_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', (['a', 'b', 'c'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', (['c', 'd', 'e'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['d', 'e', 'f'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['f', 'g', 'g'], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(status),
    avgMap(status),
    minMap(status)
FROM map_map
GROUP BY timeslot;

┌────────────timeslot─┬─sumMap(status)───────────────────────┬─avgMap(status)───────────────────────┬─minMap(status)───────────────────────┐
│ 2000-01-01 00:00:00 │ {'a':10,'b':10,'c':20,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │
│ 2000-01-01 00:01:00 │ {'d':10,'e':10,'f':20,'g':20}        │ {'d':10,'e':10,'f':10,'g':10}        │ {'d':10,'e':10,'f':10,'g':10}        │
└─────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┘
```

<div id="-simplestate">
  ## -SimpleState
</div>

如果应用此组合器，聚合函数返回的值相同，但类型会不同。它是一个 [SimpleAggregateFunction(...)](../../sql-reference/data-types/simpleaggregatefunction.md)，可存储在表中，以便与 [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) 表配合使用。

**语法**

```sql
<aggFunction>SimpleState(x)
```

**参数**

* `x` — 聚合函数的参数。

**返回值**

`SimpleAggregateFunction(...)` 类型的聚合函数值。

**示例**

```sql title="Query"
WITH anySimpleState(number) AS c SELECT toTypeName(c), c FROM numbers(1);
```

```text title="Response"
┌─toTypeName(c)────────────────────────┬─c─┐
│ SimpleAggregateFunction(any, UInt64) │ 0 │
└──────────────────────────────────────┴───┘
```

<div id="-state">
  ## -State
</div>

如果应用这个组合器，聚合函数返回的不是最终结果值 (例如 [uniq](/zh/sql-reference/aggregate-functions/reference/uniq) 函数返回的唯一值数量) ，而是聚合的中间状态 (对于 `uniq`，就是用于计算唯一值数量的哈希表) 。这是一个 `AggregateFunction(...)`，可用于进一步处理，或者存储在表中，以便稍后完成聚合。

:::note
请注意，由于中间状态中的数据顺序可能发生变化，-MapState 对于相同数据并不是不变的，不过这不会影响这些数据的摄取。
:::

要处理这些状态，请使用：

* [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) 表引擎。
* [finalizeAggregation](/zh/sql-reference/functions/other-functions#finalizeAggregation) 函数。
* [runningAccumulate](../../sql-reference/functions/other-functions.md#runningAccumulate) 函数。
* [-Merge](#-merge) 组合器。
* [-MergeState](#-mergestate) 组合器。

<div id="-merge">
  ## -Merge
</div>

如果应用此组合器，聚合函数会将中间聚合状态作为参数，合并这些状态以完成聚合，并返回最终结果值。

<div id="-mergestate">
  ## -MergeState
</div>

以与 -Merge 组合器相同的方式合并中间聚合状态。但它返回的不是结果值，而是像 -State 组合器一样返回一个中间聚合状态。

<div id="-foreach">
  ## -ForEach
</div>

将用于表的聚合函数转换为用于数组的聚合函数，对对应位置的数组元素进行聚合，并返回结果数组。例如，对于数组 `[1, 2]`、`[3, 4, 5]` 和 `[6, 7]`，`sumForEach` 会将对应位置的数组元素相加，并返回结果 `[10, 13, 5]`。

<div id="-tuple">
  ## -Tuple
</div>

`-Tuple` 后缀可以附加到任何聚合函数上。组合后的函数会为底层聚合函数的每个参数接收一个 `Tuple` 类型的参数；所有 `Tuple` 都必须包含相同数量的元素。聚合会在每个元素位置分别进行，接收每个 `Tuple` 中对应位置的元素，并返回一个结果 `Tuple`。

如果第一个输入 `Tuple` 具有显式的元素名称，这些名称会在结果中保留。

可自行处理 `NULL` 值的聚合函数 (`anyRespectNulls`、`anyLastRespectNulls`、`RESPECT NULLS` 修饰符) 不支持将 `Nullable(Tuple(...))` 类型用作参数；请改用 `Nullable` 元素。

**语法**

```sql
<aggFunction>Tuple(tuple1[, tuple2, ...])
```

**参数**

* `tuple1[, tuple2, ...]` — `Tuple` 类型的列，每个参数对应底层聚合函数的一个参数，且这些列的元素个数必须相同。每个元素都必须是在对应参数位置上受底层聚合函数支持的类型。

**返回值**

* 一个 `Tuple`，其中包含将聚合函数分别应用到各个元素后得到的结果。

类型：`Tuple(aggFunction(element1), aggFunction(element2), ...)`。

**示例**

查询：

```sql
SELECT sumTuple(t) FROM
(
    SELECT tuple(toInt64(1), toFloat64(2.5)) AS t
    UNION ALL
    SELECT tuple(toInt64(3), toFloat64(4.5))
    UNION ALL
    SELECT tuple(toInt64(5), toFloat64(6.5))
);
```

结果：

```text
┌─sumTuple(t)─┐
│ (9,13.5)    │
└─────────────┘
```

与 `GROUP BY` 搭配使用：

```sql
SELECT
    k,
    avgTuple(t)
FROM
(
    SELECT
        number % 2 AS k,
        tuple(toInt64(number), toFloat64(number) * 1.5) AS t
    FROM numbers(6)
)
GROUP BY k
ORDER BY k;
```

```text
┌─k─┬─avgTuple(t)─┐
│ 0 │ (2,3)       │
│ 1 │ (3,4.5)     │
└───┴─────────────┘
```

与多参数聚合函数一起使用时：每个 `Tuple` 参数都为底层函数提供一个参数，元素按位置一一配对：

```text
corrTuple((a1, a2), (b1, b2)) = (corr(a1, b1), corr(a2, b2))
```

```sql
SELECT corrTuple((a1, a2), (b1, b2))
FROM
(
    SELECT
        toFloat64(number) AS a1,
        toFloat64(number * 2) AS a2,
        toFloat64(100 - number) AS b1,
        toFloat64(number * 3) AS b2
    FROM numbers(10)
);
```

```text
┌─corrTuple((a1, a2), (b1, b2))─┐
│ (-1,1)                        │
└───────────────────────────────┘
```

`a1` 和 `b1` 呈负相关，而 `a2` 和 `b2` 成正比，因此结果为 `(-1, 1)`。

`-Tuple` 可以与其他组合器 (如 `-If`) 结合使用。例如：`sumTupleIf(tuple_column, cond)`。

<div id="-distinct">
  ## -Distinct
</div>

每种唯一的参数组合只会被聚合一次。重复的值会被忽略。
示例：`sum(DISTINCT x)` (或 `sumDistinct(x)`) 、`groupArray(DISTINCT x)` (或 `groupArrayDistinct(x)`) 、`corrStable(DISTINCT x, y)` (或 `corrStableDistinct(x, y)`) 等。

<div id="-ordefault">
  ## -OrDefault
</div>

改变聚合函数的行为。

如果聚合函数没有输入值，使用此组合器时，会返回其返回数据类型的默认值。适用于可接受空输入数据的聚合函数。

`-OrDefault` 可以与其他组合器配合使用。

**语法**

```sql
<aggFunction>OrDefault(x)
```

**参数**

* `x` — 聚合函数的参数。

**返回值**

如果没有可聚合的数据，则返回该聚合函数返回类型的默认值。

类型取决于所使用的聚合函数。

**示例**

```sql title="Query"
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

```text title="Response"
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

此外，`-OrDefault` 也可以与其他组合器配合使用。在聚合函数不接受空输入时，它就很有用。

```sql title="Query"
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

<div id="-ornull">
  ## -OrNull
</div>

改变聚合函数的行为。

此组合器会将聚合函数的结果转换为 [Nullable](../../sql-reference/data-types/nullable.md) 数据类型。如果聚合函数没有可供计算的值，则返回 [NULL](/zh/operations/settings/formats#input_format_null_as_default)。

`-OrNull` 可与其他组合器一起使用。

**语法**

```sql
<aggFunction>OrNull(x)
```

**参数**

* `x` — 聚合函数的参数。

**返回值**

* 聚合函数的结果，转换为 `Nullable` 数据类型。
* 如果没有可供聚合的内容，则返回 `NULL`。

类型：`Nullable(aggregate function return type)`。

**示例**

在聚合函数末尾加上 `-orNull`。

```sql title="Query"
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

```text title="Response"
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

此外，`-OrNull` 也可以与其他组合器配合使用。当聚合函数不接受空输入时，它就很有用。

```sql title="Query"
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

<div id="-resample">
  ## -Resample
</div>

可将数据划分为多个组，然后分别对各组中的数据进行聚合。分组是通过将某一列中的值拆分到不同区间中来创建的。

```sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**参数**

* `start` — `resampling_key` 值所属整个区间的起始值。
* `stop` — `resampling_key` 值所属整个区间的结束值。整个区间不包含 `stop` 值，即 `[start, stop)`。
* `step` — 将整个区间划分为多个子区间的步长。`aggFunction` 会在每个子区间上独立执行。
* `resampling_key` — 其值用于将数据划分到各个区间的列。
* `aggFunction_params` — `aggFunction` 的参数。

**返回值**

* 由各个子区间的 `aggFunction` 结果组成的数组。

**示例**

假设 `people` 表包含以下数据：

```text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

让我们获取年龄落在 `[30,60)` 和 `[60,75)` 区间内的人员姓名。由于年龄使用整数表示，因此对应的年龄范围分别是 `[30, 59]` 和 `[60,74]`。

要将姓名聚合到数组中，我们使用 [groupArray](/zh/sql-reference/aggregate-functions/reference/grouparray) 聚合函数。它接受一个参数。在本例中，这个参数是 `name` 列。`groupArrayResample` 函数应使用 `age` 列按年龄聚合姓名。为了定义所需的区间，我们向 `groupArrayResample` 函数传入 `30, 75, 30` 这几个参数。

```sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

```text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

请看结果。

`John` 不在样本中，因为他太年轻了。其他人则按指定的年龄区间分布。

现在让我们统计指定年龄区间内的总人数及其平均工资。

```sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

```text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

<div id="-argmin">
  ## -ArgMin
</div>

后缀 -ArgMin 可附加到任何聚合函数的名称后。在这种情况下，聚合函数会接受一个额外参数，该参数可以是任意可比较的表达式。聚合函数仅处理在指定额外表达式上取最小值的那些行。

示例：`sumArgMin(column, expr)`、`countArgMin(expr)`、`avgArgMin(x, expr)` 等。

<div id="-argmax">
  ## -ArgMax
</div>

与后缀 -ArgMin 类似，但只处理指定附加表达式取最大值的行。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[在 ClickHouse 中使用聚合组合器：Array、Map 和状态](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)