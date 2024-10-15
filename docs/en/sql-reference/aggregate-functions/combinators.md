---
slug: /en/sql-reference/aggregate-functions/combinators
sidebar_position: 37
sidebar_label: Combinators
---

# Aggregate Function Combinators

The name of an aggregate function can have a suffix appended to it. This changes the way the aggregate function works.

## -If

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

Examples: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` and so on.

With conditional aggregate functions, you can calculate aggregates for several conditions at once, without using subqueries and `JOIN`s. For example, conditional aggregate functions can be used to implement the segment comparison functionality.

## -Array

The -Array suffix can be appended to any aggregate function. In this case, the aggregate function takes arguments of the ‘Array(T)’ type (arrays) instead of ‘T’ type arguments. If the aggregate function accepts multiple arguments, this must be arrays of equal lengths. When processing arrays, the aggregate function works like the original aggregate function across all array elements.

Example 1: `sumArray(arr)` - Totals all the elements of all ‘arr’ arrays. In this example, it could have been written more simply: `sum(arraySum(arr))`.

Example 2: `uniqArray(arr)` – Counts the number of unique elements in all ‘arr’ arrays. This could be done an easier way: `uniq(arrayJoin(arr))`, but it’s not always possible to add ‘arrayJoin’ to a query.

-If and -Array can be combined. However, ‘Array’ must come first, then ‘If’. Examples: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. Due to this order, the ‘cond’ argument won’t be an array.

## -Map

The -Map suffix can be appended to any aggregate function. This will create an aggregate function which gets Map type as an argument, and aggregates values of each key of the map separately using the specified aggregate function. The result is also of a Map type.

**Example**

```sql
CREATE TABLE map_map(
    date Date,
    timeslot DateTime,
    status Map(String, UInt64)
) ENGINE = Log;

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

## -SimpleState

If you apply this combinator, the aggregate function returns the same value but with a different type. This is a [SimpleAggregateFunction(...)](../../sql-reference/data-types/simpleaggregatefunction.md) that can be stored in a table to work with [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) tables.

**Syntax**

``` sql
<aggFunction>SimpleState(x)
```

**Arguments**

- `x` — Aggregate function parameters.

**Returned values**

The value of an aggregate function with the `SimpleAggregateFunction(...)` type.

**Example**

Query:

``` sql
WITH anySimpleState(number) AS c SELECT toTypeName(c), c FROM numbers(1);
```

Result:

``` text
┌─toTypeName(c)────────────────────────┬─c─┐
│ SimpleAggregateFunction(any, UInt64) │ 0 │
└──────────────────────────────────────┴───┘
```

## -State

If you apply this combinator, the aggregate function does not return the resulting value (such as the number of unique values for the [uniq](../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) function), but an intermediate state of the aggregation (for `uniq`, this is the hash table for calculating the number of unique values). This is an `AggregateFunction(...)` that can be used for further processing or stored in a table to finish aggregating later.

:::note
Please notice, that -MapState is not an invariant for the same data due to the fact that order of data in intermediate state can change, though it doesn't impact ingestion of this data.
:::

To work with these states, use:

- [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) table engine.
- [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) function.
- [runningAccumulate](../../sql-reference/functions/other-functions.md#runningaccumulate) function.
- [-Merge](#-merge) combinator.
- [-MergeState](#-mergestate) combinator.

## -Merge

If you apply this combinator, the aggregate function takes the intermediate aggregation state as an argument, combines the states to finish aggregation, and returns the resulting value.

## -MergeState

Merges the intermediate aggregation states in the same way as the -Merge combinator. However, it does not return the resulting value, but an intermediate aggregation state, similar to the -State combinator.

## -ForEach

Converts an aggregate function for tables into an aggregate function for arrays that aggregates the corresponding array items and returns an array of results. For example, `sumForEach` for the arrays `[1, 2]`, `[3, 4, 5]`and`[6, 7]`returns the result `[10, 13, 5]` after adding together the corresponding array items.

## -Distinct

Every unique combination of arguments will be aggregated only once. Repeating values are ignored.
Examples: `sum(DISTINCT x)` (or `sumDistinct(x)`), `groupArray(DISTINCT x)` (or `groupArrayDistinct(x)`), `corrStable(DISTINCT x, y)` (or `corrStableDistinct(x, y)`) and so on.

## -OrDefault

Changes behavior of an aggregate function.

If an aggregate function does not have input values, with this combinator it returns the default value for its return data type. Applies to the aggregate functions that can take empty input data.

`-OrDefault` can be used with other combinators.

**Syntax**

``` sql
<aggFunction>OrDefault(x)
```

**Arguments**

- `x` — Aggregate function parameters.

**Returned values**

Returns the default value of an aggregate function’s return type if there is nothing to aggregate.

Type depends on the aggregate function used.

**Example**

Query:

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

Result:

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

Also `-OrDefault` can be used with another combinators. It is useful when the aggregate function does not accept the empty input.

Query:

``` sql
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

Result:

``` text
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

## -OrNull

Changes behavior of an aggregate function.

This combinator converts a result of an aggregate function to the [Nullable](../../sql-reference/data-types/nullable.md) data type. If the aggregate function does not have values to calculate it returns [NULL](../../sql-reference/syntax.md#null-literal).

`-OrNull` can be used with other combinators.

**Syntax**

``` sql
<aggFunction>OrNull(x)
```

**Arguments**

- `x` — Aggregate function parameters.

**Returned values**

- The result of the aggregate function, converted to the `Nullable` data type.
- `NULL`, if there is nothing to aggregate.

Type: `Nullable(aggregate function return type)`.

**Example**

Add `-orNull` to the end of aggregate function.

Query:

``` sql
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

Result:

``` text
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

Also `-OrNull` can be used with another combinators. It is useful when the aggregate function does not accept the empty input.

Query:

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

Result:

``` text
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

## -Resample

Lets you divide data into groups, and then separately aggregates the data in those groups. Groups are created by splitting the values from one column into intervals.

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**Arguments**

- `start` — Starting value of the whole required interval for `resampling_key` values.
- `stop` — Ending value of the whole required interval for `resampling_key` values. The whole interval does not include the `stop` value `[start, stop)`.
- `step` — Step for separating the whole interval into subintervals. The `aggFunction` is executed over each of those subintervals independently.
- `resampling_key` — Column whose values are used for separating data into intervals.
- `aggFunction_params` — `aggFunction` parameters.

**Returned values**

- Array of `aggFunction` results for each subinterval.

**Example**

Consider the `people` table with the following data:

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

Let’s get the names of the people whose age lies in the intervals of `[30,60)` and `[60,75)`. Since we use integer representation for age, we get ages in the `[30, 59]` and `[60,74]` intervals.

To aggregate names in an array, we use the [groupArray](../../sql-reference/aggregate-functions/reference/grouparray.md#agg_function-grouparray) aggregate function. It takes one argument. In our case, it’s the `name` column. The `groupArrayResample` function should use the `age` column to aggregate names by age. To define the required intervals, we pass the `30, 75, 30` arguments into the `groupArrayResample` function.

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

Consider the results.

`John` is out of the sample because he’s too young. Other people are distributed according to the specified age intervals.

Now let’s count the total number of people and their average wage in the specified age intervals.

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

## -ArgMin

The suffix -ArgMin can be appended to the name of any aggregate function. In this case, the aggregate function accepts an additional argument, which should be any comparable expression. The aggregate function processes only the rows that have the minimum value for the specified extra expression.

Examples: `sumArgMin(column, expr)`, `countArgMin(expr)`, `avgArgMin(x, expr)` and so on.

## -ArgMax

Similar to suffix -ArgMin but processes only the rows that have the maximum value for the specified extra expression.

## Related Content

- Blog: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
