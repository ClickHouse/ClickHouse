---
toc_priority: 37
toc_title: Combinators
---

# Aggregate Function Combinators {#aggregate_functions_combinators}

The name of an aggregate function can have a suffix appended to it. This changes the way the aggregate function works.

## -If {#agg-functions-combinator-if}

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

Examples: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` and so on.

With conditional aggregate functions, you can calculate aggregates for several conditions at once, without using subqueries and `JOIN`s. For example, in Yandex.Metrica, conditional aggregate functions are used to implement the segment comparison functionality.

## -Array {#agg-functions-combinator-array}

The -Array suffix can be appended to any aggregate function. In this case, the aggregate function takes arguments of the ‘Array(T)’ type (arrays) instead of ‘T’ type arguments. If the aggregate function accepts multiple arguments, this must be arrays of equal lengths. When processing arrays, the aggregate function works like the original aggregate function across all array elements.

Example 1: `sumArray(arr)` - Totals all the elements of all ‘arr’ arrays. In this example, it could have been written more simply: `sum(arraySum(arr))`.

Example 2: `uniqArray(arr)` – Counts the number of unique elements in all ‘arr’ arrays. This could be done an easier way: `uniq(arrayJoin(arr))`, but it’s not always possible to add ‘arrayJoin’ to a query.

-If and -Array can be combined. However, ‘Array’ must come first, then ‘If’. Examples: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. Due to this order, the ‘cond’ argument won’t be an array.

## -State {#agg-functions-combinator-state}

If you apply this combinator, the aggregate function doesn’t return the resulting value (such as the number of unique values for the [uniq](../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) function), but an intermediate state of the aggregation (for `uniq`, this is the hash table for calculating the number of unique values). This is an `AggregateFunction(...)` that can be used for further processing or stored in a table to finish aggregating later.

To work with these states, use:

-   [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) table engine.
-   [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) function.
-   [runningAccumulate](../../sql-reference/functions/other-functions.md#runningaccumulate) function.
-   [-Merge](#aggregate_functions_combinators-merge) combinator.
-   [-MergeState](#aggregate_functions_combinators-mergestate) combinator.

## -Merge {#aggregate_functions_combinators-merge}

If you apply this combinator, the aggregate function takes the intermediate aggregation state as an argument, combines the states to finish aggregation, and returns the resulting value.

## -MergeState {#aggregate_functions_combinators-mergestate}

Merges the intermediate aggregation states in the same way as the -Merge combinator. However, it doesn’t return the resulting value, but an intermediate aggregation state, similar to the -State combinator.

## -ForEach {#agg-functions-combinator-foreach}

Converts an aggregate function for tables into an aggregate function for arrays that aggregates the corresponding array items and returns an array of results. For example, `sumForEach` for the arrays `[1, 2]`, `[3, 4, 5]`and`[6, 7]`returns the result `[10, 13, 5]` after adding together the corresponding array items.

## -OrDefault {#agg-functions-combinator-ordefault}

Changes behavior of an aggregate function.

If an aggregate function doesn’t have input values, with this combinator it returns the default value for its return data type. Applies to the aggregate functions that can take empty input data.

`-OrDefault` can be used with other combinators.

**Syntax**

``` sql
<aggFunction>OrDefault(x)
```

**Parameters**

-   `x` — Aggregate function parameters.

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

## -OrNull {#agg-functions-combinator-ornull}

Changes behavior of an aggregate function.

This combinator converts a result of an aggregate function to the [Nullable](../../sql-reference/data-types/nullable.md) data type. If the aggregate function does not have values to calculate it returns [NULL](../../sql-reference/syntax.md#null-literal).

`-OrNull` can be used with other combinators.

**Syntax**

``` sql
<aggFunction>OrNull(x)
```

**Parameters**

-   `x` — Aggregate function parameters.

**Returned values**

-   The result of the aggregate function, converted to the `Nullable` data type.
-   `NULL`, if there is nothing to aggregate.

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

## -Resample {#agg-functions-combinator-resample}

Lets you divide data into groups, and then separately aggregates the data in those groups. Groups are created by splitting the values from one column into intervals.

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**Parameters**

-   `start` — Starting value of the whole required interval for `resampling_key` values.
-   `stop` — Ending value of the whole required interval for `resampling_key` values. The whole interval doesn’t include the `stop` value `[start, stop)`.
-   `step` — Step for separating the whole interval into subintervals. The `aggFunction` is executed over each of those subintervals independently.
-   `resampling_key` — Column whose values are used for separating data into intervals.
-   `aggFunction_params` — `aggFunction` parameters.

**Returned values**

-   Array of `aggFunction` results for each subinterval.

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

`Jonh` is out of the sample because he’s too young. Other people are distributed according to the specified age intervals.

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

[Original article](https://clickhouse.tech/docs/en/query_language/agg_functions/combinators/) <!--hide-->
