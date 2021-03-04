---
toc_priority: 150
---

## initializeAggregation {#initializeaggregation}

Initializes aggregation for your input rows. It is intended for the functions with the suffix `State`.
Use it for tests or to process columns of types `AggregateFunction` and `AggregationgMergeTree`.

**Syntax**

``` sql
initializeAggregation (aggregate_function, column_1, column_2);
```

**Parameters**

-   `aggregate_function` — Name of the aggregation function. The state of this function — the creating one. [String](../../../sql-reference/data-types/string.md#string).
-   `column_n` — The column to translate it into the function as it's argument. [String](../../../sql-reference/data-types/string.md#string).

**Returned value(s)**

Returns the result of the aggregation for your input rows. The return type will be the same as the return type of function, that `initializeAgregation` takes as first argument.
For example for functions with the suffix `State` the return type will be `AggregateFunction`.

**Example**

Query:

```sql
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM system.numbers LIMIT 10000);
```
Result:

┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘
