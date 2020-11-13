---
toc_priority: 150
---

## initializeAggregation {#initializeaggregation}

Initializes aggregation for your input rows. It is intended for the functions with the suffix `State`.
Use it for tests or to process columns of types `AggregateFunction` and `AggregationgMergeTree`.

**Syntax**

``` sql
initializeAggregation(input_rows_count);
```

**Parameters**

-   `input_rows_count` — Number of your inputed rows. [UInt64](../sql-reference/data-types/int-uint.md#uint-ranges).
-   `uniqState` — Uniq identifier key of the state. [Type name](relative/path/to/type/dscr.md#type).

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