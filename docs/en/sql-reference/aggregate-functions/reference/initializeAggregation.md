---
toc_priority: 150
---

## initializeAggregation {#initializeaggregation}

Initializes aggregation for your input lines.
Use it for tests or to process columns of types `AggregateFunction` and `AggregationgMergeTree`.

**Syntax**

``` sql
initializeAggregation(input_rows_count);
```

**Returned value(s)**

Returns the result of the aggregation for your input lines.

**Example**

Query:

```sql
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM system.numbers LIMIT 10000);
```
Result:

┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘