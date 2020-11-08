---
toc_priority: 150
---

## initializeAggregation {#initializeaggregation}

Initializes aggregation (on) some lines from input.
Use it for tests or to process columns of types `AggregateFunction` and `AggregationgMergeTree`.

Например можно вставлять в такие столбцы с помощью initializeAggregation или использовать ее в качестве значения по умолчанию.

**Syntax** (without SELECT)

``` sql
initializeAggregation(input_rows_count);
```

**Returned value(s)**

-   Returned values list.

**Example**

Query

```sql
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM system.numbers LIMIT 10000);
```
Result

┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘