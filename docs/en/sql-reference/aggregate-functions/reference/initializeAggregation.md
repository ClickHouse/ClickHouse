---
toc_priority: 150
---

## initializeAggregation {#initializeaggregation}

Initializes aggregation (on) some lines from input.
Может быть полезно для тестов, а также для работы со столбцами типа AggregateFunction в AggregationgMergeTree. Например можно вставлять в такие столбцы с помощью initializeAggregation или использовать ее в качестве значения по умолчанию.

**Syntax** (without SELECT)

``` sql
initializeAggregation(input_rows_count);
```

**Returned value(s)**

-   Returned values list.

**Example**

The example must show usage and/or a use cases. The following text contains recommended parts of an example.

Input table (Optional):

``` text
```

Query:

``` sql
```

Result:

``` text
```

**See Also** (Optional)

-   [link](#)