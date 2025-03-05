---
slug: /sql-reference/aggregate-functions/reference/max
sidebar_position: 162
title: max
description: "Aggregate function that calculates the maximum across a group of values."
---

Aggregate function that calculates the maximum across a group of values.

Example:

```sql
SELECT max(salary) FROM employees;
```

```sql
SELECT department, max(salary) FROM employees GROUP BY department;
```

If you need non-aggregate function to choose a maximum of two values, see `greatest`:

```sql
SELECT greatest(a, b) FROM table;
```

## Combinators

The following combinators can be applied to the `max` function:

### maxIf
Calculates the maximum only for rows that match the given condition.

### maxArray
Calculates the maximum among elements in the array.

### maxMap
Calculates the maximum for each key in the map separately.

### maxSimpleState
Returns the maximum value with SimpleAggregateFunction type.

### maxState
Returns the intermediate state of maximum calculation.

### maxMerge
Combines intermediate maximum states to get the final maximum.

### maxMergeState
Combines intermediate maximum states but returns an intermediate state.

### maxForEach
Calculates the maximum for corresponding elements in multiple arrays.

### maxDistinct
Calculates the maximum among distinct values only.

### maxOrDefault
Returns the default value for the input type if there are no rows to calculate maximum.

### maxOrNull
Returns NULL if there are no rows to calculate maximum.
