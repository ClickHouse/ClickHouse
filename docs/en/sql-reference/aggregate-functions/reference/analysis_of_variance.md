---
slug: /en/sql-reference/aggregate-functions/reference/analysis_of_variance
sidebar_position: 101
---

# analysisOfVariance

Provides a statistical test for one-way analysis of variance (ANOVA test). It is a test over several groups of normally distributed observations to find out whether all groups have the same mean or not. 

**Syntax**

```sql
analysisOfVariance(val, group_no)
```

Aliases: `anova`

**Parameters**
- `val`: value. 
- `group_no` : group number that `val` belongs to.

:::note
Groups are enumerated starting from 0 and there should be at least two groups to perform a test.
There should be at least one group with the number of observations greater than one.
:::

**Returned value**

- `(f_statistic, p_value)`. [Tuple](../../data-types/tuple.md)([Float64](../../data-types/float.md), [Float64](../../data-types/float.md)).

**Example**

Query:

```sql
SELECT analysisOfVariance(number, number % 2) FROM numbers(1048575);
```

Result:

```response
┌─analysisOfVariance(number, modulo(number, 2))─┐
│ (0,1)                                         │
└───────────────────────────────────────────────┘
```
