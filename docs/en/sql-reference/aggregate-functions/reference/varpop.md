---
title: "varPop"
slug: /en/sql-reference/aggregate-functions/reference/varPop
sidebar_position: 210
description: "Calculates the population variance."
---

## varPop {#varpop}

Calculates the population variance:

$$
\frac{\Sigma{(x - \bar{x})^2}}{n}
$$

**Syntax**

```sql
varPop(x)
```

Alias: `VAR_POP`.

**Parameters**

- `x`: Population of values to find the population variance of. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Returned value**

- Returns the population variance of `x`. [`Float64`](../../data-types/float.md).

**Example**

Query:

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x UInt8,
)
ENGINE = Memory;

INSERT INTO test_data VALUES (3), (3), (3), (4), (4), (5), (5), (7), (11), (15);

SELECT
    varPop(x) AS var_pop
FROM test_data;
```

Result:

```response
┌─var_pop─┐
│    14.4 │
└─────────┘
```

## Combinators

The following combinators can be applied to the `varPop` function:

### varPopIf
Calculates the population variance only for rows that match the given condition.

### varPopArray
Calculates the population variance of elements in the array.

### varPopMap
Calculates the population variance for each key in the map separately.

### varPopSimpleState
Returns the population variance value with SimpleAggregateFunction type.

### varPopState
Returns the intermediate state of population variance calculation.

### varPopMerge
Combines intermediate population variance states to get the final result.

### varPopMergeState
Combines intermediate population variance states but returns an intermediate state.

### varPopForEach
Calculates the population variance for corresponding elements in multiple arrays.

### varPopDistinct
Calculates the population variance of distinct values only.

### varPopOrDefault
Returns 0 if there are no rows to calculate population variance.

### varPopOrNull
Returns NULL if there are no rows to calculate population variance.
