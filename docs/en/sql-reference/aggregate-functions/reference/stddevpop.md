---
slug: /sql-reference/aggregate-functions/reference/stddevpop
sidebar_position: 188
title: "stddevPop"
description: "The result is equal to the square root of varPop."
---

# stddevPop

The result is equal to the square root of [varPop](../../../sql-reference/aggregate-functions/reference/varpop.md).

Aliases: `STD`, `STDDEV_POP`.

:::note
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`stddevPopStable`](../reference/stddevpopstable.md) function. It works slower but provides a lower computational error.
:::

**Syntax**

```sql
stddevPop(x)
```

**Parameters**

- `x`: Population of values to find the standard deviation of. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Returned value**

- Square root of standard deviation of `x`. [Float64](../../data-types/float.md).


**Example**

Query:

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    population UInt8,
)
ENGINE = Log;

INSERT INTO test_data VALUES (3),(3),(3),(4),(4),(5),(5),(7),(11),(15);

SELECT
    stddevPop(population) AS stddev
FROM test_data;
```

Result:

```response
┌────────────stddev─┐
│ 3.794733192202055 │
└───────────────────┘
```

## Combinators

The following combinators can be applied to the `stddevPop` function:

### stddevPopIf
Calculates the population standard deviation only for rows that match the given condition.

### stddevPopArray
Calculates the population standard deviation of elements in the array.

### stddevPopMap
Calculates the population standard deviation for each key in the map separately.

### stddevPopSimpleState
Returns the population standard deviation value with SimpleAggregateFunction type.

### stddevPopState
Returns the intermediate state of population standard deviation calculation.

### stddevPopMerge
Combines intermediate population standard deviation states to get the final result.

### stddevPopMergeState
Combines intermediate population standard deviation states but returns an intermediate state.

### stddevPopForEach
Calculates the population standard deviation for corresponding elements in multiple arrays.

### stddevPopDistinct
Calculates the population standard deviation of distinct values only.

### stddevPopOrDefault
Returns 0 if there are no rows to calculate population standard deviation.

### stddevPopOrNull
Returns NULL if there are no rows to calculate population standard deviation.
