---
description: 'Selects a frequently occurring value using the heavy hitters algorithm.
  If there is a value that occurs more than in half the cases in each of the query
  execution threads, this value is returned. Normally, the result is nondeterministic.'
sidebar_position: 104
slug: /sql-reference/aggregate-functions/reference/anyheavy
title: 'anyHeavy'
---

# anyHeavy

Selects a frequently occurring value using the [heavy hitters](https://doi.org/10.1145/762471.762473) algorithm. If there is a value that occurs more than in half the cases in each of the query's execution threads, this value is returned. Normally, the result is nondeterministic.

```sql
anyHeavy(column)
```

**Arguments**

- `column` – The column name.

**Example**

Take the [OnTime](../../../getting-started/example-datasets/ontime.md) data set and select any frequently occurring value in the `AirlineID` column.

```sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

```text
┌───res─┐
│ 19690 │
└───────┘
```
