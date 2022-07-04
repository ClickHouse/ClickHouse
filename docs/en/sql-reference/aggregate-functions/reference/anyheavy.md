---
sidebar_position: 103
---

# anyHeavy {#anyheavyx}

Selects a frequently occurring value using the [heavy hitters](http://www.cs.umd.edu/~samir/498/karp.pdf) algorithm. If there is a value that occurs more than in half the cases in each of the query’s execution threads, this value is returned. Normally, the result is nondeterministic.

``` sql
anyHeavy(column)
```

**Arguments**

-   `column` – The column name.

**Example**

Take the [OnTime](../../../getting-started/example-datasets/ontime.md) data set and select any frequently occurring value in the `AirlineID` column.

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```
