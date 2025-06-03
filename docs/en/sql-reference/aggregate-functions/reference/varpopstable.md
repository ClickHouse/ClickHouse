---
title: "varPopStable"
slug: "/en/sql-reference/aggregate-functions/reference/varpopstable"
sidebar_position: 211
---

## varPopStable

Returns the population variance. Unlike [`varPop`](../reference/varpop.md), this function uses a [numerically stable](https://en.wikipedia.org/wiki/Numerical_stability) algorithm. It works slower but provides a lower computational error.

**Syntax**

```sql
varPopStable(x)
```

Alias: `VAR_POP_STABLE`.

**Parameters**

- `x`: Population of values to find the population variance of. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Returned value**

- Returns the population variance of `x`. [Float64](../../data-types/float.md).

**Example**

Query:

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x UInt8,
)
ENGINE = Memory;

INSERT INTO test_data VALUES (3),(3),(3),(4),(4),(5),(5),(7),(11),(15);

SELECT
    varPopStable(x) AS var_pop_stable
FROM test_data;
```

Result:

```response
┌─var_pop_stable─┐
│           14.4 │
└────────────────┘
```
