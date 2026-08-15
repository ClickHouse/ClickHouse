---
description: 'Calculates the population variance.'
sidebar_position: 210
slug: /en/sql-reference/aggregate-functions/reference/varPop
title: 'varPop'
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
