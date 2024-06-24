---
title: "varPop"
slug: "/en/sql-reference/aggregate-functions/reference/varPop"
sidebar_position: 210
---

## varPop

Calculates the population variance.

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
