---
slug: /en/sql-reference/aggregate-functions/reference/stddevpopstable
sidebar_position: 189
---

# stddevPopStable

The result is equal to the square root of [varPop](../../../sql-reference/aggregate-functions/reference/varpop.md). Unlike [`stddevPop`](../reference/stddevpop.md), this function uses a numerically stable algorithm. It works slower but provides a lower computational error.

**Syntax**

```sql
stddevPopStable(x)
```

**Parameters**

- `x`: Population of values to find the standard deviation of. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Returned value**

Square root of standard deviation of `x`. [Float64](../../data-types/float.md).

**Example**

Query:

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    population Float64,
)
ENGINE = Log;

INSERT INTO test_data SELECT randUniform(5.5, 10) FROM numbers(1000000)

SELECT
    stddevPopStable(population) AS stddev
FROM test_data;
```

Result:

```response
┌─────────────stddev─┐
│ 1.2999977786592576 │
└────────────────────┘
```
