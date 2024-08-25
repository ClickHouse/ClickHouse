---
slug: /en/sql-reference/aggregate-functions/reference/stddevpop
sidebar_position: 188
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
