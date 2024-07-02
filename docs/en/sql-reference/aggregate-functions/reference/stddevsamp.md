---
slug: /en/sql-reference/aggregate-functions/reference/stddevsamp
sidebar_position: 190
---

# stddevSamp

The result is equal to the square root of [varSamp](../../../sql-reference/aggregate-functions/reference/varsamp.md).

Alias: `STDDEV_SAMP`.

:::note
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`stddevSampStable`](../reference/stddevsampstable.md) function. It works slower but provides a lower computational error.
:::

**Syntax**

```sql
stddevSamp(x)
```

**Parameters**

- `x`: Values for which to find the square root of sample variance. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Returned value**

Square root of sample variance of `x`. [Float64](../../data-types/float.md).

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
    stddevSamp(population)
FROM test_data;
```

Result:

```response
┌─stddevSamp(population)─┐
│                      4 │
└────────────────────────┘
```