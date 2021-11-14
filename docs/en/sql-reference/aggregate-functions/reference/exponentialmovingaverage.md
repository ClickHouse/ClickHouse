---
toc_priority: 108
---

## exponentialMovingAverage {#exponential-moving-average}

An aggregate function that calculates the exponential moving average of values for the determined time. 

**Syntax**

```sql
exponentialMovingAverage(x)(value, timestamp)
```

Each `value` corresponds to the determinate `timestamp`. The half-decay period is the time interval `x` during which the previous values are taken into account. The function returns a weighted average: the older the time point, the less weight the corresponding value is considered to be.

**Arguments**
- `value` - value must be [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), or [Decimal](../../../sql-reference/data-types/decimal.md).
- `timestamp` - timestamp must be [Integer](../../../sql-reference/data-types/int-uint.md).

**Parameters**
- `x` - half-decay period in seconds must be  [Integer](../../../sql-reference/data-types/int-uint.md).

**Returned value(s)**
- Returnes an exponentially smoothed moving average of the values for the past `x` time at the latest point of time.

Type: [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Example**

Input table:

``` text
┌──temperature─┬─timestamp──┐
│          95  │         1  │
│          95  │         2  │
│          95  │         3  │
│          96  │         4  │
│          96  │         5  │
│          96  │         6  │
│          96  │         7  │
│          97  │         8  │
│          97  │         9  │
│          97  │        10  │
│          97  │        11  │
│          98  │        12  │
│          98  │        13  │
│          98  │        14  │
│          98  │        15  │
│          99  │        16  │
│          99  │        17  │
│          99  │        18  │
│         100  │        19  │
│         100  │        20  │
└──────────────┴────────────┘
```

Query: 

```sql
exponentialMovingAverage(5)(temperature, timestamp)
```

Result:

``` text
┌──exponentialMovingAverage(5)(temperature, timestamp)──┐
│                                    92.25779635374204  │
└───────────────────────────────────────────────────────┘
```