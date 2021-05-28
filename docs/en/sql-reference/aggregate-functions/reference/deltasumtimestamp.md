---
toc_priority: 141
---

# deltaSumTimestamp {#agg_functions-deltasum}

Syntax: `deltaSumTimestamp(value, timestamp)`

Adds the differences between consecutive rows. If the difference is negative, it is ignored. 
Uses `timestamp` to order values. 

This function is primarily for materialized views that are ordered by some time bucket aligned
timestamp, for example a `toStartOfMinute` bucket. Because the rows in such a materialized view
will all have the same timestamp, it is impossible for them to be merged in the "right" order. This
function keeps track of the `timestamp` of the values it's seen, so it's possible to order the states
correctly during merging.

To calculate the delta sum across an ordered collection you can simply use the 
[deltaSum](./deltasum.md) function.

**Arguments**

- `value` must be some [Integer](../../data-types/int-uint.md) type or [Float](../../data-types/float.md) type or a [Date](../../data-types/date.md) or [DateTime](../../data-types/datetime.md).
- `timestamp` must be some [Integer](../../data-types/int-uint.md) type or [Float](../../data-types/float.md) type or a [Date](../../data-types/date.md) or [DateTime](../../data-types/datetime.md).

**Returned value**

- Accumulated differences between consecutive values, ordered by the `timestamp` parameter.

**Example**

```sql
SELECT deltaSumTimestamp(value, timestamp) 
FROM (select number as timestamp, [0, 4, 8, 3, 0, 0, 0, 1, 3, 5][number] as value from numbers(1, 10))
```

``` text
┌─deltaSumTimestamp(value, timestamp)─┐
│                                  13 │
└─────────────────────────────────────┘
```
