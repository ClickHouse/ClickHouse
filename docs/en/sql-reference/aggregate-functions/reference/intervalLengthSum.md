---
toc_priority: 146
toc_title: intervalLengthSum
---

# intervalLengthSum {#agg_function-intervallengthsum}

Calculates the sum of the length of all ranges (segments on numeric axis) excluding intersections.

**Syntax**

``` sql
intervalLengthSum(start, end)
```

**Arguments**

-   `start` — The starting value of the interval. [Int32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Int64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Float32](../../../sql-reference/data-types/float.md#float32-float64), [Float64](../../../sql-reference/data-types/float.md#float32-float64), [DateTime](../../../sql-reference/data-types/datetime.md#data_type-datetime) or [Date](../../../sql-reference/data-types/date.md#data_type-date).
-   `end` — Ending interval value. [Int32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Int64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Float32](../../../sql-reference/data-types/float.md#float32-float64), [Float64](../../../sql-reference/data-types/float.md#float32-float64), [DateTime](../../../sql-reference/data-types/datetime.md#data_type-datetime) or [Date](../../../sql-reference/data-types/date.md#data_type-date).

!!! info "Note"
    Arguments must be of the same data type. Otherwise, an exception will be thrown.

**Returned value**

-   Sum of the length of all ranges (segments on numeric axis) without counting intersection twice. If the arguments are of integer type, the function returns a value of the [UInt64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64) type. If the arguments are of the floating-point type, the function returns a value of the [Float64](../../../sql-reference/data-types/float.md#float32-float64) type.

**Examples**

1. Input table:

``` text
┌─id─┬─start─┬─end─┐
│ a  │     1 │   3 │
│ a  │     5 │   9 │
└────┴───────┴─────┘
```

In this example, the non-intersecting intervals are summed up:

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM interval GROUP BY id ORDER BY id;
```

Result:

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                             6 │ UInt64                                    │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```

2. Input table:

``` text
┌─id─┬─start─┬─end─┐
│ a  │     1 │   3 │
│ a  │     2 │   4 │
└────┴───────┴─────┘
```

In this example, the intersecting intervals are summed up. In this case, calculates the sum of the length of ranges without counting intersection twice:

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM interval GROUP BY id ORDER BY id;
```

Result:

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                             3 │ UInt64                                    │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```

3. Input table:

``` text
┌─id─┬─start─┬─end─┐
│ a  │   1.1 │ 3.2 │
│ a  │     4 │   5 │
└────┴───────┴─────┘
```

In this example, the arguments of the `Float32` type are used. In this case, the function returns a value of the `Float64` type:

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM fl_interval GROUP BY id ORDER BY id;
```

Result:

``` text
┌─id─┬─segmentLengthSum(start, end)─┬─toTypeName(segmentLengthSum(start, end))─┐
│ a  │                          3.1 │ Float64                                  │
└────┴──────────────────────────────┴──────────────────────────────────────────┘
```

4. Input table:

``` text
┌─id─┬───────────────start─┬─────────────────end─┐
│ a  │ 2020-01-01 01:12:30 │ 2020-01-01 02:50:31 │
│ a  │ 2020-01-01 03:11:22 │ 2020-01-01 03:23:31 │
└────┴─────────────────────┴─────────────────────┘
```

In this example, the arguments of the `DateTime` type are used. In this case, the function returns a value in seconds:

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM dt_interval GROUP BY id ORDER BY id;
```

Result:

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                          6610 │ UInt64                                    │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```

5. Input table:

``` text
┌─id─┬──────start─┬────────end─┐
│ a  │ 2020-01-01 │ 2020-01-04 │
│ a  │ 2020-01-12 │ 2020-01-18 │
└────┴────────────┴────────────┘
```

In this example, the arguments of the `Date` type are used. In this case, the function returns a value in days:

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM date_interval GROUP BY id ORDER BY id;
```

Result:

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                             9 │ UInt64                                    │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```
