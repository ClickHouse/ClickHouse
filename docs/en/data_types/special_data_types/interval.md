# Interval {#data-type-interval}

Represents time and date intervals. Resulting type of the `INTERVAL` operator.

!!!warning "Warning"
    You can't use the `Interval` data type for storing values in tables.

Structure:

- Unsigned integer value of interval.
- Type of interval.

Supported interval types:

- SECOND
- MINUTE
- HOUR
- DAY
- WEEK
- MONTH
- QUARTER
- YEAR

For each interval type there are the separated data type. For example, the DAY interval is expressed as the `IntervalDay` data type:

```sql
SELECT toTypeName(INTERVAL 4 DAY)
```
```text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## Usage Remarks {#data-type-interval-usage-remarks}

You can use `Interval`-type values in arithmetical operations with [DateTime](../datetime.md) and [Date](../date.md)-type values. For example, you can add 4 days to current time:

```sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```
```text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

Intervals of different type can't be combined in one `INTERVAL` operator. You can't use the expressions like `INTERVAL 4 DAY 1 HOUR`, so you need to express intervals in the minimal units. For example, `1 day and an hour` interval should be expressed as `INTERVAL 25 HOUR`.

You can't perform arithmetical operations with the `Interval`-type values, but you can add intervals of different types consequenly to some value. For example:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```
```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

The following query causes the exception:

```sql
select now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```
```text
Received exception from server (version 19.14.1):
Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: Wrong argument types for function plus: if one argument is Interval, then another must be Date or DateTime.. 
```

## See Also

- [INTERVAL](../../query_language/operators.md#operator-interval) operator
- [toInterval](../../query_language/functions/type_conversion_functions.md#function-tointerval) type convertion functions