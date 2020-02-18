# DateTime64 {#data_type-datetime64}

Stores a moment in time, including date, time and timezone, with defined sub-second precision

Tick size (precision): 10<sup>-precision</sup> seconds

Syntax:
```sql
DateTime64(precision, [timezone])
```

Internally, stores data as number of 'ticks' since epoch start (1970/1/1) as UInt64

## Examples

**1.** Parsing

```sql
select toDateTime64('2020-01-01 11:22:33.123456', 3)
```
```text
2020-01-01 11:22:33.123
```

**2.** Creating a table with `DateTime64`-type column and inserting data into it:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Europe/Moscow'), 
    `event_id` UInt8
)
ENGINE = TinyLog
```
```sql
INSERT INTO dt Values (1546300800000, 1), ('2019-01-01 00:00:00', 2)
```
```sql
SELECT * FROM dt
```
```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

Unix timestamp `1546300800000` (in milliseconds, as precision=3) represents the `'2019-01-01 00:00:00'` date and time in `Europe/London` (UTC+0) time zone, but the `timestamp` column stores values in the `Europe/Moscow` (UTC+3) timezone, so the value inserted as Unix timestamp is formatted as `2019-01-01 03:00:00`.

```sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Europe/Moscow')
```
```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

**3.** Getting a time zone for a `DateTime64`-type value:

```sql
SELECT toDateTime64(now(), 3, 'Europe/Moscow') AS column, toTypeName(column) AS x
```
```text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Europe/Moscow') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** Timezone conversion 

```sql
SELECT 
toDateTime64(timestamp, 3, 'Europe/London') as lon_time, 
toDateTime64(timestamp, 3, 'Europe/Moscow') as mos_time
FROM dt
```
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘

## See Also

- [Type conversion functions](../query_language/functions/type_conversion_functions.md)
- [Functions for working with dates and times](../query_language/functions/date_time_functions.md)
- [Functions for working with arrays](../query_language/functions/array_functions.md)
- [The `date_time_input_format` setting](../operations/settings/settings.md#settings-date_time_input_format)
- [The `timezone` server configuration parameter](../operations/server_settings/settings.md#server_settings-timezone)
- [Operators for working with dates and times](../query_language/operators.md#operators-datetime)
- [`Date` data type](date.md)
- [`DateTime` data type](datetime.md)
