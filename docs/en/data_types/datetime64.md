# DateTime64 {#data_type-datetime64}

Stores a moment in time, like DateTime, but with 2 differences:
* Data is stored in 'ticks', not seconds
* Data is stored in `Int64` instead of `Int32` in DateTime

Syntax:

```sql
DateTime64(precision, [timezone])
```

Tick size (precision) is defined as type parameter:

```sql
select toDateTime64('2020-01-01 11:22:33.123456', 3)
```
Result:
`2020-01-01 11:22:33.123` - '3' means millisecond precision


## Examples

**1.** Creating a table with a `DateTim64e`-type column and inserting data into it:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3,'Europe/Moscow'), 
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

Unix timestamp `1546300800000` represents the `'2019-01-01 00:00:00'` date and time in `Europe/London` (UTC+0) time zone, but the `timestamp` column stores values in the `Europe/Moscow` (UTC+3) timezone, so the value inserted as Unix timestamp is formatted as `2019-01-01 03:00:00`.


## See Also

- [Type conversion functions](../query_language/functions/type_conversion_functions.md)
- [Functions for working with dates and times](../query_language/functions/date_time_functions.md)
- [Functions for working with arrays](../query_language/functions/array_functions.md)
- [The `date_time_input_format` setting](../operations/settings/settings.md#settings-date_time_input_format)
- [The `timezone` server configuration parameter](../operations/server_settings/settings.md#server_settings-timezone)
- [Operators for working with dates and times](../query_language/operators.md#operators-datetime)
- [`Date` data type](date.md)
- [`DateTime` data type](datetime.md)

