#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationInterval.h>
#include <Common/SipHash.h>


namespace DB
{

SerializationPtr DataTypeInterval::doGetSerialization(const SerializationInfoSettings &) const { return SerializationInterval::create(kind); }

bool DataTypeInterval::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && kind == static_cast<const DataTypeInterval &>(rhs).kind;
}

void DataTypeInterval::updateHashImpl(SipHash & hash) const
{
    hash.update(static_cast<uint8_t>(IntervalKind::Kind(kind)));
}

void registerDataTypeInterval(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("IntervalNanosecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Nanosecond)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of nanoseconds; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalNanosecond",
            .related = {"IntervalDay"},
        });
    factory.registerSimpleDataType("IntervalMicrosecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Microsecond)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of microseconds; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalMicrosecond",
            .related = {"IntervalDay"},
        });
    factory.registerSimpleDataType("IntervalMillisecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Millisecond)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of milliseconds; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalMillisecond",
            .related = {"IntervalDay"},
        });
    factory.registerSimpleDataType("IntervalSecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Second)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of seconds; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalSecond",
            .related = {"IntervalDay"},
        });
    factory.registerSimpleDataType("IntervalMinute", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Minute)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of minutes; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalMinute",
            .related = {"IntervalDay"},
        });
    factory.registerSimpleDataType("IntervalHour", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Hour)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of hours; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalHour",
            .related = {"IntervalDay"},
        });
    factory.registerSimpleDataType("IntervalDay", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Day)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
The family of data types representing time and date intervals. The resulting types of the [INTERVAL](/sql-reference/operators#interval) operator.

Structure:

- Time interval as an unsigned integer value.
- Type of an interval.

Supported interval types:

- `NANOSECOND`
- `MICROSECOND`
- `MILLISECOND`
- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `WEEK`
- `MONTH`
- `QUARTER`
- `YEAR`

For each interval type, there is a separate data type. For example, the `DAY` interval corresponds to the `IntervalDay` data type:

```sql
SELECT toTypeName(INTERVAL 4 DAY)
```

```text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## Usage Remarks {#usage-remarks}

You can use `Interval`-type values in arithmetical operations with [Date](../../../sql-reference/data-types/date.md) and [DateTime](../../../sql-reference/data-types/datetime.md)-type values. For example, you can add 4 days to the current time:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY
```

```text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

Also it is possible to use multiple intervals simultaneously:

```sql
SELECT now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

```text
┌───current_date_time─┬─plus(current_date_time, plus(toIntervalDay(4), toIntervalHour(3)))─┐
│ 2024-08-08 18:31:39 │                                                2024-08-12 21:31:39 │
└─────────────────────┴────────────────────────────────────────────────────────────────────┘
```

And to compare values with different intervals:

```sql
SELECT toIntervalMicrosecond(179999999) < toIntervalMinute(3);
```

```text
┌─less(toIntervalMicrosecond(179999999), toIntervalMinute(3))─┐
│                                                           1 │
└─────────────────────────────────────────────────────────────┘
```

## Mixed-type Intervals {#mixed-type-intervals}

Intervals of mixed type, e.g. multiple hours and multiple minutes, can be created using `INTERVAL 'value' <from_kind> TO <to_kind>` syntax.
The result is a tuple of two or more intervals.

Supported combinations:

| Syntax | String format | Example |
|---|---|---|
| `YEAR TO MONTH` | `Y-M` | `INTERVAL '2-6' YEAR TO MONTH` |
| `DAY TO HOUR` | `D H` | `INTERVAL '5 12' DAY TO HOUR` |
| `DAY TO MINUTE` | `D H:M` | `INTERVAL '5 12:30' DAY TO MINUTE` |
| `DAY TO SECOND` | `D H:M:S` | `INTERVAL '5 12:30:45' DAY TO SECOND` |
| `HOUR TO MINUTE` | `H:M` | `INTERVAL '1:30' HOUR TO MINUTE` |
| `HOUR TO SECOND` | `H:M:S` | `INTERVAL '1:30:45' HOUR TO SECOND` |
| `MINUTE TO SECOND` | `M:S` | `INTERVAL '5:30' MINUTE TO SECOND` |

Non-leading fields are validated per the SQL standard: `MONTH` 0-11, `HOUR` 0-23, `MINUTE` 0-59, `SECOND` 0-59.

```sql
SELECT INTERVAL '1:30' HOUR TO MINUTE;
```

```text
┌─(toIntervalHour(1), toIntervalMinute(30))─┐
│ (1,30)                                     │
└────────────────────────────────────────────┘
```

An optional leading `+` or `-` sign applies to all components:

```sql
SELECT INTERVAL '+1:30' HOUR TO MINUTE;
-- this is equivalent to:
-- SELECT INTERVAL '1:30' HOUR TO MINUTE;
```

```text
┌─(toIntervalHour(1), toIntervalMinute(30))─┐
│ (1,30)                                     │
└────────────────────────────────────────────┘
```

## See Also {#see-also}

- [INTERVAL](/sql-reference/operators#interval) operator
- [toInterval](/sql-reference/functions/type-conversion-functions#toIntervalYear) type conversion functions
)DOCS_MD",
            .syntax = "IntervalDay",
            .related = {"IntervalSecond", "IntervalMonth"},
        });
    factory.registerSimpleDataType("IntervalWeek", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Week)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of weeks; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalWeek",
            .related = {"IntervalDay"},
        });
    factory.registerSimpleDataType("IntervalMonth", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Month)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of months; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalMonth",
            .related = {"IntervalDay"},
        });
    factory.registerSimpleDataType("IntervalQuarter", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Quarter)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of quarters; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalQuarter",
            .related = {"IntervalDay"},
        });
    factory.registerSimpleDataType("IntervalYear", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Year)); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = "Represents an interval of years; used as the result of arithmetic on dates/times. See the `IntervalDay` entry for full documentation.",
            .syntax = "IntervalYear",
            .related = {"IntervalDay"},
        });
}

}
