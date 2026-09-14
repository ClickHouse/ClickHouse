#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationDate32.h>
#include <Common/DateLUT.h>

namespace DB
{
bool DataTypeDate32::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeDate32::doGetSerialization(const SerializationInfoSettings &) const
{
    return SerializationDate32::create();
}

Field DataTypeDate32::getDefault() const
{
    return -static_cast<Int64>(getDayNumOffsetEpoch());
}

void registerDataTypeDate32(DataTypeFactory & factory)
{
    factory.registerSimpleDataType(
        "Date32", [] { return DataTypePtr(std::make_shared<DataTypeDate32>()); }, DataTypeFactory::Case::Insensitive,
        Documentation{
            .description = R"DOCS_MD(
A date. Supports the date range same with [DateTime64](../../sql-reference/data-types/datetime64.md). Stored as a signed 32-bit integer in native byte order with the value representing the days since `1900-01-01`. **Important!** 0 represents `1970-01-01`, and negative values represent the days before `1970-01-01`.

**Examples**

Creating a table with a `Date32`-type column and inserting data into it:

```sql
CREATE TABLE dt32
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt32 VALUES ('2100-01-01', 1), (47482, 2), (4102444800, 3);

SELECT * FROM dt32;
```

```text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
│ 2100-01-01 │        3 │
└────────────┴──────────┘
```

**See Also**

- [toDate32](../../sql-reference/functions/type-conversion-functions.md#toDate32)
- [toDate32OrZero](/sql-reference/functions/type-conversion-functions#toDate32OrZero)
- [toDate32OrNull](/sql-reference/functions/type-conversion-functions#toDate32OrNull)
)DOCS_MD",
            .syntax = "Date32",
            .related = {"Date", "DateTime"},
        });
}

}
