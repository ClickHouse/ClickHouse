#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationDate32.h>
#include <Columns/ColumnsNumber.h>
#include <Common/DateLUT.h>
#include <Common/typeid_cast.h>

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

/// The default is 1900-01-01, not the column zero.
void DataTypeDate32::insertDefaultInto(IColumn & column) const
{
    if (auto * column_int32 = typeid_cast<ColumnInt32 *>(&column))
        column_int32->getData().push_back(-static_cast<Int32>(getDayNumOffsetEpoch()));
    else
        column.insert(getDefault());
}

void DataTypeDate32::insertManyDefaultsInto(IColumn & column, size_t n) const
{
    if (auto * column_int32 = typeid_cast<ColumnInt32 *>(&column))
    {
        auto & data = column_int32->getData();
        data.resize_fill(data.size() + n, -static_cast<Int32>(getDayNumOffsetEpoch()));
    }
    else
        IDataType::insertManyDefaultsInto(column, n);
}

void registerDataTypeDate32(DataTypeFactory & factory)
{
    factory.registerSimpleDataType(
        "Date32", [] { return DataTypePtr(std::make_shared<DataTypeDate32>()); }, DataTypeFactory::Case::Insensitive,
        Documentation{
            .description = R"DOCS_MD(
A date. Supports the date range from `0000-01-01` to `9999-12-31`. Stored as a signed 32-bit integer in native byte order with the value representing the days since the Unix epoch: 0 represents `1970-01-01`, and negative values represent the days before `1970-01-01`.

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

- [toDate32](/reference/functions/regular-functions/type-conversion-functions#toDate32)
- [toDate32OrZero](/reference/functions/regular-functions/type-conversion-functions#toDate32OrZero)
- [toDate32OrNull](/reference/functions/regular-functions/type-conversion-functions#toDate32OrNull)
)DOCS_MD",
            .syntax = "Date32",
            .related = {"Date", "DateTime"},
        });
}

}
