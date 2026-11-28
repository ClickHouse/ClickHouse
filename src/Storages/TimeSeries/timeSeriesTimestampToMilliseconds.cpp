#include <Storages/TimeSeries/timeSeriesTimestampToMilliseconds.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesDecimal.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{
    /// The scale of a timestamp in milliseconds as a `DateTime64`.
    constexpr UInt32 MILLISECONDS_SCALE = 3;
}

Int64 timeSeriesTimestampToMilliseconds(Int64 timestamp, UInt32 scale)
{
    /// A timestamp stored bypassing the sink can be too far from the epoch to be expressed in milliseconds.
    DateTime64 timestamp_ms{};
    if (!tryConvertDecimals<DataTypeDateTime64, DataTypeDateTime64>(DateTime64{timestamp}, scale, MILLISECONDS_SCALE, timestamp_ms))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Stored timestamp {} with scale {} can't be expressed in milliseconds", timestamp, scale);
    return timestamp_ms.value;
}

void insertMillisecondsAsTimeSeriesTimestamp(Int64 timestamp_ms, IColumn & column)
{
    /// A sender can put any `Int64` into the timestamp. It's rejected with `INCORRECT_DATA`, an HTTP 400 on which the sender drops
    /// the request, not with the `DECIMAL_OVERFLOW` of the throwing conversions, an HTTP 500 on which it retries the request.
    if (auto * const decimal_column = typeid_cast<ColumnDecimal<DateTime64> *>(&column))
    {
        const UInt32 scale = decimal_column->getScale();
        DateTime64 timestamp{};
        if (!tryConvertDecimals<DataTypeDateTime64, DataTypeDateTime64>(DateTime64{timestamp_ms}, MILLISECONDS_SCALE, scale, timestamp))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Timestamp {} ms is out of the range of DateTime64({})", timestamp_ms, scale);
        decimal_column->getData().push_back(timestamp);
        return;
    }

    UInt32 timestamp_seconds = 0;
    if (!DecimalUtils::tryConvertTo<UInt32>(DateTime64{timestamp_ms}, MILLISECONDS_SCALE, timestamp_seconds))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Timestamp {} ms is out of the range of DateTime", timestamp_ms);
    assert_cast<ColumnUInt32 &>(column).getData().push_back(timestamp_seconds);
}

}
