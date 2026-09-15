#include <Storages/TimeSeries/getPromQLResultTypes.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace
{
    /// Prometheus uses millisecond timestamps, so the results of PromQL evaluation never use a scale less than 3.
    constexpr UInt32 MIN_RESULT_TIMESTAMP_SCALE = 3;
}


UInt32 getPromQLResultTimestampScale(const DataTypePtr & table_timestamp_type)
{
    UInt32 table_timestamp_scale = tryGetDecimalScale(*table_timestamp_type).value_or(0);
    return std::max(table_timestamp_scale, MIN_RESULT_TIMESTAMP_SCALE);
}


DataTypePtr getPromQLResultTimestampType(const DataTypePtr & table_timestamp_type)
{
    /// UInt32 timestamps have no time zone, and getDateTimeTimezone() would throw for them.
    String time_zone;
    if (WhichDataType{table_timestamp_type}.isDateTimeOrDateTime64())
        time_zone = getDateTimeTimezone(*table_timestamp_type);

    return std::make_shared<DataTypeDateTime64>(getPromQLResultTimestampScale(table_timestamp_type), time_zone);
}


DataTypePtr getPromQLResultValueType()
{
    return std::make_shared<DataTypeFloat64>();
}

}
