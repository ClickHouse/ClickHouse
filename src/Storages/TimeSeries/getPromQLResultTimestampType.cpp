#include <Storages/TimeSeries/getPromQLResultTimestampType.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesDecimal.h>

#include <algorithm>


namespace DB
{

namespace
{
    /// Prometheus uses millisecond timestamps, so the results of PromQL evaluation never use a scale less than 3.
    constexpr UInt32 MIN_RESULT_TIMESTAMP_SCALE = 3;

    /// Returns the time zone specified explicitly in a `DateTime` or `DateTime64` type,
    /// or an empty string if the type has no explicit time zone (for example, if it is UInt32).
    String getExplicitTimeZone(const DataTypePtr & type)
    {
        /// getDateTimeTimezone() throws for types other than DateTime and DateTime64.
        if (!WhichDataType{type}.isDateTimeOrDateTime64())
            return {};
        return getDateTimeTimezone(*type);
    }
}


UInt32 getPromQLResultTimestampScale(const DataTypePtr & table_timestamp_type)
{
    UInt32 table_timestamp_scale = tryGetDecimalScale(*table_timestamp_type).value_or(0);
    return std::max(table_timestamp_scale, MIN_RESULT_TIMESTAMP_SCALE);
}


String getPromQLResultTimeZone(const DataTypePtr & table_timestamp_type, const DataTypes & parameter_types)
{
    String parameters_time_zone;
    for (const auto & parameter_type : parameter_types)
    {
        const String time_zone = getExplicitTimeZone(removeLowCardinalityAndNullable(parameter_type));
        if (time_zone.empty())
            continue;
        if (parameters_time_zone.empty())
            parameters_time_zone = time_zone;
        else if (parameters_time_zone != time_zone)
            return getExplicitTimeZone(table_timestamp_type);  /// The parameters specify different time zones.
    }
    if (!parameters_time_zone.empty())
        return parameters_time_zone;
    return getExplicitTimeZone(table_timestamp_type);
}


DataTypePtr getPromQLResultTimestampType(UInt32 time_scale, const String & time_zone)
{
    return std::make_shared<DataTypeDateTime64>(time_scale, time_zone);
}

}
