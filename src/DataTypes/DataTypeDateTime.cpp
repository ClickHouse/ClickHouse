#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/Serializations/SerializationDateTime.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

DataTypeDateTime::DataTypeDateTime(std::string_view time_zone_name)
    : TimezoneMixin(time_zone_name)
{
}

DataTypeDateTime::DataTypeDateTime(const TimezoneMixin & time_zone_)
    : TimezoneMixin(time_zone_)
{
}

String DataTypeDateTime::doGetName() const
{
    if (!has_explicit_time_zone)
        return "DateTime";

    WriteBufferFromOwnString out;
    out << "DateTime(" << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

bool DataTypeDateTime::equals(const IDataType & rhs) const
{
    /// DateTime with different timezones are equal, because:
    /// "all types with different time zones are equivalent and may be used interchangingly."
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeDateTime::doGetSerialization(const SerializationInfoSettings &) const
{
    if (!has_explicit_time_zone)
    {
        /// When no explicit timezone, resolve the effective timezone (respects session_timezone).
        /// This is called once per formatter (not per row), so the cost of DateLUT::instance() is negligible.
        const auto & effective_tz = DateLUT::instance();
        if (&effective_tz != &time_zone)
        {
            TimezoneMixin overridden(effective_tz.getTimeZone());
            return SerializationDateTime::create(overridden);
        }
    }
    return SerializationDateTime::create(*this);
}

}
