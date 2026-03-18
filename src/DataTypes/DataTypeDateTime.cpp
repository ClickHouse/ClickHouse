#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/Serializations/SerializationDateTime.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

DataTypeDateTime::DataTypeDateTime(const String & time_zone_name)
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

SerializationPtr DataTypeDateTime::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationDateTime>(*this);
}

}
