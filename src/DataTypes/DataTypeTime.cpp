#include <DataTypes/DataTypeTime.h>
#include <Common/Exception.h>
#include <DataTypes/Serializations/SerializationDateTime.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

DataTypeTime::DataTypeTime(const String & time_zone_name)
{
    if (!time_zone_name.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Specifying timezone for Time type is not allowed");
}

DataTypeTime::DataTypeTime(const TimezoneMixin & time_zone_)
{
    if (time_zone_.hasExplicitTimeZone())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Specifying timezone for Time type is not allowed");
}

String DataTypeTime::doGetName() const
{
    if (!has_explicit_time_zone)
        return "Time";

    WriteBufferFromOwnString out;
    out << "Time(" << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

bool DataTypeTime::equals(const IDataType & rhs) const
{
    /// Time with different timezones are equal, because:
    /// "all types with different time zones are equivalent and may be used interchangingly."
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeTime::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationTime>(*this);
}

}
