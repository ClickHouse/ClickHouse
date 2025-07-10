#include <DataTypes/DataTypeTime.h>
#include <DataTypes/Serializations/SerializationDateTime.h>

#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

DataTypeTime::DataTypeTime(const String & time_zone_name)
    : TimezoneMixin(time_zone_name)
{
}

DataTypeTime::DataTypeTime(const TimezoneMixin & time_zone_)
    : TimezoneMixin(time_zone_)
{
}

String DataTypeTime::doGetName() const
{
    if (!has_explicit_time_zone)
        return "Time";

    WriteBufferFromOwnString out;
    out << "Time(" << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

void DataTypeTime::updateHashImpl(SipHash & hash) const
{
    hash.update(has_explicit_time_zone);
    if (has_explicit_time_zone)
        hash.update(time_zone.getTimeZone());
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
