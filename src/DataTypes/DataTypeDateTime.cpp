#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/Serializations/SerializationDateTime.h>

#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <common/DateLUT.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

TimezoneMixin::TimezoneMixin(const String & time_zone_name)
    : has_explicit_time_zone(!time_zone_name.empty()),
    time_zone(DateLUT::instance(time_zone_name)),
    utc_time_zone(DateLUT::instance("UTC"))
{
}

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
    return std::make_shared<SerializationDateTime>(time_zone, utc_time_zone);
}

}
