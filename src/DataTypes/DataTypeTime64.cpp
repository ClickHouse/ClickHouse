#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/Serializations/SerializationTime64.h>
#include <Common/Exception.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

static constexpr UInt32 TIME64_MAX_SCALE = 9;

DataTypeTime64::DataTypeTime64(UInt32 scale_, const std::string & time_zone_name)
    : DataTypeDecimalBase<Time64>(DecimalUtils::max_precision<Time64>, scale_),
      TimezoneMixin("")
{
    if (scale_ > TIME64_MAX_SCALE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DataTypeTime64 scale {} is too large, maximum is {}", scale_, TIME64_MAX_SCALE);

    if (!time_zone_name.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Specifying timezone for Time64 type is not allowed");
}

DataTypeTime64::DataTypeTime64(UInt32 scale_, const TimezoneMixin & time_zone_info)
    : DataTypeDecimalBase<Time64>(DecimalUtils::max_precision<Time64>, scale_),
      TimezoneMixin("")
{
    if (scale_ > TIME64_MAX_SCALE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DataTypeTime64 scale {} is too large, maximum is {}", scale_, TIME64_MAX_SCALE);

    if (time_zone_info.hasExplicitTimeZone())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Specifying timezone for Time64 type is not allowed");
}

std::string DataTypeTime64::doGetName() const
{
    if (!has_explicit_time_zone)
        return std::string(getFamilyName()) + "(" + std::to_string(this->scale) + ")";

    WriteBufferFromOwnString out;
    out << "Time64(" << this->scale << ", " << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

bool DataTypeTime64::equals(const IDataType & rhs) const
{
    if (const auto * ptype = typeid_cast<const DataTypeTime64 *>(&rhs))
        return this->scale == ptype->getScale();
    return false;
}

SerializationPtr DataTypeTime64::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationTime64>(scale, *this);
}

std::string getTimeTimezone(const IDataType & data_type)
{
    if (const auto * type = typeid_cast<const DataTypeTime *>(&data_type))
        return type->hasExplicitTimeZone() ? type->getTimeZone().getTimeZone() : std::string();
    if (const auto * type = typeid_cast<const DataTypeTime64 *>(&data_type))
        return type->hasExplicitTimeZone() ? type->getTimeZone().getTimeZone() : std::string();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get time zone from type {}", data_type.getName());
}

}
