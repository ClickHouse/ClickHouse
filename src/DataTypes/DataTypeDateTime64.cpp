#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/Serializations/SerializationDateTime64.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <optional>
#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

static constexpr UInt32 max_scale = 9;

DataTypeDateTime64::DataTypeDateTime64(UInt32 scale_, const std::string & time_zone_name)
    : DataTypeDecimalBase<DateTime64>(DecimalUtils::max_precision<DateTime64>, scale_),
      TimezoneMixin(time_zone_name)
{
    if (scale > max_scale)
        throw Exception("Scale " + std::to_string(scale) + " is too large for DateTime64. Maximum is up to nanoseconds (9).",
            ErrorCodes::ARGUMENT_OUT_OF_BOUND);
}

DataTypeDateTime64::DataTypeDateTime64(UInt32 scale_, const TimezoneMixin & time_zone_info)
    : DataTypeDecimalBase<DateTime64>(DecimalUtils::max_precision<DateTime64>, scale_),
      TimezoneMixin(time_zone_info)
{
    if (scale > max_scale)
        throw Exception("Scale " + std::to_string(scale) + " is too large for DateTime64. Maximum is up to nanoseconds (9).",
            ErrorCodes::ARGUMENT_OUT_OF_BOUND);
}

std::string DataTypeDateTime64::doGetName() const
{
    if (!has_explicit_time_zone)
        return std::string(getFamilyName()) + "(" + std::to_string(this->scale) + ")";

    WriteBufferFromOwnString out;
    out << "DateTime64(" << this->scale << ", " << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

bool DataTypeDateTime64::equals(const IDataType & rhs) const
{
    if (const auto * ptype = typeid_cast<const DataTypeDateTime64 *>(&rhs))
        return this->scale == ptype->getScale();
    return false;
}

SerializationPtr DataTypeDateTime64::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationDateTime64>(scale, *this);
}

}
