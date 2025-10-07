#include <Common/DateLUT.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/Serializations/SerializationDateTime64.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

static constexpr UInt32 max_scale = 9;

DataTypeDateTime64::DataTypeDateTime64(UInt32 scale_, const std::string & time_zone_name)
    : DataTypeDecimalBase<DateTime64>(DecimalUtils::max_precision<DateTime64>, scale_),
      TimezoneMixin(time_zone_name)
{
    if (scale > max_scale)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale {} is too large for DateTime64. "
            "Maximum is up to nanoseconds (9).", std::to_string(scale));
}

DataTypeDateTime64::DataTypeDateTime64(UInt32 scale_, const TimezoneMixin & time_zone_info)
    : DataTypeDecimalBase<DateTime64>(DecimalUtils::max_precision<DateTime64>, scale_),
      TimezoneMixin(time_zone_info)
{
    if (scale > max_scale)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale {} is too large for DateTime64. "
            "Maximum is up to nanoseconds (9).", std::to_string(scale));
}

std::string DataTypeDateTime64::doGetName() const
{
    if (!has_explicit_time_zone)
        return std::string(getFamilyName()) + "(" + std::to_string(this->scale) + ")";

    WriteBufferFromOwnString out;
    out << "DateTime64(" << this->scale << ", " << quote << getDateLUTTimeZone(time_zone) << ")";
    return out.str();
}

void DataTypeDateTime64::updateHashImpl(SipHash & hash) const
{
    Base::updateHashImpl(hash);
    if (has_explicit_time_zone)
        hash.update(getDateLUTTimeZone(time_zone));
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

std::string getDateTimeTimezone(const IDataType & data_type)
{
    if (const auto * type = typeid_cast<const DataTypeDateTime *>(&data_type))
        return type->hasExplicitTimeZone() ? getDateLUTTimeZone(type->getTimeZone()) : std::string();
    if (const auto * type = typeid_cast<const DataTypeDateTime64 *>(&data_type))
        return type->hasExplicitTimeZone() ? getDateLUTTimeZone(type->getTimeZone()) : std::string();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get time zone from type {}", data_type.getName());
}

}
