#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/Serializations/SerializationTime64.h>
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

DataTypeTime64::DataTypeTime64(UInt32 scale_, const std::string & time_zone_name)
    : DataTypeDecimalBase<Time64>(DecimalUtils::max_precision<Time64>, scale_),
      TimezoneMixin(time_zone_name)
{
    if (scale > max_scale)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale {} is too large for Time64. "
            "Maximum is up to nanoseconds (9).", std::to_string(scale));
}

DataTypeTime64::DataTypeTime64(UInt32 scale_, const TimezoneMixin & time_zone_info)
    : DataTypeDecimalBase<Time64>(DecimalUtils::max_precision<Time64>, scale_),
      TimezoneMixin(time_zone_info)
{
    if (scale > max_scale)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale {} is too large for Time64. "
            "Maximum is up to nanoseconds (9).", std::to_string(scale));
}

void DataTypeTime64::updateHashImpl(SipHash & hash) const
{
    hash.update(has_explicit_time_zone);
    if (has_explicit_time_zone)
        hash.update(time_zone.getTimeZone());
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
