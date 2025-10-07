#include <string>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/Serializations/SerializationTime64.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}

static constexpr UInt32 TIME64_MAX_SCALE = 9;

DataTypeTime64::DataTypeTime64(UInt32 scale_)
    : DataTypeDecimalBase<Time64>(DecimalUtils::max_precision<Time64>, scale_)
{
    if (scale_ > TIME64_MAX_SCALE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DataTypeTime64 scale {} is too large, maximum is {}", scale_, TIME64_MAX_SCALE);
}


std::string DataTypeTime64::doGetName() const
{
    return std::string(getFamilyName()) + "(" + std::to_string(this->scale) + ")";
}

void DataTypeTime64::updateHashImpl(SipHash & hash) const
{
    hash.update(this->scale);
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

const DateLUTImpl & DataTypeTime64::getTimeZone() const
{
    return DateLUT::instance();
}

std::string getTimeTimezone(const IDataType & data_type)
{
    if (typeid_cast<const DataTypeTime *>(&data_type) || typeid_cast<const DataTypeTime64 *>(&data_type))
        return std::string();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get time zone from type {}", data_type.getName());
}

}
