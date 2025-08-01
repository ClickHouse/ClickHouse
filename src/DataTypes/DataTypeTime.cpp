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

String DataTypeTime::doGetName() const
{
    return "Time";
}

bool DataTypeTime::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeTime::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationTime>(*this);
}

const DateLUTImpl & DataTypeTime::getTimeZone() const
{
    return DateLUT::instance();
}

}
