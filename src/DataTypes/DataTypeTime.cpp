#include <DataTypes/DataTypeTime.h>
#include <Common/Exception.h>
#include <DataTypes/Serializations/SerializationDateTime.h>

#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

String DataTypeTime::doGetName() const
{
    return "Time";
}

void DataTypeTime::updateHashImpl(SipHash & /*hash*/) const
{
    // Time type has no additional parameters to hash
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
