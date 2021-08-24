#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationDate32.h>

namespace DB
{
bool DataTypeDate32::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeDate32::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationDate32>();
}

void registerDataTypeDate32(DataTypeFactory & factory)
{
    factory.registerSimpleDataType(
        "Date32", [] { return DataTypePtr(std::make_shared<DataTypeDate32>()); }, DataTypeFactory::CaseInsensitive);
}

}
