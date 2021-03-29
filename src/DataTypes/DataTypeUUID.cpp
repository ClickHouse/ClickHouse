#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationUUID.h>


namespace DB
{

bool DataTypeUUID::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeUUID::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationUUID>();
}

void registerDataTypeUUID(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UUID", [] { return DataTypePtr(std::make_shared<DataTypeUUID>()); });
}

}
