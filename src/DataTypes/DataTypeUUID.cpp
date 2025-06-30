#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationUUID.h>
#include <Common/SipHash.h>


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

Field DataTypeUUID::getDefault() const
{
    return UUID{};
}

MutableColumnPtr DataTypeUUID::createColumn() const
{
    return ColumnVector<UUID>::create();
}

void registerDataTypeUUID(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UUID", [] { return DataTypePtr(std::make_shared<DataTypeUUID>()); });
}

}
