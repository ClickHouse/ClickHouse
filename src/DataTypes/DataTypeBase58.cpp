#include <DataTypes/DataTypeBase58.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationBase58.h>


namespace DB
{

bool DataTypeBase58::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeBase58::doGetDefaultSerialization() const
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
