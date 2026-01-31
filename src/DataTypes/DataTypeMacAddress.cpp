#include <DataTypes/DataTypeMacAddress.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationMacAddress.h>


namespace DB
{

bool DataTypeMacAddress::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeMacAddress::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationMacAddress>();
}

Field DataTypeMacAddress::getDefault() const
{
    return MacAddress{};
}

MutableColumnPtr DataTypeMacAddress::createColumn() const
{
    return ColumnVector<MacAddress>::create();
}

void registerDataTypeMacAddress(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("MacAddress", [] { return DataTypePtr(std::make_shared<DataTypeMacAddress>()); });
}

}

