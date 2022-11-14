#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationIPv4andIPv6.h>


namespace DB
{

bool DataTypeIPv4::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeIPv4::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationIPv4>();
}

Field DataTypeIPv4::getDefault() const
{
    return IPv4{};
}

MutableColumnPtr DataTypeIPv4::createColumn() const
{
    return ColumnVector<IPv4>::create();
}

bool DataTypeIPv6::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeIPv6::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationIPv6>();
}

Field DataTypeIPv6::getDefault() const
{
    return IPv6{};
}

MutableColumnPtr DataTypeIPv6::createColumn() const
{
    return ColumnVector<IPv6>::create();
}

void registerDataTypeIPv4andIPv6(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("IPv4", [] { return DataTypePtr(std::make_shared<DataTypeIPv4>()); });
    factory.registerAlias("INET4", "IPv4", DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("IPv6", [] { return DataTypePtr(std::make_shared<DataTypeIPv6>()); });
    factory.registerAlias("INET6", "IPv6", DataTypeFactory::CaseInsensitive);
}

}
