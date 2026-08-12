#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationIPv4andIPv6.h>


namespace DB
{

void registerDataTypeIPv4andIPv6(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("IPv4", [] { return DataTypePtr(std::make_shared<DataTypeIPv4>()); });
    factory.registerAlias("INET4", "IPv4", DataTypeFactory::Case::Insensitive);
    factory.registerSimpleDataType("IPv6", [] { return DataTypePtr(std::make_shared<DataTypeIPv6>()); });
    factory.registerAlias("INET6", "IPv6", DataTypeFactory::Case::Insensitive);
}

}
