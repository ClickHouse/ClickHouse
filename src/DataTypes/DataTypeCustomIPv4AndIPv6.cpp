#include <DataTypes/Serializations/SerializationIP.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustom.h>

namespace DB
{

void registerDataTypeDomainIPv4AndIPv6(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom("IPv4", []
    {
        auto type = DataTypeFactory::instance().get("UInt32");
        return std::make_pair(type, std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomFixedName>("IPv4"), std::make_unique<SerializationIPv4>(type->getDefaultSerialization())));
    });

    factory.registerSimpleDataTypeCustom("IPv6", []
    {
        auto type = DataTypeFactory::instance().get("FixedString(16)");
        return std::make_pair(type, std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomFixedName>("IPv6"), std::make_unique<SerializationIPv6>(type->getDefaultSerialization())));
    });

    /// MySQL, MariaDB
    factory.registerAlias("INET4", "IPv4", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INET6", "IPv6", DataTypeFactory::CaseInsensitive);
}

}
