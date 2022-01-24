#include <DataTypes/Serializations/SerializationBool.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustom.h>

namespace DB
{

void registerDataTypeDomainBool(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom("Bool", []
    {
        auto type = DataTypeFactory::instance().get("UInt8");
        return std::make_pair(type, std::make_unique<DataTypeCustomDesc>(
                std::make_unique<DataTypeCustomFixedName>("Bool"), std::make_unique<SerializationBool>(type->getDefaultSerialization())));
    });

    factory.registerAlias("bool", "Bool", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("boolean", "Bool", DataTypeFactory::CaseInsensitive);
}

}
