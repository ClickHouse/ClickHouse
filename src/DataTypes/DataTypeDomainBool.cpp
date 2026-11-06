#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationBool.h>

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

    factory.registerAlias("bool", "Bool", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("boolean", "Bool", DataTypeFactory::Case::Insensitive);
}

}
