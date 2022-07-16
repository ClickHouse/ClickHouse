#include <DataTypes/Serializations/SerializationBase58.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustom.h>

namespace DB
{

void registerDataTypeDomainBase58(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom("Base58", []
         {
             auto type = DataTypeFactory::instance().get("String");
             return std::make_pair(type, std::make_unique<DataTypeCustomDesc>(
                                             std::make_unique<DataTypeCustomFixedName>("Base58"), std::make_unique<SerializationBase58>(type->getDefaultSerialization())));
         });

}

}
