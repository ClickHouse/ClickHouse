#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

void registerDataTypeNumbers(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UInt8", [] { return DataTypePtr(std::make_shared<DataTypeUInt8>()); });
    factory.registerSimpleDataType("UInt16", [] { return DataTypePtr(std::make_shared<DataTypeUInt16>()); });
    factory.registerSimpleDataType("UInt32", [] { return DataTypePtr(std::make_shared<DataTypeUInt32>()); });
    factory.registerSimpleDataType("UInt64", [] { return DataTypePtr(std::make_shared<DataTypeUInt64>()); });

    factory.registerSimpleDataType("Int8", [] { return DataTypePtr(std::make_shared<DataTypeInt8>()); });
    factory.registerSimpleDataType("Int16", [] { return DataTypePtr(std::make_shared<DataTypeInt16>()); });
    factory.registerSimpleDataType("Int32", [] { return DataTypePtr(std::make_shared<DataTypeInt32>()); });
    factory.registerSimpleDataType("Int64", [] { return DataTypePtr(std::make_shared<DataTypeInt64>()); });

    factory.registerSimpleDataType("Float32", [] { return DataTypePtr(std::make_shared<DataTypeFloat32>()); });
    factory.registerSimpleDataType("Float64", [] { return DataTypePtr(std::make_shared<DataTypeFloat64>()); });

    /// These synonims are added for compatibility.

    factory.registerSimpleDataType("TINYINT", [] { return DataTypePtr(std::make_shared<DataTypeInt8>()); }, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("SMALLINT", [] { return DataTypePtr(std::make_shared<DataTypeInt16>()); }, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("INT", [] { return DataTypePtr(std::make_shared<DataTypeInt32>()); }, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("INTEGER", [] { return DataTypePtr(std::make_shared<DataTypeInt32>()); }, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("BIGINT", [] { return DataTypePtr(std::make_shared<DataTypeInt64>()); }, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("FLOAT", [] { return DataTypePtr(std::make_shared<DataTypeFloat32>()); }, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("DOUBLE", [] { return DataTypePtr(std::make_shared<DataTypeFloat64>()); }, DataTypeFactory::CaseInsensitive);
}

}
