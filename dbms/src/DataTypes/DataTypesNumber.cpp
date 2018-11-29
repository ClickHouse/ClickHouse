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

    factory.registerAlias("TINYINT", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("SMALLINT", "Int16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INTEGER", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIGINT", "Int64", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("FLOAT", "Float32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("DOUBLE", "Float64", DataTypeFactory::CaseInsensitive);
}

}
