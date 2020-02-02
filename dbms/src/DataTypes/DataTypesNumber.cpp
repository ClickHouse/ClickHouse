#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

template <typename NumberType>
static DataTypePtr create(const String & type_name)
{
    return DataTypePtr(std::make_shared<NumberType>(type_name));
}

void registerDataTypeNumbers(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UInt8", create<DataTypeUInt8>);
    factory.registerSimpleDataType("UInt16", create<DataTypeUInt16>);
    factory.registerSimpleDataType("UInt32", create<DataTypeUInt32>);
    factory.registerSimpleDataType("UInt64", create<DataTypeUInt64>);

    factory.registerSimpleDataType("Int8", create<DataTypeInt8>);
    factory.registerSimpleDataType("Int16", create<DataTypeInt16>);
    factory.registerSimpleDataType("Int32", create<DataTypeInt32>);
    factory.registerSimpleDataType("Int64", create<DataTypeInt64>);

    factory.registerSimpleDataType("Float32", create<DataTypeFloat32>);
    factory.registerSimpleDataType("Float64", create<DataTypeFloat64>);

    /// These synonyms are added for compatibility.

    factory.registerAlias("TINYINT", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("SMALLINT", "Int16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INTEGER", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIGINT", "Int64", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("FLOAT", "Float32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("DOUBLE", "Float64", DataTypeFactory::CaseInsensitive);
}

}
