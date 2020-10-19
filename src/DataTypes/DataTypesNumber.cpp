#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>


#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename T>
static DataTypePtr createNumericDataType(const ASTPtr & arguments)
{
    if (arguments)
    {
        if (std::is_integral_v<T>)
        {
            if (arguments->children.size() > 1)
                throw Exception(String(TypeName<T>::get()) + " data type family must not have more than one argument - display width", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        else
        {
            if (arguments->children.size() > 2)
                throw Exception(String(TypeName<T>::get()) + " data type family must not have more than two arguments - total number of digits and number of digits following the decimal point", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
    }
    return std::make_shared<DataTypeNumber<T>>();
}


void registerDataTypeNumbers(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UInt8", [] { return DataTypePtr(std::make_shared<DataTypeUInt8>()); });
    factory.registerSimpleDataType("UInt16", [] { return DataTypePtr(std::make_shared<DataTypeUInt16>()); });
    factory.registerSimpleDataType("UInt32", [] { return DataTypePtr(std::make_shared<DataTypeUInt32>()); });
    factory.registerSimpleDataType("UInt64", [] { return DataTypePtr(std::make_shared<DataTypeUInt64>()); });

    factory.registerDataType("Int8", createNumericDataType<Int8>);
    factory.registerDataType("Int16", createNumericDataType<Int16>);
    factory.registerDataType("Int32", createNumericDataType<Int32>);
    factory.registerDataType("Int64", createNumericDataType<Int64>);
    factory.registerDataType("Float32", createNumericDataType<Float32>);
    factory.registerDataType("Float64", createNumericDataType<Float64>);

    /// These synonims are added for compatibility.

    factory.registerAlias("TINYINT", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BOOL", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BOOLEAN", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT1", "Int8", DataTypeFactory::CaseInsensitive);    /// MySQL
    factory.registerAlias("BYTE", "Int8", DataTypeFactory::CaseInsensitive);    /// MS Access
    factory.registerAlias("SMALLINT", "Int16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INTEGER", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIGINT", "Int64", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("FLOAT", "Float32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("REAL", "Float32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("SINGLE", "Float32", DataTypeFactory::CaseInsensitive);   /// MS Access
    factory.registerAlias("DOUBLE", "Float64", DataTypeFactory::CaseInsensitive);

    factory.registerAlias("DOUBLE PRECISION", "Float64", DataTypeFactory::CaseInsensitive);

    /// MySQL
    factory.registerAlias("TINYINT SIGNED", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT1 SIGNED", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("SMALLINT SIGNED", "Int16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT SIGNED", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INTEGER SIGNED", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIGINT SIGNED", "Int64", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("TINYINT UNSIGNED", "UInt8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT1 UNSIGNED", "UInt8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("SMALLINT UNSIGNED", "UInt16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT UNSIGNED", "UInt32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INTEGER UNSIGNED", "UInt32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIGINT UNSIGNED", "UInt64", DataTypeFactory::CaseInsensitive);

}

}
