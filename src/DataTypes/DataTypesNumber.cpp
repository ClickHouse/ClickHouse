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
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "{} data type family must not have more than one argument - display width", TypeName<T>);
        }
        else
        {
            if (arguments->children.size() > 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "{} data type family must not have more than two arguments - total number of digits and number of digits following the decimal point", TypeName<T>);
        }
    }
    return std::make_shared<DataTypeNumber<T>>();
}


void registerDataTypeNumbers(DataTypeFactory & factory)
{
    factory.registerDataType("UInt8", createNumericDataType<UInt8>);
    factory.registerDataType("UInt16", createNumericDataType<UInt16>);
    factory.registerDataType("UInt32", createNumericDataType<UInt32>);
    factory.registerDataType("UInt64", createNumericDataType<UInt64>);

    factory.registerDataType("Int8", createNumericDataType<Int8>);
    factory.registerDataType("Int16", createNumericDataType<Int16>);
    factory.registerDataType("Int32", createNumericDataType<Int32>);
    factory.registerDataType("Int64", createNumericDataType<Int64>);

    factory.registerDataType("Float32", createNumericDataType<Float32>);
    factory.registerDataType("Float64", createNumericDataType<Float64>);

    factory.registerSimpleDataType("UInt128", [] { return DataTypePtr(std::make_shared<DataTypeUInt128>()); });
    factory.registerSimpleDataType("UInt256", [] { return DataTypePtr(std::make_shared<DataTypeUInt256>()); });

    factory.registerSimpleDataType("Int128", [] { return DataTypePtr(std::make_shared<DataTypeInt128>()); });
    factory.registerSimpleDataType("Int256", [] { return DataTypePtr(std::make_shared<DataTypeInt256>()); });

    /// These synonyms are added for compatibility.

    factory.registerAlias("TINYINT", "Int8", DataTypeFactory::CaseInsensitive);
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
    factory.registerAlias("MEDIUMINT", "Int32", DataTypeFactory::CaseInsensitive);    /// MySQL

    factory.registerAlias("DOUBLE PRECISION", "Float64", DataTypeFactory::CaseInsensitive);

    /// MySQL
    factory.registerAlias("TINYINT SIGNED", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT1 SIGNED", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("SMALLINT SIGNED", "Int16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("MEDIUMINT SIGNED", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT SIGNED", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INTEGER SIGNED", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIGINT SIGNED", "Int64", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("TINYINT UNSIGNED", "UInt8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT1 UNSIGNED", "UInt8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("SMALLINT UNSIGNED", "UInt16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("MEDIUMINT UNSIGNED", "UInt32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT UNSIGNED", "UInt32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INTEGER UNSIGNED", "UInt32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIGINT UNSIGNED", "UInt64", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIT", "UInt64", DataTypeFactory::CaseInsensitive);  /// MySQL
    factory.registerAlias("SET", "UInt64", DataTypeFactory::CaseInsensitive);  /// MySQL
    factory.registerAlias("YEAR", "UInt16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("TIME", "Int64", DataTypeFactory::CaseInsensitive);
}

}
