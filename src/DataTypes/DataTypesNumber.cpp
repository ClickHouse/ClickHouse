#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>


#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

static DataTypePtr createForInt8(const ASTPtr & arguments)
{
    if (arguments) {
        if (arguments->children.size() > 1)
            throw Exception("INT8 data type family must not have more than one argument - display width", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * argument = arguments->children[0]->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.get<UInt64>() == 0)
            throw Exception("INT8 data type family may have only a number (positive integer) as its argument", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    }

    return std::make_shared<DataTypeInt8>();
}

static DataTypePtr createForInt16(const ASTPtr & arguments)
{
    if (arguments) {
        if (arguments->children.size() > 1)
            throw Exception("INT16 data type family must not have more than one argument - display width", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * argument = arguments->children[0]->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.get<UInt64>() == 0)
            throw Exception("INT16 data type family may have only a number (positive integer) as its argument", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    }

    return std::make_shared<DataTypeInt16>();
}

static DataTypePtr createForInt32(const ASTPtr & arguments)
{
    if (arguments) {
        if (arguments->children.size() > 1)
            throw Exception("INT32 data type family must not have more than one argument - display width", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * argument = arguments->children[0]->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.get<UInt64>() == 0)
            throw Exception("INT32 data type family may have only a number (positive integer) as its argument", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    }

    return std::make_shared<DataTypeInt32>();
}

static DataTypePtr createForInt64(const ASTPtr & arguments)
{
    if (arguments) {
        if (arguments->children.size() > 1)
            throw Exception("INT64 data type family must not have more than one argument - display width", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * argument = arguments->children[0]->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.get<UInt64>() == 0)
            throw Exception("INT64 data type family may have only a number (positive integer) as its argument", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    }

    return std::make_shared<DataTypeInt64>();
}

static DataTypePtr createForFloat32(const ASTPtr & arguments)
{
    if (arguments) {
        if (arguments->children.size() > 2)
            throw Exception("FLOAT32 data type family must not have more than two arguments - total number of digits and number of digits following the decimal point", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
         else if (arguments->children.size() == 1) {
            const auto * argument = arguments->children[0]->as<ASTLiteral>();
            if (!argument || argument->value.getType() != Field::Types::UInt64)
                throw Exception("FLOAT32 data type family may have  a non negative number as its argument", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
        } else if (arguments->children.size() == 2) {
            const auto * beforePoint = arguments->children[0]->as<ASTLiteral>();
            const auto * afterPoint = arguments->children[1]->as<ASTLiteral>();
            if (!beforePoint || beforePoint->value.getType() != Field::Types::UInt64 ||
                !afterPoint|| afterPoint->value.getType() != Field::Types::UInt64)
                throw Exception("FLOAT32 data type family may have  a non negative number as its arguments", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
        }
    }

    return std::make_shared<DataTypeFloat32>();
}

static DataTypePtr createForFloat64(const ASTPtr & arguments)
{
    if (arguments) {
        if (arguments->children.size() != 2)
            throw Exception("FLOAT64 data type family must have only two arguments - total number of digits and number of digits following the decimal point", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        else {
            const auto * beforePoint = arguments->children[0]->as<ASTLiteral>();
            const auto * afterPoint = arguments->children[1]->as<ASTLiteral>();
            if (!beforePoint || beforePoint->value.getType() != Field::Types::UInt64 ||
                !afterPoint|| afterPoint->value.getType() != Field::Types::UInt64)
                throw Exception("FLOAT64 data type family may have  a non negative number as its arguments", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
        }
    }

    return std::make_shared<DataTypeFloat64>();
}



void registerDataTypeNumbers(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UInt8", [] { return DataTypePtr(std::make_shared<DataTypeUInt8>()); });
    factory.registerSimpleDataType("UInt16", [] { return DataTypePtr(std::make_shared<DataTypeUInt16>()); });
    factory.registerSimpleDataType("UInt32", [] { return DataTypePtr(std::make_shared<DataTypeUInt32>()); });
    factory.registerSimpleDataType("UInt64", [] { return DataTypePtr(std::make_shared<DataTypeUInt64>()); });

    factory.registerDataType("Int8", createForInt8);
    factory.registerDataType("Int16", createForInt16);
    factory.registerDataType("Int32", createForInt32);
    factory.registerDataType("Int64", createForInt64);
    factory.registerDataType("Float32", createForFloat32);
    factory.registerDataType("Float64", createForFloat64);

    /// These synonyms are added for compatibility.

    factory.registerAlias("TINYINT", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BOOL", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BOOLEAN", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT1", "Int8", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("SMALLINT", "Int16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT2", "Int16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT4", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INTEGER", "Int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIGINT", "Int64", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("FLOAT", "Float32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("DOUBLE", "Float64", DataTypeFactory::CaseInsensitive);
}

}
