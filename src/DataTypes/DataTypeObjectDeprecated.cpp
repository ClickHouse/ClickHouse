#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeObjectDeprecated.h>
#include <DataTypes/Serializations/SerializationObjectDeprecated.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>
#include <IO/Operators.h>

#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

DataTypeObjectDeprecated::DataTypeObjectDeprecated(const String & schema_format_, bool is_nullable_)
    : schema_format(Poco::toLower(schema_format_))
    , is_nullable(is_nullable_)
{
}

bool DataTypeObjectDeprecated::equals(const IDataType & rhs) const
{
    if (const auto * object = typeid_cast<const DataTypeObjectDeprecated *>(&rhs))
        return schema_format == object->schema_format && is_nullable == object->is_nullable;
    return false;
}

SerializationPtr DataTypeObjectDeprecated::doGetDefaultSerialization() const
{
    return getObjectSerialization(schema_format);
}

String DataTypeObjectDeprecated::doGetName() const
{
    WriteBufferFromOwnString out;
    if (is_nullable)
        out << "Object(Nullable(" << quote << schema_format << "))";
    else
        out << "Object(" << quote << schema_format << ")";
    return out.str();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Object data type family must have one argument - name of schema format");

    ASTPtr schema_argument = arguments->children[0];
    bool is_nullable = false;

    if (const auto * type = schema_argument->as<ASTDataType>())
    {
        if (type->name != "Nullable" || type->arguments->children.size() != 1)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                            "Expected 'Nullable(<schema_name>)' as parameter for type Object (function: {})", type->name);

        schema_argument = type->arguments->children[0];
        is_nullable = true;
    }

    const auto * literal = schema_argument->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "Object data type family must have a const string as its schema name parameter");

    return std::make_shared<DataTypeObjectDeprecated>(literal->value.safeGet<const String &>(), is_nullable);
}

void registerDataTypeObjectDeprecated(DataTypeFactory & factory)
{
    factory.registerDataType("Object", create);
}

}
