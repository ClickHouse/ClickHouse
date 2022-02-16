#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationObject.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int BAD_ARGUMENTS;
}

DataTypeObject::DataTypeObject(const String & schema_format_, bool is_nullable_)
    : schema_format(Poco::toLower(schema_format_))
    , is_nullable(is_nullable_)
    , default_serialization(getObjectSerialization(schema_format))
{
}

bool DataTypeObject::equals(const IDataType & rhs) const
{
    if (const auto * object = typeid_cast<const DataTypeObject *>(&rhs))
        return schema_format == object->schema_format;
    return false;
}

SerializationPtr DataTypeObject::doGetDefaultSerialization() const
{
    return default_serialization;
}

static constexpr auto NAME_DEFAULT = "Default";
static constexpr auto NAME_NULL = "Null";

String DataTypeObject::doGetName() const
{
    WriteBufferFromOwnString out;
    out << "Object(" << quote << schema_format;
    if (is_nullable)
        out << ", " << quote << NAME_NULL;
    out << ")";
    return out.str();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty() || arguments->children.size() > 2)
        throw Exception("Object data type family must have one or two arguments -"
            " name of schema format and type of default value",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * literal = arguments->children[0]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "Object data type family must have a const string as its first argument");

    bool is_nullable = false;
    if (arguments->children.size() == 2)
    {
        const auto * default_literal = arguments->children[1]->as<ASTLiteral>();
        if (!default_literal || default_literal->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                "Object data type family must have a const string as its second argument");

        const auto & default_kind = default_literal->value.get<const String &>();
        if (default_kind == NAME_NULL)
            is_nullable = true;
        else if (default_kind != NAME_DEFAULT)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unexpected type of default value '{}'. Should be {} or {}",
                default_kind, NAME_DEFAULT, NAME_NULL);
    }

    return std::make_shared<DataTypeObject>(literal->value.get<const String>(), is_nullable);
}

void registerDataTypeObject(DataTypeFactory & factory)
{
    factory.registerDataType("Object", create);
    factory.registerSimpleDataType("JSON",
        [] { return std::make_shared<DataTypeObject>("JSON", false); },
        DataTypeFactory::CaseInsensitive);
}

}
