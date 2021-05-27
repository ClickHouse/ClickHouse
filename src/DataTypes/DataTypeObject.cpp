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
}

DataTypeObject::DataTypeObject(const String & schema_format_)
    : schema_format(Poco::toLower(schema_format_))
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

String DataTypeObject::doGetName() const
{
    WriteBufferFromOwnString out;
    out << "Object(" << quote << schema_format << ")";
    return out.str();
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("Object data type family must have exactly one argument - name of schema format",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * argument = arguments->children[0]->as<ASTLiteral>();
    if (!argument || argument->value.getType() != Field::Types::String)
        throw Exception("Object data type family must have a string as its argument",
            ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    return std::make_shared<DataTypeObject>(argument->value.get<String>());
}

void registerDataTypeObject(DataTypeFactory & factory)
{
    factory.registerDataType("Object", create);
    factory.registerSimpleDataType("JSON",
        [] { return std::make_shared<DataTypeObject>("JSON"); },
        DataTypeFactory::CaseInsensitive);
}

}
