#include <Columns/ColumnString.h>
#include <Core/Field.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationString.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

Field DataTypeString::getDefault() const
{
    return String();
}

MutableColumnPtr DataTypeString::createColumn() const
{
    return ColumnString::create();
}


bool DataTypeString::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeString::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationString>();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (arguments && !arguments->children.empty())
    {
        if (arguments->children.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "String data type family mustn't have more than one argument - size in characters");

        const auto * argument = arguments->children[0]->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "String data type family may have only a number (positive integer) as its argument");
    }

    return std::make_shared<DataTypeString>();
}


void registerDataTypeString(DataTypeFactory & factory)
{
    factory.registerDataType("String", create);

    /// These synonims are added for compatibility.

    factory.registerAlias("CHAR", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NCHAR", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("CHARACTER", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("VARCHAR", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NVARCHAR", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("VARCHAR2", "String", DataTypeFactory::Case::Insensitive); /// Oracle
    factory.registerAlias("TEXT", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("TINYTEXT", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("MEDIUMTEXT", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("LONGTEXT", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("BLOB", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("CLOB", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("TINYBLOB", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("MEDIUMBLOB", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("LONGBLOB", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("BYTEA", "String", DataTypeFactory::Case::Insensitive); /// PostgreSQL

    factory.registerAlias("CHARACTER LARGE OBJECT", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("CHARACTER VARYING", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("CHAR LARGE OBJECT", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("CHAR VARYING", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NATIONAL CHAR", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NATIONAL CHARACTER", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NATIONAL CHARACTER LARGE OBJECT", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NATIONAL CHARACTER VARYING", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NATIONAL CHAR VARYING", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NCHAR VARYING", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NCHAR LARGE OBJECT", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("BINARY LARGE OBJECT", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("BINARY VARYING", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("VARBINARY", "String", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("GEOMETRY", "String", DataTypeFactory::Case::Insensitive); //mysql

}
}
