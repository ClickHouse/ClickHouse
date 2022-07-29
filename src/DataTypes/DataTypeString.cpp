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
            throw Exception("String data type family mustn't have more than one argument - size in characters", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * argument = arguments->children.front()->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64)
            throw Exception("String data type family may have only a number (positive integer) as its argument", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    }

    return std::make_shared<DataTypeString>();
}


void registerDataTypeString(DataTypeFactory & factory)
{
    factory.registerDataType("String", create);

    /// These synonims are added for compatibility.

    factory.registerAlias("CHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NCHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CHARACTER", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("VARCHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NVARCHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("VARCHAR2", "String", DataTypeFactory::CaseInsensitive); /// Oracle
    factory.registerAlias("TEXT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("TINYTEXT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("MEDIUMTEXT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("LONGTEXT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("TINYBLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("MEDIUMBLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("LONGBLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BYTEA", "String", DataTypeFactory::CaseInsensitive); /// PostgreSQL

    factory.registerAlias("CHARACTER LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CHARACTER VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CHAR LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CHAR VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHARACTER", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHARACTER LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHARACTER VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHAR VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NCHAR VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NCHAR LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BINARY LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BINARY VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("VARBINARY", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("GEOMETRY", "String", DataTypeFactory::CaseInsensitive); //mysql

}
}
