#include <Columns/ColumnString.h>

#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/Serializations/SerializationString.h>
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

SerializationPtr DataTypeString::doGetSerialization(const SerializationInfoSettings & settings) const
{
    return SerializationString::create(settings.string_serialization_version);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (arguments && !arguments->children.empty())
    {
        if (arguments->children.size() > 1)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "String data type family mustn't have more than one argument - size in characters");
        }

        const auto * argument = arguments->children[0]->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64)
        {
            throw Exception(
                ErrorCodes::UNEXPECTED_AST_STRUCTURE, "String data type family may have only a number (positive integer) as its argument");
        }
    }

    return std::make_shared<DataTypeString>();
}

void registerDataTypeString(DataTypeFactory & factory)
{
    factory.registerDataType("String", create, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
Strings of an arbitrary length. The length is not limited. The value can contain an arbitrary set of bytes, including null bytes.
The String type replaces the types VARCHAR, BLOB, CLOB, and others from other DBMSs.

When creating tables, numeric parameters for string fields can be set (e.g. `VARCHAR(255)`), but ClickHouse ignores them.

Aliases:

- `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

## Encodings {#encodings}

ClickHouse does not have the concept of encodings. Strings can contain an arbitrary set of bytes, which are stored and output as-is.
If you need to store texts, we recommend using UTF-8 encoding. At the very least, if your terminal uses UTF-8 (as recommended), you can read and write your values without making conversions.
Similarly, certain functions for working with strings have separate variations that work under the assumption that the string contains a set of bytes representing a UTF-8 encoded text.
For example, the [length](/sql-reference/functions/array-functions#length) function calculates the string length in bytes, while the [lengthUTF8](../functions/string-functions.md#lengthUTF8) function calculates the string length in Unicode code points, assuming that the value is UTF-8 encoded.
)DOCS_MD",
            .syntax = "String",
            .related = {"FixedString"},
        });

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

}

}
