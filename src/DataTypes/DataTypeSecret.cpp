#include <Core/Defines.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeSecret.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationSecret.h>

#include <Parsers/IAST.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

Field DataTypeSecret::getDefault() const
{
    return String();
}

MutableColumnPtr DataTypeSecret::createColumn() const
{
    return ColumnString::create();
}

bool DataTypeSecret::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeSecret::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationSecret>();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (arguments && !arguments->children.empty())
    {
        throw Exception("Secret data type family doesn't support arguments", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    }

    return std::make_shared<DataTypeSecret>();
}

void registerDataTypeSecret(DataTypeFactory & factory)
{
    factory.registerDataType("Secret", create);
}

}
