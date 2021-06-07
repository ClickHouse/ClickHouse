#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/UserDefinedDataType.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTCreateDataTypeQuery.h>
#include <Parsers/IAST.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_ALREADY_EXISTS;
}

DataTypePtr UserDefinedDataType::getNested() const
{
    return nested;
}

ASTPtr UserDefinedDataType::getNestedAST() const
{
    return nested_ast;
}

String UserDefinedDataType::getTypeName() const
{
    return type_name;
}

void UserDefinedDataType::setNested(const DataTypePtr & nested_)
{
    nested = nested_;
}

void UserDefinedDataType::setNestedAST(const ASTPtr & nested_ast_)
{
    nested_ast = nested_ast_;
}

void UserDefinedDataType::setTypeName(const String & type_name_)
{
    type_name = type_name_;
}

std::string UserDefinedDataType::doGetName() const
{
    WriteBufferFromOwnString s;
    s << type_name;
    return s.str();
}


DataTypePtr UserDefinedDataType::tryGetSubcolumnType(const String & subcolumn_name) const
{
    return nested->tryGetSubcolumnType(subcolumn_name);
}

ColumnPtr UserDefinedDataType::getSubcolumn(const String & subcolumn_name, const IColumn & column) const
{
    return nested->getSubcolumn(subcolumn_name, column);
}

SerializationPtr UserDefinedDataType::getSubcolumnSerialization(
    const String & subcolumn_name, const BaseSerializationGetter & base_serialization_getter) const
{
    return nested->getSubcolumnSerialization(subcolumn_name, base_serialization_getter);
}

MutableColumnPtr UserDefinedDataType::createColumn() const
{
    return nested->createColumn();
}

Field UserDefinedDataType::getDefault() const
{
    return nested->getDefault();
}

bool UserDefinedDataType::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const UserDefinedDataType & rhs_map = static_cast<const UserDefinedDataType &>(rhs);
    return nested->equals(*rhs_map.nested) && type_name == rhs_map.type_name;
}

static UserDefinedDataTypePtr create()
{
    return std::make_shared<UserDefinedDataType>();
}


void registerUserDefinedDataType(
    DataTypeFactory & factory,
    const ASTCreateDataTypeQuery & createDataTypeQuery)
{
    if (factory.hasNameOrAlias(createDataTypeQuery.type_name))
        throw Exception("The data type '" + createDataTypeQuery.type_name + "' already exists", ErrorCodes::TYPE_ALREADY_EXISTS);

    factory.get(createDataTypeQuery.nested); // will throw exception if nested type was not registered
    factory.registerUserDefinedDataType(createDataTypeQuery.type_name, create, createDataTypeQuery);
}

UserDefinedDataType::UserDefinedDataType()
    : nested{nullptr}
{
}

SerializationPtr UserDefinedDataType::doGetDefaultSerialization() const
{
    return nested->getDefaultSerialization();
}

}
