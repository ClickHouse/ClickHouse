#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationTupleElement.h>
#include <Columns/ColumnNullable.h>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ConcatReadBuffer.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


DataTypeNullable::DataTypeNullable(const DataTypePtr & nested_data_type_)
    : nested_data_type{nested_data_type_}
{
    if (!nested_data_type->canBeInsideNullable())
        throw Exception("Nested type " + nested_data_type->getName() + " cannot be inside Nullable type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


bool DataTypeNullable::onlyNull() const
{
    return typeid_cast<const DataTypeNothing *>(nested_data_type.get());
}


MutableColumnPtr DataTypeNullable::createColumn() const
{
    return ColumnNullable::create(nested_data_type->createColumn(), ColumnUInt8::create());
}

Field DataTypeNullable::getDefault() const
{
    return Null();
}

size_t DataTypeNullable::getSizeOfValueInMemory() const
{
    throw Exception("Value of type " + getName() + " in memory is not of fixed size.", ErrorCodes::LOGICAL_ERROR);
}


bool DataTypeNullable::equals(const IDataType & rhs) const
{
    return rhs.isNullable() && nested_data_type->equals(*static_cast<const DataTypeNullable &>(rhs).nested_data_type);
}

DataTypePtr DataTypeNullable::tryGetSubcolumnType(const String & subcolumn_name) const
{
    if (subcolumn_name == "null")
        return std::make_shared<DataTypeUInt8>();

    return nested_data_type->tryGetSubcolumnType(subcolumn_name);
}

ColumnPtr DataTypeNullable::getSubcolumn(const String & subcolumn_name, const IColumn & column) const
{
    const auto & column_nullable = assert_cast<const ColumnNullable &>(column);
    if (subcolumn_name == "null")
        return column_nullable.getNullMapColumnPtr();

    return nested_data_type->getSubcolumn(subcolumn_name, column_nullable.getNestedColumn());
}

SerializationPtr DataTypeNullable::getSubcolumnSerialization(
    const String & subcolumn_name, const BaseSerializationGetter & base_serialization_getter) const
{
    if (subcolumn_name == "null")
        return std::make_shared<SerializationTupleElement>(base_serialization_getter(DataTypeUInt8()), subcolumn_name, false);

    return nested_data_type->getSubcolumnSerialization(subcolumn_name, base_serialization_getter);
}

SerializationPtr DataTypeNullable::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationNullable>(nested_data_type->getDefaultSerialization());
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("Nullable data type family must have exactly one argument - nested type", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    DataTypePtr nested_type = DataTypeFactory::instance().get(arguments->children[0]);

    return std::make_shared<DataTypeNullable>(nested_type);
}


void registerDataTypeNullable(DataTypeFactory & factory)
{
    factory.registerDataType("Nullable", create);
}


DataTypePtr makeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return type;
    return std::make_shared<DataTypeNullable>(type);
}

DataTypePtr removeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return static_cast<const DataTypeNullable &>(*type).getNestedType();
    return type;
}

}
