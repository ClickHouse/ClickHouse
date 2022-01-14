#include <Columns/ColumnArray.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationTupleElement.h>
#include <DataTypes/Serializations/SerializationNumber.h>

#include <Parsers/IAST.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <Core/NamesAndTypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


DataTypeArray::DataTypeArray(const DataTypePtr & nested_)
    : nested{nested_}
{
}


MutableColumnPtr DataTypeArray::createColumn() const
{
    return ColumnArray::create(nested->createColumn(), ColumnArray::ColumnOffsets::create());
}


Field DataTypeArray::getDefault() const
{
    return Array();
}


bool DataTypeArray::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && nested->equals(*static_cast<const DataTypeArray &>(rhs).nested);
}

DataTypePtr DataTypeArray::tryGetSubcolumnType(const String & subcolumn_name) const
{
    return tryGetSubcolumnTypeImpl(subcolumn_name, 0);
}

DataTypePtr DataTypeArray::tryGetSubcolumnTypeImpl(const String & subcolumn_name, size_t level) const
{
    if (subcolumn_name == "size" + std::to_string(level))
        return std::make_shared<DataTypeUInt64>();

    DataTypePtr subcolumn;
    if (const auto * nested_array = typeid_cast<const DataTypeArray *>(nested.get()))
        subcolumn = nested_array->tryGetSubcolumnTypeImpl(subcolumn_name, level + 1);
    else
        subcolumn = nested->tryGetSubcolumnType(subcolumn_name);

    if (subcolumn && subcolumn_name != MAIN_SUBCOLUMN_NAME)
        subcolumn = std::make_shared<DataTypeArray>(std::move(subcolumn));

    return subcolumn;
}

ColumnPtr DataTypeArray::getSubcolumn(const String & subcolumn_name, const IColumn & column) const
{
    return getSubcolumnImpl(subcolumn_name, column, 0);
}

ColumnPtr DataTypeArray::getSubcolumnImpl(const String & subcolumn_name, const IColumn & column, size_t level) const
{
    const auto & column_array = assert_cast<const ColumnArray &>(column);
    if (subcolumn_name == "size" + std::to_string(level))
        return arrayOffsetsToSizes(column_array.getOffsetsColumn());

    ColumnPtr subcolumn;
    if (const auto * nested_array = typeid_cast<const DataTypeArray *>(nested.get()))
        subcolumn = nested_array->getSubcolumnImpl(subcolumn_name, column_array.getData(), level + 1);
    else
        subcolumn = nested->getSubcolumn(subcolumn_name, column_array.getData());

    return ColumnArray::create(subcolumn, column_array.getOffsetsPtr());
}

SerializationPtr DataTypeArray::getSubcolumnSerialization(
    const String & subcolumn_name, const BaseSerializationGetter & base_serialization_getter) const
{
    return getSubcolumnSerializationImpl(subcolumn_name, base_serialization_getter, 0);
}

SerializationPtr DataTypeArray::getSubcolumnSerializationImpl(
    const String & subcolumn_name, const BaseSerializationGetter & base_serialization_getter, size_t level) const
{
    if (subcolumn_name == "size" + std::to_string(level))
        return std::make_shared<SerializationTupleElement>(base_serialization_getter(DataTypeUInt64()), subcolumn_name, false);

    SerializationPtr subcolumn;
    if (const auto * nested_array = typeid_cast<const DataTypeArray *>(nested.get()))
        subcolumn = nested_array->getSubcolumnSerializationImpl(subcolumn_name, base_serialization_getter, level + 1);
    else
        subcolumn = nested->getSubcolumnSerialization(subcolumn_name, base_serialization_getter);

    return std::make_shared<SerializationArray>(subcolumn);
}

SerializationPtr DataTypeArray::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationArray>(nested->getDefaultSerialization());
}

size_t DataTypeArray::getNumberOfDimensions() const
{
    const DataTypeArray * nested_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!nested_array)
        return 1;
    return 1 + nested_array->getNumberOfDimensions();   /// Every modern C++ compiler optimizes tail recursion.
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("Array data type family must have exactly one argument - type of elements", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<DataTypeArray>(DataTypeFactory::instance().get(arguments->children[0]));
}


void registerDataTypeArray(DataTypeFactory & factory)
{
    factory.registerDataType("Array", create);
}

}
