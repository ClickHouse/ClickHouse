#include <Core/Field.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeOneElementTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void DataTypeOneElementTuple::addToPath(SubstreamPath & path) const
{
    path.push_back(Substream::TupleElement);
    path.back().tuple_element_name = name;
    path.back().escape_tuple_delimiter = escape_delimiter;
}

std::string DataTypeOneElementTuple::doGetName() const
{
    WriteBufferFromOwnString s;
    s << TYPE_NAME << "(" << backQuoteIfNeed(name) << " " << element->getName() << ")";
    return s.str();
}

bool DataTypeOneElementTuple::equals(const IDataType & rhs) const
{
    const auto * rhs_tuple = typeid_cast<const DataTypeOneElementTuple *>(&rhs);
    if (!rhs_tuple)
        return false;

    return element->equals(*rhs_tuple->element)
        && name == rhs_tuple->name
        && escape_delimiter == rhs_tuple->escape_delimiter;
}

void DataTypeOneElementTuple::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    addToPath(path);
    element->enumerateStreams(callback, path);
    path.pop_back();
}

void DataTypeOneElementTuple::DataTypeOneElementTuple::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    element->serializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}

void DataTypeOneElementTuple::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    element->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void DataTypeOneElementTuple::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    element->deserializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}

void DataTypeOneElementTuple::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    element->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
    settings.path.pop_back();
}

void DataTypeOneElementTuple::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    element->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state);
    settings.path.pop_back();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data type {} takes only 1 argument", DataTypeOneElementTuple::TYPE_NAME);

    const auto * name_type = arguments->children[0]->as<ASTNameTypePair>();
    if (!name_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data type {} takes only pair with name and type", DataTypeOneElementTuple::TYPE_NAME);

    auto nested_type = DataTypeFactory::instance().get(name_type->type);
    return std::make_shared<DataTypeOneElementTuple>(std::move(nested_type), name_type->name);
}

void registerDataTypeOneElementTuple(DataTypeFactory & factory)
{
    factory.registerDataType(DataTypeOneElementTuple::TYPE_NAME, create);
}

}
