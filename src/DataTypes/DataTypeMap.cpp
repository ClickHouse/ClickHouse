#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <ext/map.h>
#include <ext/enumerate.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int EMPTY_DATA_PASSED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int DUPLICATE_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}


DataTypeMap::DataTypeMap(const DataTypes & elems_)
{
    key_type = elems_.size() == 1 ? DataTypeFactory::instance().get("String") : elems_[0];
    value_type = elems_.size() == 1 ? elems_[0] : elems_[1];

    keys = std::make_shared<DataTypeArray>(key_type);
    values = std::make_shared<DataTypeArray>(value_type);
    kv.push_back(keys);
    kv.push_back(values);
}

std::string DataTypeMap::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "Map(" 
        << (typeid_cast<const DataTypeArray *>(keys.get()))->getNestedType()->getName()
	<< ","
	<< typeid_cast<const DataTypeArray *>(values.get())->getNestedType()->getName() 
	<< ")";

    return s.str();
}

static inline IColumn & extractElementColumn(IColumn & column, size_t idx)
{
    return assert_cast<ColumnMap &>(column).getColumn(idx);
}

static inline const IColumn & extractElementColumn(const IColumn & column, size_t idx)
{
    return assert_cast<const ColumnMap &>(column).getColumn(idx);
}


void DataTypeMap::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & map = get<const Map &>(field);
    keys->serializeBinary(map[0], ostr);
    values->serializeBinary(map[1], ostr);
}

void DataTypeMap::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    field = Map(2);
    Map & map = get<Map &>(field);
    keys->deserializeBinary(map[0], istr);
    values->deserializeBinary(map[1], istr);
}

void DataTypeMap::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    keys->serializeBinary(extractElementColumn(column, 0), row_num, ostr);
    values->serializeBinary(extractElementColumn(column, 1), row_num, ostr);
}


template <typename F>
static void addElementSafe(const DataTypes & elems, IColumn & column, F && impl)
{
    /// We use the assumption that maps of zero size do not exist.
    size_t old_size = column.size();

    try
    {
        impl();
    }
    catch (...)
    {
        for (const auto & i : ext::range(0, ext::size(elems)))
        {
            auto & element_column = extractElementColumn(column, i);
            if (element_column.size() > old_size)
                element_column.popBack(1);
        }

        throw;
    }
}


void DataTypeMap::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    keys->deserializeBinary(extractElementColumn(column, 0), istr);
    values->deserializeBinary(extractElementColumn(column, 1), istr);
}

void DataTypeMap::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnArray & column_keys = assert_cast<const ColumnArray &>(extractElementColumn(column, 0));
    const ColumnArray & column_values = assert_cast<const ColumnArray &>(extractElementColumn(column, 1));
    const ColumnArray::Offsets & offsets = column_keys.getOffsets();
    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn & nested_keys = column_keys.getData();
    const IColumn & nested_values = column_values.getData();

    writeChar('{', ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeChar(',', ostr);
        key_type->serializeAsTextQuoted(nested_keys, i, ostr, settings);
        writeChar(':', ostr);
        value_type->serializeAsTextQuoted(nested_values, i, ostr, settings);
    }
    writeChar('}', ostr);
}

void DataTypeMap::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    assertChar('{', istr);
    keys->deserializeAsTextQuoted(extractElementColumn(column, 0), istr, settings);
    assertChar(',', istr);
    values->deserializeAsTextQuoted(extractElementColumn(column, 1), istr, settings);
    assertChar('}', istr);
}

void DataTypeMap::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('[', ostr);
    keys->serializeAsTextJSON(extractElementColumn(column, 0), row_num, ostr, settings);
    writeChar(',', ostr);
    values->serializeAsTextJSON(extractElementColumn(column, 1), row_num, ostr, settings);
    writeChar(']', ostr);
}

void DataTypeMap::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    assertChar('[', istr);
    keys->deserializeAsTextJSON(extractElementColumn(column, 0), istr, settings);
    assertChar(',', istr);
    values->deserializeAsTextJSON(extractElementColumn(column, 1), istr, settings);
    assertChar(']', istr);
}

void DataTypeMap::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeCString("<map>", ostr);
    writeCString("<keys>", ostr);
    keys->serializeAsTextXML(extractElementColumn(column, 0), row_num, ostr, settings);
    writeCString("</keys>", ostr);
    writeCString("<values>", ostr);
    values->serializeAsTextXML(extractElementColumn(column, 1), row_num, ostr, settings);
    writeCString("</values>", ostr);
    writeCString("</map>", ostr);
}

void DataTypeMap::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    keys->serializeAsTextCSV(extractElementColumn(column, 0), row_num, ostr, settings);
    writeChar(',', ostr);
    values->serializeAsTextCSV(extractElementColumn(column, 1), row_num, ostr, settings);
}

void DataTypeMap::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    keys->deserializeAsTextCSV(extractElementColumn(column, 0), istr, settings);
    assertChar(settings.csv.delimiter, istr);
    values->deserializeAsTextCSV(extractElementColumn(column, 1), istr, settings);
}

struct SerializeBinaryBulkStateMap : public IDataType::SerializeBinaryBulkState
{
    std::vector<IDataType::SerializeBinaryBulkStatePtr> states;
};

struct DeserializeBinaryBulkStateMap : public IDataType::DeserializeBinaryBulkState
{
    std::vector<IDataType::DeserializeBinaryBulkStatePtr> states;
};

static SerializeBinaryBulkStateMap * checkAndGetMapSerializeState(IDataType::SerializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeMap.", ErrorCodes::LOGICAL_ERROR);

    auto * map_state = typeid_cast<SerializeBinaryBulkStateMap *>(state.get());
    if (!map_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid SerializeBinaryBulkState for DataTypeMap. Expected: "
                        + demangle(typeid(SerializeBinaryBulkStateMap).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return map_state;
}

static DeserializeBinaryBulkStateMap * checkAndGetMapDeserializeState(IDataType::DeserializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeMap.", ErrorCodes::LOGICAL_ERROR);

    auto * map_state = typeid_cast<DeserializeBinaryBulkStateMap *>(state.get());
    if (!map_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid DeserializeBinaryBulkState for DataTypeMap. Expected: "
                        + demangle(typeid(DeserializeBinaryBulkStateMap).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return map_state;
}


void DataTypeMap::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto map_state = std::make_shared<SerializeBinaryBulkStateMap>();
    map_state->states.resize(2);

    settings.path.push_back(Substream::MapElement);
    keys->serializeBinaryBulkStatePrefix(settings, map_state->states[0]);
    values->serializeBinaryBulkStatePrefix(settings, map_state->states[1]);
    settings.path.pop_back();

    state = std::move(map_state);
}

void DataTypeMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetMapSerializeState(state);

    settings.path.push_back(Substream::MapElement);
    keys->serializeBinaryBulkStateSuffix(settings, map_state->states[0]);
    values->serializeBinaryBulkStateSuffix(settings, map_state->states[1]);
    settings.path.pop_back();
}

void DataTypeMap::deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const
{
    auto map_state = std::make_shared<DeserializeBinaryBulkStateMap>();
    map_state->states.resize(2);

    settings.path.push_back(Substream::MapElement);
    keys->deserializeBinaryBulkStatePrefix(settings, map_state->states[0]);
    values->deserializeBinaryBulkStatePrefix(settings, map_state->states[1]);
    settings.path.pop_back();

    state = std::move(map_state);
}


void DataTypeMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetMapSerializeState(state);
    settings.path.push_back(Substream::MapElement);
    const auto & keys_col = extractElementColumn(column, 0);
    keys->serializeBinaryBulkWithMultipleStreams(keys_col, offset, limit, settings, map_state->states[0]);
    const auto & values_col = extractElementColumn(column, 1);
    values->serializeBinaryBulkWithMultipleStreams(values_col, offset, limit, settings, map_state->states[1]);
    settings.path.pop_back();
}

void DataTypeMap::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetMapDeserializeState(state);

    settings.path.push_back(Substream::MapElement);
    settings.avg_value_size_hint = 0;
    auto & keys_col = extractElementColumn(column, 0);
    keys->deserializeBinaryBulkWithMultipleStreams(keys_col, limit, settings, map_state->states[0]);
    auto & values_col = extractElementColumn(column, 1);
    values->deserializeBinaryBulkWithMultipleStreams(values_col, limit, settings, map_state->states[1]);

    settings.path.pop_back();
}


void DataTypeMap::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    size_t stored = 0;
    if (!value_index)
    {
        keys->serializeProtobuf(extractElementColumn(column, value_index), row_num, protobuf, stored);
        if (!stored)
            return;
    }
    values->serializeProtobuf(extractElementColumn(column, value_index), row_num, protobuf, stored);
}

void DataTypeMap::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    bool key_get_row, value_get_row;
    keys->deserializeProtobuf(extractElementColumn(column, 0), protobuf, allow_add_row, key_get_row);
    values->deserializeProtobuf(extractElementColumn(column, 1), protobuf, allow_add_row, value_get_row);
    row_added = key_get_row & value_get_row;
}

MutableColumnPtr DataTypeMap::createColumn() const
{
    MutableColumns map_columns(2);
    map_columns[0] = keys->createColumn();
    map_columns[1] = values->createColumn();

    return ColumnMap::create(std::move(map_columns));
}

Field DataTypeMap::getDefault() const
{
    return Map();
}

bool DataTypeMap::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeMap & rhs_map = static_cast<const DataTypeMap &>(rhs);

    if (!keys->equals(*rhs_map.keys) || !values->equals(*rhs_map.values))
        return false;

    return true;
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception("Map cannot be empty", ErrorCodes::EMPTY_DATA_PASSED);
    if (arguments->children.size() > 2)
        throw Exception("Map arguments only support one/two type", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
    {
        if (child->as<ASTNameTypePair>())
        {
            throw Exception("Map arguments only support type", ErrorCodes::BAD_ARGUMENTS);
        }
        else
	        nested_types.emplace_back(DataTypeFactory::instance().get(child));
    }

    return std::make_shared<DataTypeMap>(nested_types);
}


void registerDataTypeMap(DataTypeFactory & factory)
{
    factory.registerDataType("Map", create);
}
}
