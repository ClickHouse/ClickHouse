#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/JSONDataParser.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/HashTable/HashSet.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionsConversion.h>

#include <Common/FieldVisitorToString.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>
#include <magic_enum.hpp>
#include <memory>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

template <typename Parser>
template <typename Reader>
void SerializationObject<Parser>::deserializeTextImpl(IColumn & column, Reader && reader) const
{
    auto & column_object = assert_cast<ColumnObject &>(column);

    String buf;
    reader(buf);
    std::optional<ParseResult> result;

    /// Treat empty string as an empty object
    /// for better CAST from String to Object.
    if (!buf.empty())
    {
        auto parser = parsers_pool.get([] { return new Parser; });
        result = parser->parse(buf.data(), buf.size());
    }
    else
    {
        result = ParseResult{};
    }

    if (!result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse object");

    auto & [paths, values] = *result;
    assert(paths.size() == values.size());

    HashSet<StringRef, StringRefHash> paths_set;
    size_t column_size = column_object.size();

    for (size_t i = 0; i < paths.size(); ++i)
    {
        auto field_info = getFieldInfo(values[i]);
        if (isNothing(field_info.scalar_type))
            continue;

        if (!paths_set.insert(paths[i].getPath()).second)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Object has ambiguous path: {}", paths[i].getPath());

        if (!column_object.hasSubcolumn(paths[i]))
        {
            if (paths[i].hasNested())
                column_object.addNestedSubcolumn(paths[i], field_info, column_size);
            else
                column_object.addSubcolumn(paths[i], column_size);
        }

        auto & subcolumn = column_object.getSubcolumn(paths[i]);
        assert(subcolumn.size() == column_size);

        subcolumn.insert(std::move(values[i]), std::move(field_info));
    }

    /// Insert default values to missed subcolumns.
    const auto & subcolumns = column_object.getSubcolumns();
    for (const auto & entry : subcolumns)
    {
        if (!paths_set.has(entry->path.getPath()))
        {
            bool inserted = column_object.tryInsertDefaultFromNested(entry);
            if (!inserted)
                entry->data.insertDefault();
        }
    }

    column_object.incrementNumRows();
}

template <typename Parser>
void SerializationObject<Parser>::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { readStringInto(s, istr); });
}

template <typename Parser>
void SerializationObject<Parser>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { readEscapedString(s, istr); });
}

template <typename Parser>
void SerializationObject<Parser>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { readQuotedStringInto<true>(s, istr); });
}

template <typename Parser>
void SerializationObject<Parser>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { Parser::readJSON(s, istr); });
}

template <typename Parser>
void SerializationObject<Parser>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, [&](String & s) { readCSVStringInto(s, istr, settings.csv); });
}

template <typename Parser>
template <typename TSettings>
void SerializationObject<Parser>::checkSerializationIsSupported(const TSettings & settings) const
{
    if (settings.position_independent_encoding)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject doesn't support serialization with position independent encoding");
}

template <typename Parser>
struct SerializationObject<Parser>::SerializeStateObject : public ISerialization::SerializeBinaryBulkState
{
    bool is_first = true;
    DataTypePtr nested_type;
    SerializationPtr nested_serialization;
    SerializeBinaryBulkStatePtr nested_state;
};

template <typename Parser>
struct SerializationObject<Parser>::DeserializeStateObject : public ISerialization::DeserializeBinaryBulkState
{
    BinarySerializationKind kind;
    DataTypePtr nested_type;
    SerializationPtr nested_serialization;
    DeserializeBinaryBulkStatePtr nested_state;
};

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings);
    if (state)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject doesn't support serialization with non-trivial state");

    settings.path.push_back(Substream::ObjectStructure);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for kind of binary serialization");

    writeIntBinary(static_cast<UInt8>(BinarySerializationKind::TUPLE), *stream);
    state = std::make_shared<SerializeStateObject>();
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings);
    auto * state_object = checkAndGetState<SerializeStateObject>(state);

    settings.path.push_back(Substream::ObjectData);
    state_object->nested_serialization->serializeBinaryBulkStateSuffix(settings, state_object->nested_state);
    settings.path.pop_back();
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings);
    if (state)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject doesn't support serialization with non-trivial state");

    settings.path.push_back(Substream::ObjectStructure);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read kind of binary serialization of DataTypeObject, because its stream is missing");

    UInt8 kind_raw;
    readIntBinary(kind_raw, *stream);
    auto kind = magic_enum::enum_cast<BinarySerializationKind>(kind_raw);
    if (!kind)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Unknown binary serialization kind of Object: " + std::to_string(kind_raw));

    auto state_object = std::make_shared<DeserializeStateObject>();
    state_object->kind = *kind;

    if (state_object->kind == BinarySerializationKind::TUPLE)
    {
        String data_type_name;
        readStringBinary(data_type_name, *stream);
        state_object->nested_type = DataTypeFactory::instance().get(data_type_name);
        state_object->nested_serialization = state_object->nested_type->getDefaultSerialization();

        if (!isTuple(state_object->nested_type))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Data of type Object should be written as Tuple, got: {}", data_type_name);
    }
    else if (state_object->kind == BinarySerializationKind::STRING)
    {
        state_object->nested_type = std::make_shared<DataTypeString>();
        state_object->nested_serialization = std::make_shared<SerializationString>();
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Unknown binary serialization kind of Object: " + std::to_string(kind_raw));
    }

    settings.path.push_back(Substream::ObjectData);
    state_object->nested_serialization->deserializeBinaryBulkStatePrefix(settings, state_object->nested_state);
    settings.path.pop_back();

    state = std::move(state_object);
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings);
    const auto & column_object = assert_cast<const ColumnObject &>(column);
    auto * state_object = checkAndGetState<SerializeStateObject>(state);

    if (!column_object.isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write non-finalized ColumnObject");

    auto [tuple_column, tuple_type] = unflattenObjectToTuple(column_object);

    if (state_object->is_first)
    {
        /// Actually it's a part of serializeBinaryBulkStatePrefix,
        /// but it cannot be done there, because we have to know the
        /// structure of column.

        settings.path.push_back(Substream::ObjectStructure);
        if (auto * stream = settings.getter(settings.path))
            writeStringBinary(tuple_type->getName(), *stream);

        state_object->nested_type = tuple_type;
        state_object->nested_serialization = tuple_type->getDefaultSerialization();
        state_object->is_first = false;

        settings.path.back() = Substream::ObjectData;
        state_object->nested_serialization->serializeBinaryBulkStatePrefix(settings, state_object->nested_state);
        settings.path.pop_back();
    }
    else if (!state_object->nested_type->equals(*tuple_type))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Types of internal column of Object mismatched. Expected: {}, Got: {}",
            state_object->nested_type->getName(), tuple_type->getName());
    }

    settings.path.push_back(Substream::ObjectData);
    if (auto * stream = settings.getter(settings.path))
    {
        state_object->nested_serialization->serializeBinaryBulkWithMultipleStreams(
            *tuple_column, offset, limit, settings, state_object->nested_state);
    }

    settings.path.pop_back();
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    checkSerializationIsSupported(settings);
    if (!column->empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject cannot be deserialized to non-empty column");

    auto mutable_column = column->assumeMutable();
    auto & column_object = assert_cast<ColumnObject &>(*mutable_column);
    auto * state_object = checkAndGetState<DeserializeStateObject>(state);

    settings.path.push_back(Substream::ObjectData);
    if (state_object->kind == BinarySerializationKind::STRING)
        deserializeBinaryBulkFromString(column_object, limit, settings, *state_object, cache);
    else
        deserializeBinaryBulkFromTuple(column_object, limit, settings, *state_object, cache);

    settings.path.pop_back();
    column_object.checkConsistency();
    column_object.finalize();
    column = std::move(mutable_column);
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinaryBulkFromString(
    ColumnObject & column_object,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeStateObject & state,
    SubstreamsCache * cache) const
{
    ColumnPtr column_string = state.nested_type->createColumn();
    state.nested_serialization->deserializeBinaryBulkWithMultipleStreams(
        column_string, limit, settings, state.nested_state, cache);

    ConvertImplGenericFromString<ColumnString>::executeImpl(*column_string, column_object, *this, column_string->size());
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinaryBulkFromTuple(
    ColumnObject & column_object,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeStateObject & state,
    SubstreamsCache * cache) const
{
    ColumnPtr column_tuple = state.nested_type->createColumn();
    state.nested_serialization->deserializeBinaryBulkWithMultipleStreams(
        column_tuple, limit, settings, state.nested_state, cache);

    auto [tuple_paths, tuple_types] = flattenTuple(state.nested_type);
    auto flattened_tuple = flattenTuple(column_tuple);
    const auto & tuple_columns = assert_cast<const ColumnTuple &>(*flattened_tuple).getColumns();

    assert(tuple_paths.size() == tuple_types.size());
    size_t num_subcolumns = tuple_paths.size();

    if (tuple_columns.size() != num_subcolumns)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Inconsistent type ({}) and column ({}) while reading column of type Object",
            state.nested_type->getName(), column_tuple->getName());

    for (size_t i = 0; i < num_subcolumns; ++i)
        column_object.addSubcolumn(tuple_paths[i], tuple_columns[i]->assumeMutable());
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinary(const Field &, WriteBuffer &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinary(Field &, ReadBuffer &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinary(const IColumn &, size_t, WriteBuffer &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinary(IColumn &, ReadBuffer &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

/// TODO: use format different of JSON in serializations.

template <typename Parser>
void SerializationObject<Parser>::serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_object = assert_cast<const ColumnObject &>(column);
    const auto & subcolumns = column_object.getSubcolumns();

    writeChar('{', ostr);
    for (auto it = subcolumns.begin(); it != subcolumns.end(); ++it)
    {
        if (it != subcolumns.begin())
            writeCString(",", ostr);

        writeDoubleQuoted((*it)->path.getPath(), ostr);
        writeChar(':', ostr);

        auto serialization = (*it)->data.getLeastCommonType()->getDefaultSerialization();
        serialization->serializeTextJSON((*it)->data.getFinalizedColumn(), row_num, ostr, settings);
    }
    writeChar('}', ostr);
}

template <typename Parser>
void SerializationObject<Parser>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr, settings);
}

template <typename Parser>
void SerializationObject<Parser>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString ostr_str;
    serializeTextImpl(column, row_num, ostr_str, settings);
    writeEscapedString(ostr_str.str(), ostr);
}

template <typename Parser>
void SerializationObject<Parser>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString ostr_str;
    serializeTextImpl(column, row_num, ostr_str, settings);
    writeQuotedString(ostr_str.str(), ostr);
}

template <typename Parser>
void SerializationObject<Parser>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr, settings);
}

template <typename Parser>
void SerializationObject<Parser>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString ostr_str;
    serializeTextImpl(column, row_num, ostr_str, settings);
    writeCSVString(ostr_str.str(), ostr);
}

SerializationPtr getObjectSerialization(const String & schema_format)
{
    if (schema_format == "json")
    {
#if USE_SIMDJSON
        return std::make_shared<SerializationObject<JSONDataParser<SimdJSONParser>>>();
#elif USE_RAPIDJSON
        return std::make_shared<SerializationObject<JSONDataParser<RapidJSONParser>>>();
#else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "To use data type Object with JSON format ClickHouse should be built with Simdjson or Rapidjson");
#endif
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown schema format '{}'", schema_format);
}

}
