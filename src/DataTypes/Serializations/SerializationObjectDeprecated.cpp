#include <DataTypes/Serializations/SerializationObjectDeprecated.h>
#include <DataTypes/Serializations/JSONDataParser.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Columns/ColumnObjectDeprecated.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>

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
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_PARSE_TEXT;
    extern const int EXPERIMENTAL_FEATURE_ERROR;
}

template <typename Parser>
template <typename Reader>
void SerializationObjectDeprecated<Parser>::deserializeTextImpl(IColumn & column, Reader && reader) const
{
    auto & column_object = assert_cast<ColumnObjectDeprecated &>(column);

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

    size_t old_column_size = column_object.size();
    for (size_t i = 0; i < paths.size(); ++i)
    {
        auto field_info = getFieldInfo(values[i]);
        if (field_info.need_fold_dimension)
            values[i] = applyVisitor(FieldVisitorFoldDimension(field_info.num_dimensions), std::move(values[i]));
        if (isNothing(field_info.scalar_type))
            continue;

        if (!column_object.hasSubcolumn(paths[i]))
        {
            if (paths[i].hasNested())
                column_object.addNestedSubcolumn(paths[i], field_info, old_column_size);
            else
                column_object.addSubcolumn(paths[i], old_column_size);
        }

        auto & subcolumn = column_object.getSubcolumn(paths[i]);
        assert(subcolumn.size() == old_column_size);

        subcolumn.insert(std::move(values[i]), std::move(field_info));
    }

    /// Insert default values to missed subcolumns.
    const auto & subcolumns = column_object.getSubcolumns();
    for (const auto & entry : subcolumns)
    {
        if (entry->data.size() == old_column_size)
        {
            bool inserted = column_object.tryInsertDefaultFromNested(entry);
            if (!inserted)
                entry->data.insertDefault();
        }
    }

    column_object.incrementNumRows();
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { readStringInto(s, istr); });
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, [&](String & s) { settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(s, istr) : readEscapedString(s, istr); });
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { readQuotedStringInto<true>(s, istr); });
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { Parser::readJSON(s, istr); });
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, [&](String & s) { readCSVStringInto(s, istr, settings.csv); });
}

template <typename Parser>
template <typename TSettings>
void SerializationObjectDeprecated<Parser>::checkSerializationIsSupported(const TSettings & settings) const
{
    if (settings.position_independent_encoding)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject doesn't support serialization with position independent encoding");
}

template <typename Parser>
struct SerializationObjectDeprecated<Parser>::SerializeStateObject : public ISerialization::SerializeBinaryBulkState
{
    DataTypePtr nested_type;
    SerializationPtr nested_serialization;
    SerializeBinaryBulkStatePtr nested_state;
};

template <typename Parser>
struct SerializationObjectDeprecated<Parser>::DeserializeStateObject : public ISerialization::DeserializeBinaryBulkState
{
    BinarySerializationKind kind;
    DataTypePtr nested_type;
    SerializationPtr nested_serialization;
    DeserializeBinaryBulkStatePtr nested_state;
};

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings);
    if (state)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject doesn't support serialization with non-trivial state");

    const auto & column_object = assert_cast<const ColumnObjectDeprecated &>(column);
    if (!column_object.isFinalized())
    {
        auto finalized = column_object.cloneFinalized();
        serializeBinaryBulkStatePrefix(*finalized, settings, state);
        return;
    }

    settings.path.push_back(Substream::DeprecatedObjectStructure);
    auto * stream = settings.getter(settings.path);

    if (!stream)
        throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR, "Missing stream for kind of binary serialization");

    auto [tuple_column, tuple_type] = unflattenObjectToTuple(column_object);

    writeIntBinary(static_cast<UInt8>(BinarySerializationKind::TUPLE), *stream);
    writeStringBinary(tuple_type->getName(), *stream);

    auto state_object = std::make_shared<SerializeStateObject>();
    state_object->nested_type = tuple_type;
    state_object->nested_serialization = tuple_type->getDefaultSerialization();

    settings.path.back() = Substream::DeprecatedObjectData;
    state_object->nested_serialization->serializeBinaryBulkStatePrefix(*tuple_column, settings, state_object->nested_state);

    state = std::move(state_object);
    settings.path.pop_back();
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings);
    auto * state_object = checkAndGetState<SerializeStateObject>(state);

    settings.path.push_back(Substream::DeprecatedObjectData);
    state_object->nested_serialization->serializeBinaryBulkStateSuffix(settings, state_object->nested_state);
    settings.path.pop_back();
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    checkSerializationIsSupported(settings);
    if (state)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject doesn't support serialization with non-trivial state");

    settings.path.push_back(Substream::DeprecatedObjectStructure);
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
            "Unknown binary serialization kind of Object: {}", std::to_string(kind_raw));

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
            "Unknown binary serialization kind of Object: {}", std::to_string(kind_raw));
    }

    settings.path.push_back(Substream::DeprecatedObjectData);
    state_object->nested_serialization->deserializeBinaryBulkStatePrefix(settings, state_object->nested_state, cache);
    settings.path.pop_back();

    state = std::move(state_object);
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings);
    const auto & column_object = assert_cast<const ColumnObjectDeprecated &>(column);
    auto * state_object = checkAndGetState<SerializeStateObject>(state);

    if (!column_object.isFinalized())
    {
        auto finalized = column_object.cloneFinalized();
        serializeBinaryBulkWithMultipleStreams(*finalized, offset, limit, settings, state);
        return;
    }

    auto [tuple_column, tuple_type] = unflattenObjectToTuple(column_object);

    if (!state_object->nested_type->equals(*tuple_type))
    {
        throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR,
            "Types of internal column of Object mismatched. Expected: {}, Got: {}",
            state_object->nested_type->getName(), tuple_type->getName());
    }

    settings.path.push_back(Substream::DeprecatedObjectData);
    if (auto * /*stream*/ _ = settings.getter(settings.path))
    {
        state_object->nested_serialization->serializeBinaryBulkWithMultipleStreams(
            *tuple_column, offset, limit, settings, state_object->nested_state);
    }

    settings.path.pop_back();
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeBinaryBulkWithMultipleStreams(
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
    auto & column_object = assert_cast<ColumnObjectDeprecated &>(*mutable_column);
    auto * state_object = checkAndGetState<DeserializeStateObject>(state);

    settings.path.push_back(Substream::DeprecatedObjectData);
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
void SerializationObjectDeprecated<Parser>::deserializeBinaryBulkFromString(
    ColumnObjectDeprecated & column_object,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeStateObject & state,
    SubstreamsCache * cache) const
{
    ColumnPtr column_string = state.nested_type->createColumn();
    state.nested_serialization->deserializeBinaryBulkWithMultipleStreams(
        column_string, limit, settings, state.nested_state, cache);

    size_t input_rows_count = column_string->size();
    column_object.reserve(input_rows_count);

    FormatSettings format_settings;
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const auto & val = column_string->getDataAt(i);
        ReadBufferFromMemory read_buffer(val.data, val.size);
        deserializeWholeText(column_object, read_buffer, format_settings);

        if (!read_buffer.eof())
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT,
                "Cannot parse string to column Object. Expected eof");
    }
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeBinaryBulkFromTuple(
    ColumnObjectDeprecated & column_object,
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
void SerializationObjectDeprecated<Parser>::serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObjectDeprecated");
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObjectDeprecated");
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObjectDeprecated");
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObjectDeprecated");
}

/// TODO: use format different of JSON in serializations.

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_object = assert_cast<const ColumnObjectDeprecated &>(column);
    const auto & subcolumns = column_object.getSubcolumns();

    writeChar('{', ostr);
    for (auto it = subcolumns.begin(); it != subcolumns.end(); ++it)
    {
        const auto & entry = *it;
        if (it != subcolumns.begin())
            writeCString(",", ostr);

        writeDoubleQuoted(entry->path.getPath(), ostr);
        writeChar(':', ostr);
        serializeTextFromSubcolumn(entry->data, row_num, ostr, settings);
    }
    writeChar('}', ostr);
}

template <typename Parser>
template <bool pretty_json>
void SerializationObjectDeprecated<Parser>::serializeTextFromSubcolumn(
    const ColumnObjectDeprecated::Subcolumn & subcolumn, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    const auto & least_common_type = subcolumn.getLeastCommonType();

    if (subcolumn.isFinalized())
    {
        const auto & finalized_column = subcolumn.getFinalizedColumn();
        auto info = least_common_type->getSerializationInfo(finalized_column);
        auto serialization = least_common_type->getSerialization(*info);
        if constexpr (pretty_json)
            serialization->serializeTextJSONPretty(finalized_column, row_num, ostr, settings, indent);
        else
            serialization->serializeTextJSON(finalized_column, row_num, ostr, settings);
        return;
    }

    size_t ind = row_num;
    if (ind < subcolumn.getNumberOfDefaultsInPrefix())
    {
        /// Suboptimal, but it should happen rarely.
        auto tmp_column = subcolumn.getLeastCommonType()->createColumn();
        tmp_column->insertDefault();

        auto info = least_common_type->getSerializationInfo(*tmp_column);
        auto serialization = least_common_type->getSerialization(*info);
        if constexpr (pretty_json)
            serialization->serializeTextJSONPretty(*tmp_column, 0, ostr, settings, indent);
        else
            serialization->serializeTextJSON(*tmp_column, 0, ostr, settings);
        return;
    }

    ind -= subcolumn.getNumberOfDefaultsInPrefix();
    for (const auto & part : subcolumn.getData())
    {
        if (ind < part->size())
        {
            auto part_type = getDataTypeByColumn(*part);
            auto info = part_type->getSerializationInfo(*part);
            auto serialization = part_type->getSerialization(*info);
            if constexpr (pretty_json)
                serialization->serializeTextJSONPretty(*part, ind, ostr, settings, indent);
            else
                serialization->serializeTextJSON(*part, ind, ostr, settings);
            return;
        }

        ind -= part->size();
    }

    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Index ({}) for text serialization is out of range", row_num);
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr, settings);
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString ostr_str;
    serializeTextImpl(column, row_num, ostr_str, settings);
    writeEscapedString(ostr_str.str(), ostr);
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString ostr_str;
    serializeTextImpl(column, row_num, ostr_str, settings);
    writeQuotedString(ostr_str.str(), ostr);
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr, settings);
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString ostr_str;
    serializeTextImpl(column, row_num, ostr_str, settings);
    writeCSVString(ostr_str.str(), ostr);
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeTextMarkdown(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.markdown.escape_special_characters)
    {
        WriteBufferFromOwnString ostr_str;
        serializeTextImpl(column, row_num, ostr_str, settings);
        writeMarkdownEscapedString(ostr_str.str(), ostr);
    }
    else
    {
        serializeTextEscaped(column, row_num, ostr, settings);
    }
}

template <typename Parser>
void SerializationObjectDeprecated<Parser>::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    const auto & column_object = assert_cast<const ColumnObjectDeprecated &>(column);
    const auto & subcolumns = column_object.getSubcolumns();

    writeCString("{\n", ostr);
    for (auto it = subcolumns.begin(); it != subcolumns.end(); ++it)
    {
        const auto & entry = *it;
        if (it != subcolumns.begin())
            writeCString(",\n", ostr);

        writeChar(' ', (indent + 1) * 4, ostr);
        writeDoubleQuoted(entry->path.getPath(), ostr);
        writeCString(": ", ostr);
        serializeTextFromSubcolumn<true>(entry->data, row_num, ostr, settings, indent + 1);
    }
    writeChar('\n', ostr);
    writeChar(' ', indent * 4, ostr);
    writeChar('}', ostr);
}


SerializationPtr getObjectSerialization(const String & schema_format)
{
    if (schema_format == "json")
    {
#if USE_SIMDJSON
        return std::make_shared<SerializationObjectDeprecated<JSONDataParser<SimdJSONParser>>>();
#elif USE_RAPIDJSON
        return std::make_shared<SerializationObjectDeprecated<JSONDataParser<RapidJSONParser>>>();
#else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "To use data type Object with JSON format ClickHouse should be built with Simdjson or Rapidjson");
#endif
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown schema format '{}'", schema_format);
}

}
