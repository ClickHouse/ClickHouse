#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/JSONDataParser.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/HashTable/HashSet.h>
#include <Columns/ColumnObject.h>

#include <Common/FieldVisitorToString.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

using Node = typename ColumnObject::Subcolumns::Node;

/// Finds a subcolumn from the same Nested type as @entry and inserts
/// an array with default values with consistent sizes as in Nested type.
bool tryInsertDefaultFromNested(
    const std::shared_ptr<Node> & entry, const ColumnObject::Subcolumns & subcolumns)
{
    if (!entry->path.hasNested())
        return false;

    const Node * current_node = subcolumns.findLeaf(entry->path);
    const Node * leaf = nullptr;
    size_t num_skipped_nested = 0;

    while (current_node)
    {
        /// Try to find the first Nested up to the current node.
        const auto * node_nested = subcolumns.findParent(current_node,
            [](const auto & candidate) { return candidate.isNested(); });

        if (!node_nested)
            break;

        /// If there are no leaves, skip current node and find
        /// the next node up to the current.
        leaf = subcolumns.findLeaf(node_nested,
            [&](const auto & candidate)
            {
                return candidate.data.size() == entry->data.size() + 1;
            });

        if (leaf)
            break;

        current_node = node_nested->parent;
        ++num_skipped_nested;
    }

    if (!leaf)
        return false;

    auto last_field = leaf->data.getLastField();
    if (last_field.isNull())
        return false;

    const auto & least_common_type = entry->data.getLeastCommonType();
    size_t num_dimensions = getNumberOfDimensions(*least_common_type);
    assert(num_skipped_nested < num_dimensions);

    /// Replace scalars to default values with consistent array sizes.
    size_t num_dimensions_to_keep = num_dimensions - num_skipped_nested;
    auto default_scalar = num_skipped_nested
        ? createEmptyArrayField(num_skipped_nested)
        : getBaseTypeOfArray(least_common_type)->getDefault();

    auto default_field = applyVisitor(FieldVisitorReplaceScalars(default_scalar, num_dimensions_to_keep), last_field);
    entry->data.insert(std::move(default_field));
    return true;
}

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
            bool inserted = tryInsertDefaultFromNested(entry, subcolumns);
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
template <typename TSettings, typename TStatePtr>
void SerializationObject<Parser>::checkSerializationIsSupported(const TSettings & settings, const TStatePtr & state) const
{
    if (settings.position_independent_encoding)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject doesn't support serialization with position independent encoding");

    if (state)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject doesn't support serialization with non-trivial state");
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings, state);
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings, state);
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings, state);
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    checkSerializationIsSupported(settings, state);
    const auto & column_object = assert_cast<const ColumnObject &>(column);

    if (!column_object.isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write non-finalized ColumnObject");

    settings.path.push_back(Substream::ObjectStructure);
    if (auto * stream = settings.getter(settings.path))
        writeVarUInt(column_object.getSubcolumns().size(), *stream);

    const auto & subcolumns = column_object.getSubcolumns();
    for (const auto & entry : subcolumns)
    {
        settings.path.back() = Substream::ObjectStructure;
        settings.path.back().object_key_name = entry->path.getPath();

        const auto & type = entry->data.getLeastCommonType();
        if (auto * stream = settings.getter(settings.path))
        {
            entry->path.writeBinary(*stream);
            writeStringBinary(type->getName(), *stream);
        }

        settings.path.back() = Substream::ObjectElement;
        if (auto * stream = settings.getter(settings.path))
        {
            auto serialization = type->getDefaultSerialization();
            serialization->serializeBinaryBulkWithMultipleStreams(
                entry->data.getFinalizedColumn(), offset, limit, settings, state);
        }
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
    checkSerializationIsSupported(settings, state);
    if (!column->empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject cannot be deserialized to non-empty column");

    auto mutable_column = column->assumeMutable();
    auto & column_object = typeid_cast<ColumnObject &>(*mutable_column);

    size_t num_subcolumns = 0;
    settings.path.push_back(Substream::ObjectStructure);
    if (auto * stream = settings.getter(settings.path))
        readVarUInt(num_subcolumns, *stream);

    settings.path.back() = Substream::ObjectElement;
    for (size_t i = 0; i < num_subcolumns; ++i)
    {
        PathInData key;
        String type_name;

        settings.path.back() = Substream::ObjectStructure;
        if (auto * stream = settings.getter(settings.path))
        {
            key.readBinary(*stream);
            readStringBinary(type_name, *stream);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read structure of DataTypeObject, because its stream is missing");
        }

        settings.path.back() = Substream::ObjectElement;
        settings.path.back().object_key_name = key.getPath();

        if (auto * stream = settings.getter(settings.path))
        {
            auto type = DataTypeFactory::instance().get(type_name);
            auto serialization = type->getDefaultSerialization();
            ColumnPtr subcolumn_data = type->createColumn();
            serialization->deserializeBinaryBulkWithMultipleStreams(subcolumn_data, limit, settings, state, cache);
            column_object.addSubcolumn(key, subcolumn_data->assumeMutable());
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read subcolumn '{}' of DataTypeObject, because its stream is missing", key.getPath());
        }
    }

    settings.path.pop_back();
    column_object.checkConsistency();
    column_object.finalize();
    column = std::move(mutable_column);
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
