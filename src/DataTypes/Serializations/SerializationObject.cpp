#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/JSONDataParser.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/HashTable/HashSet.h>
#include <Columns/ColumnObject.h>

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
    extern const int TYPE_MISMATCH;
}

template <typename Parser>
template <typename Reader>
void SerializationObject<Parser>::deserializeTextImpl(IColumn & column, Reader && reader) const
{
    auto & column_object = assert_cast<ColumnObject &>(column);

    String buf;
    reader(buf);

    auto result = parser.parse(buf.data(), buf.size());
    if (!result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse object");

    auto && [paths, values] = *result;
    assert(paths.size() == values.size());

    HashSet<StringRef, StringRefHash> paths_set;
    for (const auto & path : paths)
        paths_set.insert(path);

    if (paths.size() != paths_set.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Object has ambiguous paths");

    size_t column_size = column_object.size();
    for (size_t i = 0; i < paths.size(); ++i)
    {
        if (!column_object.hasSubcolumn(paths[i]))
            column_object.addSubcolumn(paths[i], column_size);

        auto & subcolumn = column_object.getSubcolumn(paths[i]);
        assert(subcolumn.size() == column_size);

        subcolumn.insert(std::move(values[i]));
    }

    for (auto & [key, subcolumn] : column_object.getSubcolumns())
    {
        if (!paths_set.has(key))
            subcolumn.insertDefault();
    }
}

template <typename Parser>
void SerializationObject<Parser>::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { readStringInto(s, istr); });
}

template <typename Parser>
void SerializationObject<Parser>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { readEscapedStringInto(s, istr); });
}

template <typename Parser>
void SerializationObject<Parser>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { readQuotedStringInto<true>(s, istr); });
}

template <typename Parser>
void SerializationObject<Parser>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeTextImpl(column, [&](String & s) { parser.readJSON(s, istr); });
}

template <typename Parser>
void SerializationObject<Parser>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, [&](String & s) { readCSVStringInto(s, istr, settings.csv); });
}

template <typename Parser>
template <typename Settings, typename StatePtr>
void SerializationObject<Parser>::checkSerializationIsSupported(Settings & settings, StatePtr & state) const
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

    for (const auto & [key, subcolumn] : column_object.getSubcolumns())
    {
        settings.path.back() = Substream::ObjectStructure;
        settings.path.back().object_key_name = key;

        auto type = getDataTypeByColumn(subcolumn.getFinalizedColumn());
        if (auto * stream = settings.getter(settings.path))
        {
            writeStringBinary(key, *stream);
            writeStringBinary(type->getName(), *stream);
        }

        settings.path.back() = Substream::ObjectElement;
        settings.path.back().object_key_name = key;

        if (auto * stream = settings.getter(settings.path))
        {
            auto serialization = type->getDefaultSerialization();
            serialization->serializeBinaryBulkWithMultipleStreams(
                subcolumn.getFinalizedColumn(), offset, limit, settings, state);
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
        String key;
        String type_name;

        settings.path.back() = Substream::ObjectStructure;
        if (auto * stream = settings.getter(settings.path))
        {
            readStringBinary(key, *stream);
            readStringBinary(type_name, *stream);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read structure of DataTypeObject, because its stream is missing");
        }

        settings.path.back() = Substream::ObjectElement;
        settings.path.back().object_key_name = key;

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
                "Cannot read subcolumn '{}' of DataTypeObject, because its stream is missing", key);
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

template <typename Parser>
void SerializationObject<Parser>::serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::serializeTextEscaped(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::serializeTextQuoted(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::serializeTextJSON(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::serializeTextCSV(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
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
            "To use data type Object with JSON format, ClickHouse should be built with Simdjson or Rapidjson");
#endif
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown schema format '{}'", schema_format);
}

}
