#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/JSONDataParser.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/ObjectUtils.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/FieldVisitors.h>
#include <Columns/ColumnString.h>
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
}

template <typename Parser>
void SerializationObject<Parser>::serializeText(const IColumn & /*column*/, size_t /*row_num*/, WriteBuffer & /*ostr*/, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    auto & column_object = assert_cast<ColumnObject &>(column);

    String buf;
    parser.readInto(buf, istr);
    std::cerr << "buf: " << buf << "\n";
    auto result = parser.parse(buf.data(), buf.size());
    if (!result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse object");

    const auto & [paths, values] = *result;
    assert(paths.size() == values.size());

    NameSet paths_set(paths.begin(), paths.end());

    if (paths.size() != paths_set.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Object has ambiguous paths");

    size_t column_size = column_object.size();
    for (size_t i = 0; i < paths.size(); ++i)
    {
        if (!column_object.hasSubcolumn(paths[i]))
        {
            auto new_column = ColumnString::create()->cloneResized(column_size);
            column_object.addSubcolumn(paths[i], std::move(new_column));
        }

        column_object.getSubcolumn(paths[i]).insertData(values[i].data(), values[i].size());
    }

    for (auto & [key, subcolumn] : column_object.getSubcolumns())
    {
        if (!paths_set.count(key))
            subcolumn->insertDefault();
    }
}

namespace
{

struct SerializeStateObject : public ISerialization::SerializeBinaryBulkState
{
    std::unordered_map<String, ISerialization::SerializeBinaryBulkStatePtr> states;
};

struct DeserializeStateObject : public ISerialization::DeserializeBinaryBulkState
{
    std::unordered_map<String, ISerialization::DeserializeBinaryBulkStatePtr> states;
};

}

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr &) const
{
    if (settings.position_independent_encoding)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject cannot be serialized with position independent encoding");
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    UNUSED(settings);
    UNUSED(state);
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr &) const
{
    if (settings.position_independent_encoding)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DataTypeObject cannot be deserialized with position independent encoding");
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const auto & column_object = assert_cast<const ColumnObject &>(column);
    assert(!settings.position_independent_encoding);

    settings.path.push_back(Substream::ObjectStructure);
    if (auto * stream = settings.getter(settings.path))
        writeVarUInt(column_object.getSubcolumns().size(), *stream);

    settings.path.back() = Substream::ObjectElement;
    for (const auto & [key, subcolumn] : column_object.getSubcolumns())
    {
        settings.path.back().object_key_name = key;

        settings.path.back() = Substream::ObjectStructure;
        if (auto * stream = settings.getter(settings.path))
        {
            auto type = getDataTypeByColumn(*subcolumn);
            writeStringBinary(key, *stream);
            writeStringBinary(type->getName(), *stream);
        }

        settings.path.back() = Substream::ObjectElement;
        settings.path.back().object_key_name = key;

        if (auto * stream = settings.getter(settings.path))
        {
            auto type = getDataTypeByColumn(*subcolumn);
            auto serialization = type->getDefaultSerialization();
            serialization->serializeBinaryBulkWithMultipleStreams(*subcolumn, offset, limit, settings, state);
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
            ColumnPtr subcolumn = type->createColumn();
            serialization->deserializeBinaryBulkWithMultipleStreams(subcolumn, limit, settings, state, cache);
            column_object.addSubcolumn(key, subcolumn->assumeMutable());
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read subcolumn '{}' of DataTypeObject, because its stream is missing", key);
        }
    }

    settings.path.pop_back();
    column_object.checkConsistency();
    column = std::move(mutable_column);
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinary(const Field & /*field*/, WriteBuffer & /*ostr*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinary(Field & /*field*/, ReadBuffer & /*istr*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::serializeBinary(const IColumn & /*column*/, size_t /*row_num*/, WriteBuffer & /*ostr*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinary(IColumn & /*column*/, ReadBuffer & /*istr*/) const
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

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknow schema_format '{}'", schema_format);
}

}
