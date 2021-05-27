#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/JSONDataParser.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/ObjectUtils.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/FieldVisitors.h>
#include <Columns/ColumnObject.h>
#include <Interpreters/convertFieldToType.h>

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

namespace
{
class FieldVisitorReplaceNull : public StaticVisitor<Field>
{
public:
    [[maybe_unused]] explicit FieldVisitorReplaceNull(const Field & replacement_)
        : replacement(replacement_)
    {
    }

    Field operator() (const Null &) const { return replacement; }

    template <typename T>
    Field operator() (const T & x) const
    {
        if constexpr (std::is_base_of_v<FieldVector, T>)
        {
            const size_t size = x.size();
            T res(size);
            for (size_t i = 0; i < size; ++i)
                res[i] = applyVisitor(*this, x[i]);
            return res;
        }
        else
            return x;
    }

private:
    Field replacement;
};

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

    const auto & [paths, values] = *result;
    assert(paths.size() == values.size());

    NameSet paths_set(paths.begin(), paths.end());

    if (paths.size() != paths_set.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Object has ambiguous paths");

    size_t column_size = column_object.size();
    for (size_t i = 0; i < paths.size(); ++i)
    {
        Field value = std::move(values[i]);

        auto value_type = applyVisitor(FieldToDataType(/*allow_conversion_to_string=*/ true), value);
        auto value_dim = getNumberOfDimensions(*value_type);
        auto base_type = getBaseTypeOfArray(value_type);

        if (base_type->isNullable())
        {
            base_type = removeNullable(base_type);
            auto default_field = isNothing(base_type) ? Field(String()) : base_type->getDefault();
            value = applyVisitor(FieldVisitorReplaceNull(default_field), value);
            value_type = createArrayOfType(base_type, value_dim);
        }

        auto array_type = createArrayOfType(std::make_shared<DataTypeString>(), value_dim);
        auto converted_value = isNothing(base_type)
            ? std::move(value)
            : convertFieldToTypeOrThrow(value, *array_type);

        if (!column_object.hasSubcolumn(paths[i]))
            column_object.addSubcolumn(paths[i], array_type->createColumn(), column_size);

        auto & subcolumn = column_object.getSubcolumn(paths[i]);
        size_t column_dim = getNumberOfDimensions(*subcolumn.data);

        if (value_dim != column_dim)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Dimension of types mismatched beetwen inserted value and column at key '{}'. "
                "Dimension of value: {}. Dimension of column: {}",
                paths[i], value_dim, column_dim);

        subcolumn.insert(converted_value, value_type);
    }

    for (auto & [key, subcolumn] : column_object.getSubcolumns())
    {
        if (!paths_set.count(key))
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

    settings.path.push_back(Substream::ObjectStructure);
    if (auto * stream = settings.getter(settings.path))
        writeVarUInt(column_object.getSubcolumns().size(), *stream);

    for (const auto & [key, subcolumn] : column_object.getSubcolumns())
    {
        settings.path.back() = Substream::ObjectStructure;
        settings.path.back().object_key_name = key;

        if (auto * stream = settings.getter(settings.path))
        {
            auto type = getDataTypeByColumn(*subcolumn.data);
            writeStringBinary(key, *stream);
            writeStringBinary(type->getName(), *stream);
        }

        settings.path.back() = Substream::ObjectElement;
        settings.path.back().object_key_name = key;

        if (auto * stream = settings.getter(settings.path))
        {
            auto type = getDataTypeByColumn(*subcolumn.data);
            auto serialization = type->getDefaultSerialization();
            serialization->serializeBinaryBulkWithMultipleStreams(*subcolumn.data, offset, limit, settings, state);
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
    column_object.optimizeTypesOfSubcolumns();
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
