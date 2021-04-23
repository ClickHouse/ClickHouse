#include <DataTypes/Serializations/SerializationObject.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <DataTypes/Serializations/JSONDataParser.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnObject.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
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
            column_object.addSubcolumn(paths[i], std::move(new_column), false);
        }

        column_object.getSubcolumn(paths[i]).insertData(values[i].data(), values[i].size());
    }

    for (auto & [key, subcolumn] : column_object.getSubcolumns())
    {
        if (!paths_set.count(key))
            subcolumn->insertDefault();
    }
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

template <typename Parser>
void SerializationObject<Parser>::serializeBinaryBulk(const IColumn & /*column*/, WriteBuffer & /*ostr*/, size_t /*offset*/, size_t /*limit*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for SerializationObject");
}

template <typename Parser>
void SerializationObject<Parser>::deserializeBinaryBulk(IColumn & /*column*/, ReadBuffer & /*istr*/, size_t /*limit*/, double /*avg_value_size_hint*/) const
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
