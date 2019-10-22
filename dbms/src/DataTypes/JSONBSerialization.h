#pragma once

#include <Core/Types.h>
#include <ext/scope_guard.h>
#include <rapidjson/reader.h>
#include <IO/ReadBuffer.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/JSONBinaryConverter.h>
#include <Columns/ColumnJSONB.h>
#include <Columns/ColumnString.h>
#include <Columns/JSONBDataMark.h>
#include <rapidjson/writer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_PARSE_JSON;
}

struct JSONBSerialization
{
public:
    template <typename InputStream>
    static void deserialize(bool is_nullable, IColumn & column_, const std::unique_ptr<InputStream> & input_stream)
    {
        auto & column_json_binary = static_cast<ColumnJSONB &>(column_);
        JSONBinaryConverter binary_converter(is_nullable, column_json_binary.getKeysDictionary(), column_json_binary.getRelationsDictionary());

        input_stream->skipQuoted();
        rapidjson::EncodedInputStream<rapidjson::UTF8<>, InputStream> is(*input_stream);
        if (!rapidjson::Reader{}.Parse<rapidjson::kParseStopWhenDoneFlag>(is, binary_converter))
            throw Exception("Cannot parse JSON, invalid JSON characters.", ErrorCodes::CANNOT_PARSE_JSON);

        input_stream->skipQuoted();;
        binary_converter.finalize(column_json_binary.getRelationsBinary(), column_json_binary.getDataBinary());
    }

    template <typename OutputStream>
    static void serialize(const IColumn & column_, size_t row_num, const std::unique_ptr<OutputStream> & output_stream)
    {
        rapidjson::Writer<OutputStream> writer(*output_stream.get());
        toJSONString<rapidjson::Writer<OutputStream>>(*checkAndGetColumn<ColumnJSONB>(column_), row_num, writer);
    }

private:
    template <typename RapidWrite>
    static void toJSONString(const ColumnJSONB & column, size_t row_num, RapidWrite & writer)
    {
        if (!column.isMultipleColumn())
            toJSONString(column.getKeysDictionary(), *checkAndGetColumn<ColumnString>(column.getDataBinary()), row_num, writer, column.isNullable());
    }

    template <typename RapidWrite>
    static void toJSONString(const IColumnUnique & keys, const ColumnString & binary_column, size_t row_num, RapidWrite & writer, bool is_nullable)
    {
        const auto & row_binary_data = binary_column.getDataAt(row_num);
        toJSONString(*checkAndGetColumn<ColumnString>(*keys.getNestedColumn()),
            *fleece::Value::fromData(fleece::slice(row_binary_data.data, row_binary_data.size)), writer, is_nullable);
    }

    template <typename RapidWrite>
    static void toJSONString(const ColumnString & keys, const fleece::Value & fleece_value, RapidWrite & writer, bool is_nullable)
    {
        const auto & toJSONStringForDict = [&](const fleece::Value & dict_fleece_value)
        {
            const auto & dict_value = dict_fleece_value.asDict();

            size_t member_size = dict_value->count();
            fleece::Dict::iterator dict_member_iterator = dict_value->begin();

            writer.StartObject();
            for (size_t index = 0; index < member_size; ++index, ++dict_member_iterator)
            {
                const auto & key = dict_member_iterator.key();
                const auto & string_key = keys.getDataAt(UInt64(key->asInt()) - UInt64(JSONBDataMark::End));
                writer.Key(string_key.data, string_key.size);
                toJSONString(keys, *dict_member_iterator.value(), writer, is_nullable);
            }
            writer.EndObject();
        };

        const auto & toJSONStringForNumber = [&](const fleece::Value & number_fleece_value)
        {
            if (number_fleece_value.isInteger() && !number_fleece_value.isUnsigned())
                writer.Int64(number_fleece_value.asInt());
            else if (number_fleece_value.isInteger() && number_fleece_value.isUnsigned())
                writer.Uint64(number_fleece_value.asUnsigned());
            else if (number_fleece_value.isDouble())
                writer.Double(number_fleece_value.asDouble());
            else
                writer.Double(number_fleece_value.asFloat());
        };

        const auto & toJSONStringForString = [&](const fleece::Value & string_fleece_value)
        {
            const auto & string_value = string_fleece_value.asString();
            writer.String(reinterpret_cast<const char *>(string_value.buf), string_value.size);
        };

        switch (fleece_value.type())
        {
            case fleece::valueType::kDict: toJSONStringForDict(fleece_value); break;
            case fleece::valueType::kNumber: toJSONStringForNumber(fleece_value); break;
            case fleece::valueType::kString: toJSONStringForString(fleece_value); break;
            case fleece::valueType::kBoolean: writer.Bool(fleece_value.asBool()); break;
            case fleece::valueType::kNull:
            {
                if (!is_nullable)
                    throw Exception("Cannot parse NULL, you can use Nullable(JSONB) instead of JSONB.", ErrorCodes::CANNOT_PARSE_JSON);

                writer.Null();
                break;
            }
            case fleece::valueType::kData:
            case fleece::valueType::kArray: throw Exception("Cannot support JSONArray.", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
};

}
