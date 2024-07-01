#include "config.h"

#if USE_MONGODB
#include "MongoDBSource.h"

#include <string>
#include <vector>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/Exception.h>
#include <Common/Base64.h>
#include <base/range.h>

#include <bsoncxx/document/element.hpp>
#include <bsoncxx/json.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
extern const int NOT_IMPLEMENTED;
}

std::string escapeJSONString(const std::string & input)
{
    std::string escaped;
    escaped.reserve(input.length());

    for (size_t i = 0; i < input.length(); ++i)
    {
        switch (input[i])
        {
            case '"':
                escaped += "\\\"";
                break;
            case '/':
                escaped += "\\/";
                break;
            case '\\':
                escaped += "\\\\";
                break;
            case '\f':
                escaped += "\\f";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            case '\b':
                escaped += "\\b";
                break;
            default:
                escaped += input[i];
                break;
        }
    }

    return escaped;
}

template <typename T>
std::string BSONElementAsString(const T & value)
{
    switch (value.type())
    {
        case bsoncxx::type::k_double:
            return std::to_string(value.get_double().value);
        case bsoncxx::type::k_string:
            return static_cast<std::string>(value.get_string().value);
        // MongoDB's documents and arrays may not have strict types or be nested, so the most optimal solution is store their JSON representations.
        // bsoncxx::to_json function will return something like "'number': {'$numberInt': '321'}", this why we need own realization.
        case bsoncxx::type::k_document:
        {
            String json = "{";
            auto first = true;

            for (const auto & elem : value.get_document().view())
            {
                if (!first)
                    json += ',';
                json += '"' + escapeJSONString(std::string(elem.key())) + "\":";
                switch (elem.type())
                {
                    // types which need to be quoted
                    case bsoncxx::type::k_binary:
                    case bsoncxx::type::k_date:
                    case bsoncxx::type::k_timestamp:
                    case bsoncxx::type::k_oid:
                    case bsoncxx::type::k_symbol:
                    case bsoncxx::type::k_maxkey:
                    case bsoncxx::type::k_minkey:
                    case bsoncxx::type::k_undefined:
                        json += "\"" + BSONElementAsString<bsoncxx::document::element>(elem) + "\"";
                        break;
                    // types which need to be escaped
                    case bsoncxx::type::k_string:
                    case bsoncxx::type::k_code:
                        json += "\"" + escapeJSONString(BSONElementAsString<bsoncxx::document::element>(elem)) + "\"";
                        break;
                    default:
                            json += BSONElementAsString<bsoncxx::document::element>(elem);
                }
                first = false;
            }

            json += '}';
            return json;
        }
        case bsoncxx::type::k_array:
        {
            String json = "[";
            auto first = true;

            for (const auto & elem : value.get_array().value)
            {
                if (!first)
                    json += ',';
                switch (elem.type())
                {
                    // types which need to be quoted
                    case bsoncxx::type::k_binary:
                    case bsoncxx::type::k_date:
                    case bsoncxx::type::k_timestamp:
                    case bsoncxx::type::k_oid:
                    case bsoncxx::type::k_symbol:
                    case bsoncxx::type::k_maxkey:
                    case bsoncxx::type::k_minkey:
                    case bsoncxx::type::k_undefined:
                        json += "\"" + BSONElementAsString<bsoncxx::array::element>(elem) + "\"";
                        break;
                    // types which need to be escaped
                    case bsoncxx::type::k_string:
                    case bsoncxx::type::k_code:
                        json += "\"" + escapeJSONString(BSONElementAsString<bsoncxx::array::element>(elem)) + "\"";
                        break;
                    default:
                        json += BSONElementAsString<bsoncxx::array::element>(elem);
                }
                first = false;
            }

            json += ']';
            return json;
        }
        case bsoncxx::type::k_binary:
            return base64Encode(std::string(reinterpret_cast<const char*>(value.get_binary().bytes), value.get_binary().size));
        case bsoncxx::type::k_undefined:
            return "undefined";
        case bsoncxx::type::k_oid:
            return value.get_oid().value.to_string();
        case bsoncxx::type::k_bool:
            return value.get_bool().value ? "true" : "false";
        case bsoncxx::type::k_date:
            return DateLUT::instance().timeToString(value.get_date().to_int64() / 1000);
        case bsoncxx::type::k_null:
            return "null";
        case bsoncxx::type::k_regex:
            return R"({"regex": ")" + escapeJSONString(std::string(value.get_regex().regex)) + R"(","options":")" + escapeJSONString(std::string(value.get_regex().regex)) + "\"}";
        case bsoncxx::type::k_dbpointer:
            return "{\"" + escapeJSONString(value.get_dbpointer().value.to_string()) + "\":\"" + escapeJSONString(std::string(value.get_dbpointer().collection)) + "\"}";
        case bsoncxx::type::k_symbol:
            return {1, value.get_symbol().symbol.at(0)};
        case bsoncxx::type::k_int32:
            return std::to_string(static_cast<Int64>(value.get_int32().value));
        case bsoncxx::type::k_timestamp:
            return DateLUT::instance().timeToString(value.get_timestamp().timestamp);
        case bsoncxx::type::k_int64:
            return std::to_string(value.get_int64().value);
        case bsoncxx::type::k_decimal128:
            return value.get_decimal128().value.to_string();
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BSON type {} is unserializable", to_string(value.type()));
    }
}

template <typename T, typename T2>
T BSONElementAsNumber(const T2 & value, const std::string & name)
{
    switch (value.type())
    {
        case bsoncxx::type::k_int64:
            return static_cast<T>(value.get_int64());
        case bsoncxx::type::k_int32:
            return static_cast<T>(value.get_int32());
        case bsoncxx::type::k_double:
            return static_cast<T>(value.get_double());
        case bsoncxx::type::k_bool:
            return static_cast<T>(value.get_bool());
        default:
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, {} cannot be converted to number for column {}",
                            bsoncxx::to_string(value.type()), name);
    }
}

Array BSONArrayAsArray(size_t dimensions, const bsoncxx::types::b_array & array, const DataTypePtr & type, const Field & default_value, const std::string & name)
{
    auto arr = Array();
    if (dimensions > 0)
    {
        --dimensions;
        for (auto const & elem : array.value)
        {
            if (elem.type() != bsoncxx::type::k_array)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Array {} have less dimensions then defined in the schema", name);

            arr.emplace_back(BSONArrayAsArray(dimensions, elem.get_array(), type, default_value, name));
        }
    }
    else
    {
        for (auto const & value : array.value)
        {
            if (value.type() == bsoncxx::type::k_null)
                arr.emplace_back(default_value);
            else
            {
                switch (type->getTypeId())
                {
                    case TypeIndex::Int8:
                        arr.emplace_back(BSONElementAsNumber<Int8, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt8:
                        arr.emplace_back(BSONElementAsNumber<UInt8, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int16:
                        arr.emplace_back(BSONElementAsNumber<Int16, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt16:
                        arr.emplace_back(BSONElementAsNumber<UInt16, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int32:
                        arr.emplace_back(BSONElementAsNumber<Int32, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt32:
                        arr.emplace_back(BSONElementAsNumber<UInt32, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int64:
                        arr.emplace_back(BSONElementAsNumber<Int64, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt64:
                        arr.emplace_back(BSONElementAsNumber<UInt64, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int128:
                        arr.emplace_back(BSONElementAsNumber<Int128, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt128:
                        arr.emplace_back(BSONElementAsNumber<UInt128, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int256:
                        arr.emplace_back(BSONElementAsNumber<Int256, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt256:
                        arr.emplace_back(BSONElementAsNumber<UInt256, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Float32:
                        arr.emplace_back(BSONElementAsNumber<Float32, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Float64:
                        arr.emplace_back(BSONElementAsNumber<Float64, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Date:
                    {
                        if (value.type() != bsoncxx::type::k_date)
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date, got {} for column {}",
                                            bsoncxx::to_string(value.type()), name);

                        arr.emplace_back(DateLUT::instance().toDayNum(value.get_date().to_int64() / 1000).toUnderType());
                        break;
                    }
                    case TypeIndex::Date32:
                    {
                        if (value.type() != bsoncxx::type::k_date)
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date, got {} for column {}",
                                            bsoncxx::to_string(value.type()), name);

                        arr.emplace_back(static_cast<Int32>(DateLUT::instance().toDayNum(value.get_date().to_int64() / 1000).toUnderType()));
                        break;
                    }
                    case TypeIndex::DateTime:
                    {
                        if (value.type() != bsoncxx::type::k_date)
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date, got {} for column {}",
                                            bsoncxx::to_string(value.type()), name);

                        arr.emplace_back(static_cast<UInt32>(value.get_date().to_int64() / 1000));
                        break;
                    }
                    case TypeIndex::DateTime64:
                    {
                        if (value.type() != bsoncxx::type::k_date)
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date, got {} for column {}",
                                            bsoncxx::to_string(value.type()), name);

                        arr.emplace_back(static_cast<Decimal64>(value.get_date().to_int64()));
                        break;
                    }
                    case TypeIndex::UUID:
                    {
                        if (value.type() != bsoncxx::type::k_string)
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected string (UUID), got {} for column {}",
                                            bsoncxx::to_string(value.type()), name);

                        arr.emplace_back(parse<UUID>(value.get_string().value.data()));
                        break;
                    }
                    case TypeIndex::String:
                        arr.emplace_back(BSONElementAsString(value));
                        break;
                    default:
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Array {} has unsupported nested type {}", name, type->getName());
                }
            }
        }
    }
    return arr;
}

void MongoDBSource::insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }

void MongoDBSource::insertValue(IColumn & column, const size_t & idx, const DataTypePtr & type, const std::string & name, const bsoncxx::document::element & value)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Int8:
            assert_cast<ColumnInt8 &>(column).insertValue(BSONElementAsNumber<Int8, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::UInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(BSONElementAsNumber<UInt8, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::Int16:
            assert_cast<ColumnInt16 &>(column).insertValue(BSONElementAsNumber<Int16, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::UInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(BSONElementAsNumber<UInt16, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::Int32:
            assert_cast<ColumnInt32 &>(column).insertValue(BSONElementAsNumber<Int32, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::UInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(BSONElementAsNumber<UInt32, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::Int64:
            assert_cast<ColumnInt64 &>(column).insertValue(BSONElementAsNumber<Int64, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::UInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(BSONElementAsNumber<UInt64, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::Int128:
            assert_cast<ColumnInt128 &>(column).insertValue(BSONElementAsNumber<Int128, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::UInt128:
            assert_cast<ColumnUInt128 &>(column).insertValue(BSONElementAsNumber<UInt128, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::Int256:
            assert_cast<ColumnInt256 &>(column).insertValue(BSONElementAsNumber<Int256, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::UInt256:
            assert_cast<ColumnUInt256 &>(column).insertValue(BSONElementAsNumber<UInt256, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::Float32:
            assert_cast<ColumnFloat32 &>(column).insertValue(BSONElementAsNumber<Float32, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::Float64:
            assert_cast<ColumnFloat64 &>(column).insertValue(BSONElementAsNumber<Float64, bsoncxx::document::element>(value, name));
            break;
        case TypeIndex::Date:
        {
            if (value.type() != bsoncxx::type::k_date)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnUInt16 &>(column).insertValue(DateLUT::instance().toDayNum(value.get_date().to_int64() / 1000).toUnderType());
            break;
        }
        case TypeIndex::Date32:
        {
            if (value.type() != bsoncxx::type::k_date)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnInt32 &>(column).insertValue(static_cast<Int32>(DateLUT::instance().toDayNum(value.get_date().to_int64() / 1000).toUnderType()));
            break;
        }
        case TypeIndex::DateTime:
        {
            if (value.type() != bsoncxx::type::k_date)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(value.get_date().to_int64() / 1000));
            break;
        }
        case TypeIndex::DateTime64:
        {
            if (value.type() != bsoncxx::type::k_date)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<DB::ColumnDecimal<DB::DateTime64> &>(column).insertValue(static_cast<Decimal64>(value.get_date().to_int64()));
            break;
        }
        case TypeIndex::UUID:
        {
            if (value.type() != bsoncxx::type::k_string)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected string (UUID), got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnUUID &>(column).insertValue(parse<UUID>(value.get_string().value.data()));
            break;
        }
        case TypeIndex::String:
        {
            assert_cast<ColumnString &>(column).insert(BSONElementAsString(value));
            break;
        }
        case TypeIndex::Array:
        {
            if (value.type() != bsoncxx::type::k_array)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected array, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnArray &>(column).insert(BSONArrayAsArray(arrays_info[idx].first, value.get_array(), arrays_info[idx].second.first, arrays_info[idx].second.second, name));
            break;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Column {} has unsupported type {}", name, type->getName());
    }
}


MongoDBSource::MongoDBSource(
    const mongocxx::uri & uri,
    const std::string & collection_name,
    const bsoncxx::document::view_or_value & query,
    const mongocxx::options::find & options,
    const Block & sample_block_,
    const UInt64 & max_block_size_)
    : ISource{sample_block_}
    , client{uri}
    , database{client.database(uri.database())}
    , collection{database.collection(collection_name)}
    , cursor{collection.find(query, options)}
    , sample_block{sample_block_}
    , max_block_size{max_block_size_}
{
    for (const auto & idx : collections::range(0, sample_block.columns()))
    {
        auto & sample_column = sample_block.getByPosition(idx);

        /// If default value for column was not provided, use default from data type.
        if (sample_column.column->empty())
            sample_column.column = sample_column.type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();

        if (sample_column.type->getTypeId() == TypeIndex::Array)
        {
            auto type = assert_cast<const DataTypeArray &>(*sample_column.type).getNestedType();
            size_t dimensions = 0;
            while (type->getTypeId() == TypeIndex::Array)
            {
                type = assert_cast<const DataTypeArray &>(*type).getNestedType();
                ++dimensions;
            }
            if (type->isNullable())
            {
                type = assert_cast<const DataTypeNullable &>(*type).getNestedType();
                arrays_info[idx] = {std::move(dimensions), {std::move(type), Null()}};
            }
            else
                arrays_info[idx] = {std::move(dimensions), {std::move(type), type->getDefault()}};
        }
    }
}


MongoDBSource::~MongoDBSource() = default;

Chunk MongoDBSource::generate()
{
    if (all_read)
        return {};

    auto columns = sample_block.cloneEmptyColumns();
    size_t size = columns.size();

    size_t num_rows = 0;
    for (const auto & doc : cursor)
    {
        for (auto idx : collections::range(0, size))
        {
            auto & sample_column = sample_block.getByPosition(idx);
            auto value = doc[sample_column.name];

            if (!value || value.type() == bsoncxx::type::k_null)
            {
                insertDefaultValue(*columns[idx], *sample_column.column);

                if (sample_column.type->isNullable())
                    assert_cast<ColumnNullable &>(*columns[idx]).getNullMapData().back() = true;
            }
            else
            {
                if (sample_column.type->isNullable())
                {
                    auto & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    const auto & type_nullable = assert_cast<const DataTypeNullable &>(*sample_column.type);

                    insertValue(column_nullable.getNestedColumn(), idx, type_nullable.getNestedType(), sample_column.name, value);
                    column_nullable.getNullMapData().emplace_back(false);
                }
                else
                    insertValue(*columns[idx], idx, sample_column.type, sample_column.name, value);
            }
        }

        if (++num_rows == max_block_size)
            break;
    }
    if (num_rows < max_block_size)
        all_read = true;

    if (num_rows == 0)
        return {};

    return Chunk(std::move(columns), std::move(num_rows));
}

}
#endif
