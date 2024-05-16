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
#include <base/range.h>

#include <bsoncxx/document/element.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/exception/exception.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
extern const int NOT_IMPLEMENTED;
}


template <typename T>
std::string MongoDBSource::bsonElementAsString(const T & value)
{
    switch (value.type())
    {
        case bsoncxx::type::k_double:
            return std::to_string(value.get_double().value);
        case bsoncxx::type::k_string:
            return static_cast<std::string>(value.get_string().value);
        // MongoDB's documents and arrays may not have strict types or be nested, so the most optimal solution is store their JSON representations.
        case bsoncxx::type::k_document:
            return bsoncxx::to_json(value.get_document(), bsoncxx::ExtendedJsonMode::k_canonical);
        case bsoncxx::type::k_array:
            return bsoncxx::to_json(value.get_array(), bsoncxx::ExtendedJsonMode::k_canonical);
        case bsoncxx::type::k_binary:
            return std::string(reinterpret_cast<const char*>(value.get_binary().bytes), value.get_binary().size);
        case bsoncxx::type::k_undefined:
            return "undefined";
        case bsoncxx::type::k_oid:
            return value.get_oid().value.to_string();
        case bsoncxx::type::k_bool:
            return value.get_bool().value ? "true" : "false";
        case bsoncxx::type::k_date:
            return DateLUT::instance().dateToString(value.get_date().to_int64());
        case bsoncxx::type::k_null:
            return "null";
        case bsoncxx::type::k_regex:
            return bsoncxx::to_json(bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp("regex", value.get_regex().regex), bsoncxx::builder::basic::kvp("options", value.get_regex().options)));
        case bsoncxx::type::k_dbpointer:
            return bsoncxx::to_json(bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp(value.get_dbpointer().value.to_string(), value.get_dbpointer().collection)));
        case bsoncxx::type::k_code:
            return static_cast<std::string>(value.get_code().code);
        case bsoncxx::type::k_symbol:
            return {1, value.get_symbol().symbol.at(0)};
        case bsoncxx::type::k_codewscope:
            return bsoncxx::to_json(bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp(value.get_codewscope().code, value.get_codewscope().scope)));
        case bsoncxx::type::k_int32:
            return std::to_string(static_cast<Int64>(value.get_int32().value));
        case bsoncxx::type::k_timestamp:
            return DateLUT::instance().timeToString(value.get_timestamp().timestamp);
        case bsoncxx::type::k_int64:
            return std::to_string(value.get_int64().value);
        case bsoncxx::type::k_decimal128:
            return value.get_decimal128().value.to_string();
        case bsoncxx::type::k_maxkey:
            return "maxkey";
        case bsoncxx::type::k_minkey:
            return "minkey";
    }
}

Array MongoDBSource::convertMongoDBArray(size_t dimensions, const bsoncxx::types::b_array & array, const DataTypePtr & type, const std::string & name)
{
    auto arr = Array();
    if (dimensions > 0)
    {
        --dimensions;
        for (auto const & elem : array.value)
        {
            if (elem.type() != bsoncxx::type::k_array)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Array {} must have exactly {} dimensions", name, dimensions+1);

            arr.emplace_back(convertMongoDBArray(dimensions, elem.get_array(), type, name));
        }
    }
    else
    {
        for (auto const & value : array.value)
        {
            switch (type->getTypeId())
            {
                case TypeIndex::UInt8:
                {
                    if (value.type() != bsoncxx::type::k_bool)
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected bool, got {} for array {}",
                                        bsoncxx::to_string(value.type()), name);

                    arr.emplace_back(value.get_bool().value);
                    break;
                }
                case TypeIndex::Int32:
                {
                    if (value.type() != bsoncxx::type::k_int32)
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected int32, got {} for array {}",
                                        bsoncxx::to_string(value.type()), name);

                    arr.emplace_back(value.get_int32().value);
                    break;
                }
                case TypeIndex::Int64:
                {
                    if (value.type() != bsoncxx::type::k_int64)
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected int64, got {} for array {}",
                                        bsoncxx::to_string(value.type()), name);

                    arr.emplace_back(value.get_int64().value);
                    break;
                }
                case TypeIndex::Float64:
                {
                    if (value.type() != bsoncxx::type::k_double)
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected double, got {} for array {}",
                                        bsoncxx::to_string(value.type()), name);

                    arr.emplace_back(value.get_double().value);
                    break;
                }
                case TypeIndex::Date:
                {
                    if (value.type() == bsoncxx::type::k_date)
                        arr.emplace_back(DateLUT::instance().toDayNum(value.get_date().to_int64() / 1000));
                    else if (value.type() == bsoncxx::type::k_timestamp)
                        arr.emplace_back(DateLUT::instance().toDayNum(value.get_timestamp().timestamp));
                    else
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date or timestamp, got {} for array {}",
                                        bsoncxx::to_string(value.type()), name);
                    break;
                }
                case TypeIndex::DateTime:
                {
                    if (value.type() == bsoncxx::type::k_date)
                        arr.emplace_back(static_cast<UInt32>(value.get_date().to_int64() / 1000));
                    else if (value.type() == bsoncxx::type::k_timestamp)
                        arr.emplace_back(value.get_timestamp().timestamp);
                    else
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date or timestamp, got {} for array {}",
                                        bsoncxx::to_string(value.type()), name);
                    break;
                }
                case TypeIndex::DateTime64:
                {
                    if (value.type() == bsoncxx::type::k_date)
                        arr.emplace_back(value.get_date().to_int64());
                    else if (value.type() == bsoncxx::type::k_timestamp)
                        arr.emplace_back(static_cast<Int64>(value.get_timestamp().timestamp * 1000));
                    else
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date or timestamp, got {} for array {}",
                                        bsoncxx::to_string(value.type()), name);
                    break;
                }
                case TypeIndex::UUID:
                {
                    if (value.type() != bsoncxx::type::k_string)
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected string (UUID), got {} for array {}",
                                        bsoncxx::to_string(value.type()), name);

                    arr.emplace_back(parse<UUID>(value.get_string().value.data()));
                    break;
                }
                case TypeIndex::String:
                {
                    arr.emplace_back(bsonElementAsString(value));
                    break;
                }
                default:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Array {} has unsupported nested type {}", name, type->getName());
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
        case TypeIndex::UInt8:
        {
            if (value.type() != bsoncxx::type::k_bool)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected bool, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnUInt8 &>(column).insertValue(value.get_bool().value);
            break;
        }
        case TypeIndex::Int32:
        {
            if (value.type() != bsoncxx::type::k_int32)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected int32, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnInt32 &>(column).insertValue(value.get_int32().value);
            break;
        }
        case TypeIndex::Int64:
        {
            if (value.type() != bsoncxx::type::k_int64)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected int64, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnInt64 &>(column).insertValue(value.get_int64().value);
            break;
        }
        case TypeIndex::Float64:
        {
            if (value.type() != bsoncxx::type::k_double)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected double, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnFloat64 &>(column).insertValue(value.get_double().value);
            break;
        }
        case TypeIndex::Date:
        {
            if (value.type() == bsoncxx::type::k_date)
                assert_cast<ColumnUInt16 &>(column).insertValue(DateLUT::instance().toDayNum(value.get_date().to_int64() / 1000));
            else if (value.type() == bsoncxx::type::k_timestamp)
                assert_cast<ColumnUInt16 &>(column).insertValue(DateLUT::instance().toDayNum(value.get_timestamp().timestamp));
            else
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date or timestamp, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);
            break;
        }
        case TypeIndex::DateTime:
        {
            if (value.type() == bsoncxx::type::k_date)
                assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(value.get_date().to_int64() / 1000));
            else if (value.type() == bsoncxx::type::k_timestamp)
                assert_cast<ColumnUInt32 &>(column).insertValue(value.get_timestamp().timestamp);
            else
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date or timestamp, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);
            break;
        }
        case TypeIndex::DateTime64:
        {
            if (value.type() == bsoncxx::type::k_date)
                assert_cast<DB::ColumnDecimal<DB::DateTime64> &>(column).insertValue(value.get_date().to_int64());
            else if (value.type() == bsoncxx::type::k_timestamp)
                assert_cast<DB::ColumnDecimal<DB::DateTime64> &>(column).insertValue(value.get_timestamp().timestamp * 1000);
            else
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected date or timestamp, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);
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
            auto value_string = bsonElementAsString(value);
            assert_cast<ColumnString &>(column).insertData(value_string.data(), value_string.size());
            break;
        }
        case TypeIndex::Array:
        {
            if (value.type() != bsoncxx::type::k_array)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected array, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnArray &>(column).insert(convertMongoDBArray(arrays_info[idx].first, value.get_array(), arrays_info[idx].second, name));
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
    Block & sample_block_,
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
        if (sample_block.getByPosition(idx).type->getTypeId() == TypeIndex::Array)
        {
            auto type = typeid_cast<const DataTypeArray *>(sample_block.getByPosition(idx).type.get())->getNestedType();
            size_t dimensions = 0;
            while (type->getTypeId() == TypeIndex::Array)
            {
                type = typeid_cast<const DataTypeArray *>(type.get())->getNestedType();
                ++dimensions;
            }
            arrays_info[idx] = {std::move(dimensions), std::move(type)};
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
        ++num_rows;

        for (auto idx : collections::range(0, size))
        {
            auto & column = sample_block.getByPosition(idx);
            bsoncxx::document::element value;
            try
            {
                value = doc[column.name];
            } catch (bsoncxx::exception /*&e*/) // required key is not exists in the document
            {
            }

            if (column.type->isNullable()) // column is nullable
            {
                ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                if (!value || value.type() == bsoncxx::type::k_null)
                    column_nullable.insertData(nullptr, 0);
                else
                    insertValue(column_nullable.getNestedColumn(), idx, column.type, column.name, value);
                column_nullable.getNullMapData().emplace_back(0);
            }
            else if (!value || value.type() == bsoncxx::type::k_null)
                insertDefaultValue(*columns[idx], *column.column);
            else
                insertValue(*columns[idx], idx, column.type, column.name, value);
        }

        if (num_rows == max_block_size)
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
