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
extern const int UNKNOWN_TYPE;
extern const int MONGODB_ERROR;
extern const int BAD_ARGUMENTS;
}

namespace
{

using ValueType = ExternalResultDescription::ValueType;
void insertValue(
    IColumn & column,
    const ValueType & type,
    const bsoncxx::document::element & value,
    const std::string & name)
{
    switch (type)
    {
        case ValueType::vtUInt8:
            if (value.type() != bsoncxx::type::k_bool)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected bool, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

        assert_cast<ColumnUInt8 &>(column).insertValue(value.get_bool());
        break;
        case ValueType::vtInt32:
            if (value.type() != bsoncxx::type::k_int32)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected int32, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

        assert_cast<ColumnInt32 &>(column).insertValue(value.get_int32());
        break;
        case ValueType::vtInt64:
            if (value.type() != bsoncxx::type::k_int64)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected int64, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

        assert_cast<ColumnInt64 &>(column).insertValue(value.get_int64());
        break;
        case ValueType::vtFloat64:
            if (value.type() != bsoncxx::type::k_double)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected double, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

        assert_cast<ColumnFloat64 &>(column).insertValue(value.get_double());
        break;
        case ValueType::vtDate:
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
        case ValueType::vtDateTime:
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
        case ValueType::vtDateTime64:
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
        case ValueType::vtUUID:
        {
            if (value.type() != bsoncxx::type::k_string)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected string (UUID), got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnUUID &>(column).insertValue(parse<UUID>(value.get_string().value.data()));
            break;
        }
        case ValueType::vtString:
        {
            // MongoDB's documents and arrays may not have strict types or be nested, so the most optimal solution is store their JSON representations.
            if (value.type() == bsoncxx::type::k_string)
            {
                auto value_string = value.get_string().value;
                assert_cast<ColumnString &>(column).insertData(value_string.data(), value_string.size());
            }
            else if (value.type() == bsoncxx::type::k_document)
            {
                auto value_string = bsoncxx::to_json(value.get_document(), bsoncxx::ExtendedJsonMode::k_canonical);
                assert_cast<ColumnString &>(column).insertData(value_string.data(), value_string.size());
            }
            else if (value.type() == bsoncxx::type::k_array)
            {
                auto value_string = bsoncxx::to_json(value.get_array(), bsoncxx::ExtendedJsonMode::k_canonical);
                assert_cast<ColumnString &>(column).insertData(value_string.data(), value_string.size());
            }
            else if (value.type() == bsoncxx::type::k_oid)
            {
                auto value_string = value.get_oid().value.to_string();
                assert_cast<ColumnString &>(column).insertData(value_string.data(), value_string.size());
            }
            else
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected one of (string, document, array, oid), got {} for column {}",
                                bsoncxx::to_string(value.type()), name);
            break;
        }
        default: // TODO: arrays
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Column {} has unsupported type", column.getName());
    }
}

void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }
}


MongoDBSource::MongoDBSource(
    const mongocxx::uri & uri,
    const std::string & collection_name,
    const bsoncxx::document::view_or_value & query,
    const mongocxx::options::find & options,
    Block & header_,
    const UInt64 & max_block_size_)
    : ISource{header_}
    , client{uri}
    , database{client.database(uri.database())}
    , collection{database.collection(collection_name)}
    , cursor{collection.find(query, options)}
    , header{header_}
    , max_block_size{max_block_size_}
    , description{header}
{
}


MongoDBSource::~MongoDBSource() = default;

Chunk MongoDBSource::generate()
{
    if (all_read)
        return {};

    MutableColumns columns(description.sample_block.columns());
    const size_t size = columns.size();

    for (const auto i : collections::range(0, size))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    size_t num_rows = 0;
    for (const auto & doc : cursor)
    {
        ++num_rows;

        for (const auto idx : collections::range(0, size))
        {
            const auto & name = description.sample_block.getByPosition(idx).name;
            bsoncxx::document::element value;
            try
            {
                value = doc[name];
            } catch (bsoncxx::exception /*&e*/) // required key is not exists in the document
            {
            }

            if (description.types[idx].second) // column is nullable
            {
                ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                if (!value || value.type() == bsoncxx::type::k_null)
                    column_nullable.insertData(nullptr, 0);
                else
                    insertValue(column_nullable.getNestedColumn(), description.types[idx].first, value, name);
                column_nullable.getNullMapData().emplace_back(0);
            }
            else if (!value || value.type() == bsoncxx::type::k_null)
                insertDefaultValue(*columns[idx], *description.sample_block.getByPosition(idx).column);
            else
                insertValue(*columns[idx], description.types[idx].first, value, name);
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
