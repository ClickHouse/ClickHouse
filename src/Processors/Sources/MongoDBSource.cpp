#include "config.h"

#if USE_MONGODB
#include "MongoDBSource.h"

#include <vector>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/BSONCXXHelper.h>
#include <base/range.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
extern const int NOT_IMPLEMENTED;
}

using BSONCXXHelper::BSONElementAsNumber;
using BSONCXXHelper::BSONArrayAsArray;
using BSONCXXHelper::BSONElementAsString;

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

            assert_cast<ColumnInt32 &>(column).insertValue(DateLUT::instance().toDayNum(value.get_date().to_int64() / 1000).toUnderType());
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

            assert_cast<ColumnDecimal<DateTime64> &>(column).insertValue(value.get_date().to_int64());
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
            assert_cast<ColumnString &>(column).insert(BSONElementAsString(value, json_format_settings));
            break;
        }
        case TypeIndex::Array:
        {
            if (value.type() != bsoncxx::type::k_array)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected array, got {} for column {}",
                                bsoncxx::to_string(value.type()), name);

            assert_cast<ColumnArray &>(column).insert(BSONArrayAsArray(arrays_info[idx].first, value.get_array(), arrays_info[idx].second.first, arrays_info[idx].second.second, name, json_format_settings));
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

            if (value && value.type() != bsoncxx::type::k_null)
            {
                if (sample_column.type->isNullable())
                {
                    auto & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    const auto & type_nullable = assert_cast<const DataTypeNullable &>(*sample_column.type);

                    insertValue(column_nullable.getNestedColumn(), idx, type_nullable.getNestedType(), sample_column.name, value);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                    insertValue(*columns[idx], idx, sample_column.type, sample_column.name, value);
            }
            else
                insertDefaultValue(*columns[idx], *sample_column.column);
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
