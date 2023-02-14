#include "DuckDBSource.h"

#if USE_DUCKDB
#include <base/range.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsDateTime.h>
#include <IO/ReadBufferFromString.h>

#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DUCKDB_ENGINE_ERROR;
}

void insertDuckDBValue(IColumn & column, ExternalResultDescription::ValueType type, const DataTypePtr & data_type, duckdb::Value & value)
{
    switch (type)
    {
        case ExternalResultDescription::ValueType::vtUInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(value.GetValue<uint8_t>());
            break;
        case ExternalResultDescription::ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(value.GetValue<uint16_t>());
            break;
        case ExternalResultDescription::ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(value.GetValue<uint32_t>());
            break;
        case ExternalResultDescription::ValueType::vtUInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(value.GetValue<uint64_t>());
            break;
        case ExternalResultDescription::ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(value.GetValue<int8_t>());
            break;
        case ExternalResultDescription::ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(value.GetValue<int16_t>());
            break;
        case ExternalResultDescription::ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(value.GetValue<int32_t>());
            break;
        case ExternalResultDescription::ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(value.GetValue<int64_t>());
            break;
        case ExternalResultDescription::ValueType::vtInt128:
        {
            duckdb::hugeint_t duck_int = value.GetValue<duckdb::hugeint_t>();
            Int128 our_int = duck_int.upper;
            our_int = (our_int << 64) | duck_int.lower;
            assert_cast<ColumnInt128 &>(column).insertValue(our_int);
            break;
        }
        case ExternalResultDescription::ValueType::vtFloat32:
            assert_cast<ColumnFloat32 &>(column).insertValue(value.GetValue<float>());
            break;
        case ExternalResultDescription::ValueType::vtFloat64:
            assert_cast<ColumnFloat64 &>(column).insertValue(value.GetValue<double>());
            break;
        case ExternalResultDescription::ValueType::vtDecimal32: [[fallthrough]];
        case ExternalResultDescription::ValueType::vtDecimal64: [[fallthrough]];
        case ExternalResultDescription::ValueType::vtDecimal128: [[fallthrough]];
        case ExternalResultDescription::ValueType::vtDecimal256:
        {
            auto str = value.ToString();
            ReadBufferFromString istr(str);
            data_type->getDefaultSerialization()->deserializeWholeText(column, istr, FormatSettings{});
            break;
        }
        case ExternalResultDescription::ValueType::vtDate32:
            assert_cast<ColumnInt32 &>(column).insertValue(value.GetValue<int32_t>());
            break;
        case ExternalResultDescription::ValueType::vtDateTime64:
        {
            auto str = value.ToString();
            ReadBufferFromString in(str);
            DateTime64 time = 0;
            readDateTime64Text(time, 6, in, assert_cast<const DataTypeDateTime64 *>(data_type.get())->getTimeZone());
            assert_cast<ColumnDateTime64 &>(column).insertValue(time);
            break;
        }
        case ExternalResultDescription::ValueType::vtUUID:
            assert_cast<ColumnUUID &>(column).insertValue(parse<UUID>(value.ToString()));
            break;
        default:
            String string_value = value.ToString();
            assert_cast<ColumnString &>(column).insertData(string_value.data(), string_value.size());
    }
}

DuckDBSource::DuckDBSource(
    DuckDBPtr duckdb_instance_,
    const String & query_str_,
    const Block & sample_block,
    const UInt64 max_block_size_)
    : ISource(sample_block.cloneEmpty())
    , query_str(query_str_)
    , max_block_size(max_block_size_)
    , duckdb_instance(std::move(duckdb_instance_))
{
    description.init(sample_block);
}

Chunk DuckDBSource::generate()
{
    if (finished_fetch)
        return {};

    if (!result)
    {
        /// TODO: Move it to the class
        duckdb::Connection con(*duckdb_instance);
        result = con.SendQuery(query_str);

        if (result->HasError())
        {
            throw Exception(ErrorCodes::DUCKDB_ENGINE_ERROR,
                            "Cannot perform DuckDB SELECT query. Error type: {}. Message: {}",
                            result->GetErrorType(), result->GetError());
        }

        fetched_chunk = result->Fetch();
        fetched_chunk_pos = 0;

        if (!fetched_chunk)
        {
            finished_fetch = true;
            return {};
        }
    }

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (true)
    {
        if (!fetched_chunk)
        {
            finished_fetch = true;
            break;
        }

        size_t column_count = result->ColumnCount();
        size_t row_count = fetched_chunk->size();

        for (size_t idx = fetched_chunk_pos; idx < row_count; ++idx)
        {
            for (size_t column_index = 0; column_index < column_count; ++column_index)
            {
                duckdb::Value value = fetched_chunk->GetValue(column_index, idx);

                if (value.IsNull())
                {
                    columns[column_index]->insertDefault();
                    continue;
                }

                const auto & sample = description.sample_block.getByPosition(column_index);

                auto & [type, is_nullable] = description.types[column_index];
                if (is_nullable)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[column_index]);
                    const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);

                    insertDuckDBValue(column_nullable.getNestedColumn(), type, data_type.getNestedType(), value);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                {
                    insertDuckDBValue(*columns[column_index], type, sample.type, value);
                }
            }

            ++fetched_chunk_pos;

            if (++num_rows == max_block_size)
                break;
        }

        if (num_rows == max_block_size)
            break;

        fetched_chunk = result->Fetch();
        fetched_chunk_pos = 0;
    }

    return Chunk(std::move(columns), num_rows);
}

}

#endif
