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

#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DUCKDB_ENGINE_ERROR;
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

                auto & [type, is_nullable] = description.types[column_index];
                if (is_nullable)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[column_index]);
                    insertValue(column_nullable.getNestedColumn(), type, value);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                {
                    insertValue(*columns[column_index], type, value);
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

void DuckDBSource::insertValue(IColumn & column, ExternalResultDescription::ValueType type, duckdb::Value & value)
{
    switch (type)
    {
        case ValueType::vtUInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(value.GetValue<uint8_t>());
            break;
        case ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(value.GetValue<uint16_t>());
            break;
        case ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(value.GetValue<uint32_t>());
            break;
        case ValueType::vtUInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(value.GetValue<uint64_t>());
            break;
        case ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(value.GetValue<int16_t>());
            break;
        case ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(value.GetValue<int16_t>());
            break;
        case ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(value.GetValue<int32_t>());
            break;
        case ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(value.GetValue<uint64_t>());
            break;
        case ValueType::vtFloat32:
            assert_cast<ColumnFloat32 &>(column).insertValue(value.GetValue<float>());
            break;
        case ValueType::vtFloat64:
            assert_cast<ColumnFloat64 &>(column).insertValue(value.GetValue<double>());
            break;
        default:
            String string_value = value.ToString();
            assert_cast<ColumnString &>(column).insertData(string_value.data(), string_value.size());
    }
}

}

#endif
