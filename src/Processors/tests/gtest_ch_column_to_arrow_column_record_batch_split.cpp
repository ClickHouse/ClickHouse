#include "config.h"

#if USE_ARROW || USE_PARQUET

#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Common/assert_cast.h>

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/table.h>

/// Arrow offsets its `Utf8`, `Binary` and `List` buffers with 32-bit integers, so a chunk holding more
/// than that becomes several record batches, each covering a row range. A range not starting at row 0 has
/// to rebase those offsets against the row before it. Reaching the real limit needs more than 2 GiB of
/// data, so these tests lower it.
///
/// `CHColumnToArrowColumn` is the Apache Arrow library writer, used by `ArrowFlight`,
/// `StorageArrowFlight` and Delta Lake writes rather than by the `Arrow` output format.

namespace DB
{

namespace
{

constexpr size_t NUM_ROWS = 8;
constexpr size_t VALUE_SIZE = 16;

std::string valueAt(size_t row)
{
    return std::string(VALUE_SIZE, static_cast<char>('a' + row));
}

CHColumnToArrowColumn::Settings settingsWithLimit(UInt64 max_record_batch_bytes)
{
    CHColumnToArrowColumn::Settings settings;
    settings.output_string_as_string = true;
    settings.output_max_record_batch_bytes = max_record_batch_bytes;
    return settings;
}

std::shared_ptr<arrow::Table> convertToArrow(const DataTypePtr & type, MutableColumnPtr column, UInt64 max_record_batch_bytes)
{
    ColumnsWithTypeAndName header_columns{ColumnWithTypeAndName(type->createColumn(), type, "t")};

    Columns columns;
    columns.emplace_back(std::move(column));

    std::vector<Chunk> chunks;
    chunks.emplace_back(std::move(columns), NUM_ROWS);

    CHColumnToArrowColumn converter(header_columns, "Arrow", settingsWithLimit(max_record_batch_bytes));
    std::shared_ptr<arrow::Table> arrow_table;
    converter.chChunkToArrowTable(arrow_table, chunks, 1);
    return arrow_table;
}

}

TEST(CHColumnToArrowColumn, StringSplitAcrossRecordBatches)
{
    auto type = std::make_shared<DataTypeString>();
    auto column = type->createColumn();
    for (size_t row = 0; row < NUM_ROWS; ++row)
        column->insert(valueAt(row));

    /// Two values per batch, so most batches start past row 0.
    auto table = convertToArrow(type, std::move(column), 2 * VALUE_SIZE);

    const auto & chunked = *table->column(0);
    ASSERT_GT(chunked.num_chunks(), 1);
    ASSERT_EQ(chunked.length(), static_cast<int64_t>(NUM_ROWS));

    size_t row = 0;
    for (int chunk_i = 0; chunk_i < chunked.num_chunks(); ++chunk_i)
    {
        const auto & array = assert_cast<const arrow::StringArray &>(*chunked.chunk(chunk_i));
        /// Every batch addresses its own values from zero.
        ASSERT_EQ(array.value_offset(0), 0);
        for (int64_t i = 0; i < array.length(); ++i, ++row)
            ASSERT_EQ(array.GetString(i), valueAt(row));
    }
    ASSERT_EQ(row, NUM_ROWS);
}

TEST(CHColumnToArrowColumn, ArraySplitAcrossRecordBatches)
{
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    auto column = type->createColumn();
    for (size_t row = 0; row < NUM_ROWS; ++row)
        column->insert(Array{Field(valueAt(row)), Field(valueAt(row))});

    auto table = convertToArrow(type, std::move(column), 2 * VALUE_SIZE);

    const auto & chunked = *table->column(0);
    ASSERT_GT(chunked.num_chunks(), 1);
    ASSERT_EQ(chunked.length(), static_cast<int64_t>(NUM_ROWS));

    size_t row = 0;
    for (int chunk_i = 0; chunk_i < chunked.num_chunks(); ++chunk_i)
    {
        const auto & list = assert_cast<const arrow::ListArray &>(*chunked.chunk(chunk_i));
        ASSERT_EQ(list.value_offset(0), 0);
        for (int64_t i = 0; i < list.length(); ++i, ++row)
        {
            const auto & values = assert_cast<const arrow::StringArray &>(*list.value_slice(i));
            ASSERT_EQ(values.length(), 2);
            ASSERT_EQ(values.GetString(0), valueAt(row));
            ASSERT_EQ(values.GetString(1), valueAt(row));
        }
    }
    ASSERT_EQ(row, NUM_ROWS);
}

}

#endif
