#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>
#include <Columns/IColumn.h>
#include <Processors/Chunk.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Class which represents current merging chunk of data.
/// Also it calculates the number of merged rows and other profile info.
class MergedData
{
public:
    explicit MergedData(MutableColumns columns_, bool use_average_block_size_, UInt64 max_block_size_)
        : columns(std::move(columns_)), max_block_size(max_block_size_), use_average_block_size(use_average_block_size_)
    {
    }

    /// Pull will be called at next prepare call.
    void flush() { need_flush = true; }

    void insertRow(const ColumnRawPtrs & raw_columns, size_t row, size_t block_size)
    {
        size_t num_columns = raw_columns.size();
        for (size_t i = 0; i < num_columns; ++i)
            columns[i]->insertFrom(*raw_columns[i], row);

        ++total_merged_rows;
        ++merged_rows;
        sum_blocks_granularity += block_size;
    }

    void insertRows(const ColumnRawPtrs & raw_columns, size_t start_index, size_t length, size_t block_size)
    {
        size_t num_columns = raw_columns.size();
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (length == 1)
                columns[i]->insertFrom(*raw_columns[i], start_index);
            else
                columns[i]->insertRangeFrom(*raw_columns[i], start_index, length);
        }

        total_merged_rows += length;
        merged_rows += length;
        sum_blocks_granularity += (block_size * length);
    }

    void insertChunk(Chunk && chunk, size_t rows_size)
    {
        if (merged_rows)
            throw Exception("Cannot insert to MergedData from Chunk because MergedData is not empty.",
                            ErrorCodes::LOGICAL_ERROR);

        UInt64 num_rows = chunk.getNumRows();
        columns = chunk.mutateColumns();

        if (rows_size < num_rows)
        {
            size_t pop_size = num_rows - rows_size;
            for (auto & column : columns)
                column->popBack(pop_size);
        }

        need_flush = true;
        total_merged_rows += rows_size;
        merged_rows = rows_size;
    }

    Chunk pull()
    {
        MutableColumns empty_columns;
        empty_columns.reserve(columns.size());

        for (const auto & column : columns)
            empty_columns.emplace_back(column->cloneEmpty());

        empty_columns.swap(columns);
        Chunk chunk(std::move(empty_columns), merged_rows);

        merged_rows = 0;
        sum_blocks_granularity = 0;
        ++total_chunks;
        total_allocated_bytes += chunk.allocatedBytes();
        need_flush = false;

        return chunk;
    }

    bool hasEnoughRows() const
    {
        /// If full chunk was or is going to be inserted, then we must pull it.
        /// It is needed for fast-forward optimization.
        if (need_flush)
            return true;

        /// Never return more than max_block_size.
        if (merged_rows >= max_block_size)
            return true;

        if (!use_average_block_size)
            return false;

        /// Zero rows always not enough.
        if (merged_rows == 0)
            return false;

        size_t average = sum_blocks_granularity / merged_rows;
        return merged_rows >= average;
    }

    UInt64 mergedRows() const { return merged_rows; }
    UInt64 totalMergedRows() const { return total_merged_rows; }
    UInt64 totalChunks() const { return total_chunks; }
    UInt64 totalAllocatedBytes() const { return total_allocated_bytes; }
    UInt64 maxBlockSize() const { return max_block_size; }

protected:
    MutableColumns columns;

    UInt64 sum_blocks_granularity = 0;
    UInt64 merged_rows = 0;
    UInt64 total_merged_rows = 0;
    UInt64 total_chunks = 0;
    UInt64 total_allocated_bytes = 0;

    const UInt64 max_block_size;
    const bool use_average_block_size;

    bool need_flush = false;
};

}
