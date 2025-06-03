#pragma once

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Types.h>
#include <Core/Block.h>
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
    explicit MergedData(bool use_average_block_size_, UInt64 max_block_size_, UInt64 max_block_size_bytes_)
        : max_block_size(max_block_size_), max_block_size_bytes(max_block_size_bytes_), use_average_block_size(use_average_block_size_)
    {
    }

    virtual void initialize(const Block & header, const IMergingAlgorithm::Inputs & inputs)
    {
        columns = header.cloneEmptyColumns();
        std::vector<Columns> source_columns;
        source_columns.resize(columns.size());
        for (const auto & input : inputs)
        {
            if (!input.chunk)
                continue;

            const auto & input_columns = input.chunk.getColumns();
            for (size_t i = 0; i != input_columns.size(); ++i)
                source_columns[i].push_back(input_columns[i]);
        }

        for (size_t i = 0; i != columns.size(); ++i)
        {
            if (columns[i]->hasDynamicStructure())
                columns[i]->takeDynamicStructureFromSourceColumns(source_columns[i]);
        }
    }

    /// Pull will be called at next prepare call.
    void flush() { need_flush = true; }

    void insertRow(const ColumnRawPtrs & raw_columns, size_t row, size_t block_size)
    {
        size_t num_columns = raw_columns.size();
        chassert(columns.size() == num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            columns[i]->insertFrom(*raw_columns[i], row);

        ++total_merged_rows;
        ++merged_rows;
        sum_blocks_granularity += block_size;
    }

    void insertRows(const ColumnRawPtrs & raw_columns, size_t start_index, size_t length, size_t block_size)
    {
        size_t num_columns = raw_columns.size();
        chassert(columns.size() == num_columns);
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert to MergedData from Chunk because MergedData is not empty.");

        UInt64 num_rows = chunk.getNumRows();
        UInt64 num_columns = chunk.getNumColumns();
        chassert(columns.size() == num_columns);
        auto chunk_columns = chunk.mutateColumns();

        /// Here is a special code for constant columns.
        /// Currently, 'columns' will contain constants, but 'chunk_columns' will not.
        /// We want to keep constants in the result, so just re-create them carefully.
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (isColumnConst(*columns[i]))
            {
                columns[i] = columns[i]->cloneResized(num_rows);
            }
            /// For columns with Dynamic structure we cannot just take column from input chunk because resulting column may have
            /// different Dynamic structure (and have some merge statistics after calling takeDynamicStructureFromSourceColumns).
            /// We should insert into data resulting column using insertRangeFrom.
            else if (columns[i]->hasDynamicStructure())
            {
                columns[i] = columns[i]->cloneEmpty();
                columns[i]->insertRangeFrom(*chunk_columns[i], 0, num_rows);
            }
            else
            {
                columns[i] = std::move(chunk_columns[i]);
            }
        }

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
        total_allocated_bytes += chunk.bytes();
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

        /// Never return more than max_block_size_bytes
        if (max_block_size_bytes)
        {
            size_t merged_bytes = 0;
            for (const auto & column : columns)
                merged_bytes += column->byteSize();
            if (merged_bytes >= max_block_size_bytes)
                return true;
        }

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

    IMergingAlgorithm::MergedStats getMergedStats() const { return {.bytes = total_allocated_bytes, .rows = total_merged_rows, .blocks = total_chunks}; }

    virtual ~MergedData() = default;

protected:
    MutableColumns columns;

    UInt64 sum_blocks_granularity = 0;
    UInt64 merged_rows = 0;
    UInt64 total_merged_rows = 0;
    UInt64 total_chunks = 0;
    UInt64 total_allocated_bytes = 0;

    const UInt64 max_block_size = 0;
    const UInt64 max_block_size_bytes = 0;
    const bool use_average_block_size = false;

    bool need_flush = false;
};

}
