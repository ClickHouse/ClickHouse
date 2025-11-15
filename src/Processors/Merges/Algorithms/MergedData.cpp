#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Columns/ColumnReplicated.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void MergedData::initialize(const Block & header, const IMergingAlgorithm::Inputs & inputs)
{
    columns = header.cloneEmptyColumns();
    std::vector<Columns> source_columns(columns.size());
    std::vector<bool> is_replicated(columns.size());
    for (const auto & input : inputs)
    {
        if (!input.chunk)
            continue;

        const auto & input_columns = input.chunk.getColumns();
        for (size_t i = 0; i != input_columns.size(); ++i)
        {
            source_columns[i].push_back(input_columns[i]);
            is_replicated[i] = is_replicated[i] || input_columns[i]->isReplicated();
        }
    }

    for (size_t i = 0; i != columns.size(); ++i)
    {
        if (is_replicated[i])
            columns[i] = ColumnReplicated::create(std::move(columns[i]));
        if (columns[i]->hasDynamicStructure())
            columns[i]->takeDynamicStructureFromSourceColumns(source_columns[i], max_dynamic_subcolumns);
    }
}

void MergedData::insertRow(const ColumnRawPtrs & raw_columns, size_t row, size_t block_size)
{
    size_t num_columns = raw_columns.size();
    chassert(columns.size() == num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i]->insertFrom(*raw_columns[i], row);

    ++total_merged_rows;
    ++merged_rows;
    sum_blocks_granularity += block_size;
}

void MergedData::insertRows(const ColumnRawPtrs & raw_columns, size_t start_index, size_t length, size_t block_size)
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

void MergedData::insertChunk(Chunk && chunk, size_t rows_size)
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
        else if (columns[i]->isReplicated() && !chunk_columns[i]->isReplicated())
        {
            columns[i] = ColumnReplicated::create(std::move(chunk_columns[i]));
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

Chunk MergedData::pull()
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

bool MergedData::hasEnoughRows() const
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
        {
            merged_bytes += column->byteSize();
            if (merged_bytes >= max_block_size_bytes)
                return true;
        }
    }

    if (!use_average_block_size)
        return false;

    /// Zero rows always not enough.
    if (merged_rows == 0)
        return false;

    size_t average = sum_blocks_granularity / merged_rows;
    return merged_rows >= average;
}

}
