#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
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
    std::vector<VectorWithMemoryTracking<ColumnPtr>> source_columns(columns.size());
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
        /// Sometimes header can contain Sparse columns, we don't support Sparse in merge algorithms.
        columns[i] = recursiveRemoveSparse(std::move(columns[i]))->assumeMutable();
        if (is_replicated[i])
            columns[i] = ColumnReplicated::create(std::move(columns[i]));
        /// Columns with dynamic structure (like JSON/Dynamic) need their structure to be
        /// merged from all source columns before the merge starts.
        if (columns[i]->hasDynamicStructure())
            columns[i]->chooseDynamicStructureForMerge(source_columns[i], max_dynamic_subcolumns);
        /// Columns with statistics (like Map with adaptive buckets) need their statistics to be
        /// merged from all source columns before the merge starts.
        /// Must be called after `chooseDynamicStructureForMerge` for columns that have both.
        if (columns[i]->hasStatistics())
            columns[i]->takeOrCalculateStatisticsFrom(source_columns[i]);
    }
}

void MergedData::insertRow(const ColumnRawPtrs & raw_columns, size_t row, size_t block_size)
{
    size_t num_columns = raw_columns.size();
    chassert(columns.size() == num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        /// If the source is `ColumnReplicated` but the destination is not, wrap the destination
        /// in `ColumnReplicated` so its `insertFrom` can consume both regular and replicated
        /// sources through the same optimized path. This preserves the lazy replication
        /// optimization instead of eagerly materializing the source.
        ///
        /// This can happen when `initialize` set the destination type based on the initial
        /// inputs (none of which were `ColumnReplicated`), but a later chunk arrives via
        /// `consume` with non-sort `ColumnReplicated` columns (for example, from a JOIN
        /// with `enable_lazy_columns_replication = 1`).
        if (raw_columns[i]->isReplicated() && !columns[i]->isReplicated())
            columns[i] = ColumnReplicated::create(std::move(columns[i]));

        columns[i]->insertFrom(*raw_columns[i], row);
    }

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
        /// See comment in `insertRow` for why this wrapping is needed.
        if (raw_columns[i]->isReplicated() && !columns[i]->isReplicated())
            columns[i] = ColumnReplicated::create(std::move(columns[i]));

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
        /// For columns with dynamic structure (like JSON/Dynamic) we cannot just take the column from
        /// the input chunk because the resulting column may have different dynamic structure
        /// (after calling `chooseDynamicStructureForMerge`).
        /// We need to use `cloneEmpty` + `insertRangeFrom` to properly re-insert data.
        ///
        /// If `chunk_columns[i]` is `ColumnReplicated`, wrap the empty destination in
        /// `ColumnReplicated` so `insertRangeFrom` consumes the source via the optimized path
        /// without eagerly materializing it. This preserves the lazy replication optimization.
        else if (columns[i]->hasDynamicStructure())
        {
            columns[i] = columns[i]->cloneEmpty();
            if (chunk_columns[i]->isReplicated() && !columns[i]->isReplicated())
                columns[i] = ColumnReplicated::create(std::move(columns[i]));
            columns[i]->insertRangeFrom(*chunk_columns[i], 0, num_rows);
        }
        /// For columns with statistics (like Map with adaptive buckets) we can reuse the column
        /// from the input chunk, but need to preserve the merged statistics computed during `initialize`.
        else if (columns[i]->hasStatistics())
        {
            /// We cannot call takeOrCalculateStatisticsFrom for non-replicated column with replicated arguments.
            if (columns[i]->getPtr()->isReplicated() && !chunk_columns[i]->isReplicated())
                chunk_columns[i] = ColumnReplicated::create(std::move(chunk_columns[i]));

            chunk_columns[i]->takeOrCalculateStatisticsFrom({columns[i]->getPtr()});
            columns[i] = std::move(chunk_columns[i]);
        }
        else if (columns[i]->isReplicated())
        {
            /// Destination is `ColumnReplicated` (set during `initialize`). If the chunk is also
            /// `ColumnReplicated` move it through; otherwise wrap the regular chunk column.
            if (chunk_columns[i]->isReplicated())
                columns[i] = std::move(chunk_columns[i]);
            else
                columns[i] = ColumnReplicated::create(std::move(chunk_columns[i]));
        }
        else
        {
            /// Simple case: move the chunk column into the destination. If the chunk is
            /// `ColumnReplicated`, the destination becomes `ColumnReplicated` — this preserves
            /// the lazy replication optimization.
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
