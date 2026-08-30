#include <Columns/IColumn.h>
#include <Columns/ColumnNothing.h>
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

namespace
{

bool hasNonAdditiveByteSizeAt(const IColumn & column)
{
    if (column.getDataType() == TypeIndex::LowCardinality
        || column.getDataType() == TypeIndex::AggregateFunction
        || column.isReplicated()
        || column.hasDynamicStructure())
        return true;

    bool result = false;
    column.forEachSubcolumn([&](const ColumnPtr & subcolumn)
    {
        result = result || hasNonAdditiveByteSizeAt(*subcolumn);
    });
    return result;
}

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

    if (defer_materialization)
    {
        current_materialization_sources.resize(inputs.size());
        current_materialization_source_is_set.assign(inputs.size(), false);
        materialization_source_handles.resize(inputs.size());

        for (size_t source_num = 0; source_num < inputs.size(); ++source_num)
        {
            if (inputs[source_num].chunk)
                setSource(source_num, inputs[source_num].chunk.getColumns(), inputs[source_num].chunk.getNumRows());
        }
    }
}

void MergedData::initializeFromColumns(const Columns & prototype_columns)
{
    columns.clear();
    columns.reserve(prototype_columns.size());
    for (const auto & column : prototype_columns)
        columns.emplace_back(column->cloneEmpty());
}

void MergedData::setSource(size_t source_num, const Columns & source_columns, size_t num_rows)
{
    if (!defer_materialization)
        return;

    if (source_num >= current_materialization_sources.size())
    {
        current_materialization_sources.resize(source_num + 1);
        current_materialization_source_is_set.resize(source_num + 1, false);
        materialization_source_handles.resize(source_num + 1);
    }

    current_materialization_sources[source_num] = {source_columns, num_rows};
    current_materialization_source_is_set[source_num] = true;
    materialization_source_handles[source_num].reset();
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

size_t MergedData::getOrAddMaterializationSource(size_t source_num, bool release_source)
{
    if (source_num >= current_materialization_sources.size() || !current_materialization_source_is_set[source_num])
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} is not registered for deferred materialization", source_num);

    if (materialization_source_handles[source_num])
        return *materialization_source_handles[source_num];

    const size_t handle = materialization_sources.size();
    if (release_source)
    {
        materialization_sources.emplace_back(std::move(current_materialization_sources[source_num]));
        current_materialization_source_is_set[source_num] = false;
    }
    else
    {
        materialization_sources.emplace_back(current_materialization_sources[source_num]);
    }

    materialization_source_handles[source_num] = handle;
    return handle;
}

void MergedData::appendMaterializationRun(size_t source, size_t start, size_t length)
{
    if (!length)
        return;

    if (!materialization_runs.empty())
    {
        auto & previous = materialization_runs.back();
        if (previous.source == source && previous.start + previous.length == start)
        {
            previous.length += length;
            return;
        }
    }

    materialization_runs.push_back({source, start, length});
}

void MergedData::insertRowFromSource(
    size_t source_num,
    const ColumnRawPtrs & raw_columns,
    size_t row,
    size_t block_size)
{
    insertRowsFromSource(source_num, raw_columns, row, 1, block_size);
}

void MergedData::insertRowsFromSource(
    size_t source_num,
    const ColumnRawPtrs & raw_columns,
    size_t start_index,
    size_t length,
    size_t block_size)
{
    if (!defer_materialization)
    {
        insertRows(raw_columns, start_index, length, block_size);
        return;
    }

    if (source_num >= current_materialization_sources.size() || !current_materialization_source_is_set[source_num])
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} is not registered for deferred materialization", source_num);

    const auto & source = current_materialization_sources[source_num];
    if (start_index > source.num_rows || length > source.num_rows - start_index)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Invalid deferred materialization range [{}, {}) for a source with {} rows",
            start_index,
            start_index + length,
            source.num_rows);

    const size_t source_handle = getOrAddMaterializationSource(source_num, false);
    appendMaterializationRun(source_handle, start_index, length);

    total_merged_rows += length;
    merged_rows += length;
    sum_blocks_granularity += block_size * length;
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

void MergedData::insertChunkFromSource(size_t source_num, Chunk && chunk, size_t rows_size)
{
    if (!defer_materialization)
    {
        insertChunk(std::move(chunk), rows_size);
        return;
    }

    if (merged_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert a full Chunk into non-empty deferred MergedData");

    const size_t num_rows = chunk.getNumRows();
    if (rows_size > num_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot materialize {} rows from a Chunk with {} rows",
            rows_size,
            num_rows);

    setSource(source_num, chunk.getColumns(), num_rows);
    const size_t source_handle = getOrAddMaterializationSource(source_num, true);
    appendMaterializationRun(source_handle, 0, rows_size);
    chunk.clear();

    need_flush = true;
    total_merged_rows += rows_size;
    merged_rows = rows_size;
}

Chunk MergedData::pull()
{
    if (defer_materialization)
    {
        Chunk chunk;
        if (merged_rows)
        {
            auto info = std::make_shared<MergedDataMaterializationInfo>();
            info->output_columns.reserve(columns.size());
            for (const auto & column : columns)
                info->output_columns.emplace_back(column->getPtr());
            info->sources = std::move(materialization_sources);
            info->runs = std::move(materialization_runs);

            /// Ports require matching column counts, but the payload is read only by the
            /// materializer from `info`. Avoid inserting defaults into payload columns:
            /// `ColumnFunction`, for example, does not support `insertDefault`.
            chunk = Chunk(Columns(columns.size(), ColumnNothing::create(merged_rows)), merged_rows);
            chunk.getChunkInfos().add(std::move(info));
        }

        materialization_sources.clear();
        materialization_runs.clear();
        for (auto & source_handle : materialization_source_handles)
            source_handle.reset();

        merged_rows = 0;
        sum_blocks_granularity = 0;
        ++total_chunks;
        /// The actual byte size is known only after replaying the plan. Each
        /// `MaterializeMergedDataTransform` accumulates it and the shared
        /// `MergingSortedTransformStats` reports it after all workers finish.
        need_flush = false;
        return chunk;
    }

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
    if (merged_rows && max_block_size_bytes)
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

size_t MergedData::rowsToInsertBeforeFlush(
    const ColumnRawPtrs & raw_columns,
    size_t start_index,
    size_t max_rows,
    size_t block_size) const
{
    chassert(max_rows > 0);

    size_t rows_to_insert = max_rows;

    if (use_average_block_size)
    {
        for (size_t length = 1; length <= rows_to_insert; ++length)
        {
            const size_t merged_rows_after_insert = merged_rows + length;
            const size_t average_block_size
                = (sum_blocks_granularity + block_size * length) / merged_rows_after_insert;

            if (merged_rows_after_insert >= average_block_size)
            {
                rows_to_insert = length;
                break;
            }
        }
    }

    if (!max_block_size_bytes)
        return rows_to_insert;

    chassert(columns.size() == raw_columns.size());

    /// `byteSizeAt` is not additive for dictionary/index-backed columns, so let
    /// `hasEnoughRows` measure the actual size after inserting one row.
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (hasNonAdditiveByteSizeAt(*columns[i]) || hasNonAdditiveByteSizeAt(*raw_columns[i]))
            return 1;
    }

    size_t merged_bytes = 0;
    for (const auto & column : columns)
        merged_bytes += column->byteSize();

    for (size_t length = 1; length <= rows_to_insert; ++length)
    {
        const size_t row = start_index + length - 1;
        for (const auto * column : raw_columns)
            merged_bytes += column->byteSizeAt(row);

        if (merged_bytes >= max_block_size_bytes)
            return length;
    }

    return rows_to_insert;
}

}
