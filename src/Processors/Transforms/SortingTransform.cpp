#include <Processors/Transforms/SortingTransform.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <type_traits>

#include <Columns/ColumnFixedSizeHelper.h>
#include <Columns/ColumnReplicated.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <Common/Exception.h>
#include <Common/memcpySmall.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Both arrays are padded, so a short run is copied with 16-byte moves that may read and write past its end.
ALWAYS_INLINE inline void copyFixedSizeRows(char * __restrict to, const char * __restrict from, size_t bytes)
{
    if (bytes <= 64)
        memcpySmallAllowReadWriteOverflow15(to, from, bytes);
    else
        memcpy(to, from, bytes);
}

}

MergeSorter::MergeSorter(
    SharedHeader header, Chunks chunks_, const SortDescription & description,
    size_t max_merged_block_size_, UInt64 limit_, Mode mode_, size_t preferred_block_bytes)
    : chunks(std::move(chunks_))
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
    , mode(mode_)
    , queue_variants(*header, description)
{
    Chunks nonempty_chunks;
    size_t chunks_size = chunks.size();

    for (size_t chunk_index = 0; chunk_index < chunks_size; ++chunk_index)
    {
        auto & chunk = chunks[chunk_index];
        if (chunk.getNumRows() == 0)
            continue;

        /// Materialize sparse columns to avoid searching their offsets during comparisons.
        convertToFullIfSparse(chunk);

        /// Merge cursors expect full columns.
        convertToFullIfConst(chunk);

        size_t num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();
        /// Sort cursors compare materialized keys; replicated payloads retain their representation.
        for (const auto & column_desc : description)
        {
            size_t column_number = header->getPositionByName(column_desc.column_name);
            columns[column_number] = columns[column_number]->convertToFullColumnIfReplicated();
        }
        chunk.setColumns(std::move(columns), num_rows);

        cursors.emplace_back(*header, chunk.getColumns(), chunk.getNumRows(), description, chunk_index);

        nonempty_chunks.emplace_back(std::move(chunk));
    }

    chunks.swap(nonempty_chunks);

    if (preferred_block_bytes && !chunks.empty())
    {
        size_t total_rows = 0;
        size_t total_bytes = 0;
        for (const auto & chunk : chunks)
        {
            total_rows += chunk.getNumRows();
            total_bytes += chunk.allocatedBytes();
        }

        max_merged_block_size = calculateMaxMergedBlockSize(max_merged_block_size, preferred_block_bytes, total_rows, total_bytes);
    }

    queue_variants.callOnBatchVariant([&](auto & queue)
    {
        using QueueType = std::decay_t<decltype(queue)>;
        queue = QueueType(cursors);
    });

    initFixedSizeColumns();
}

void MergeSorter::initFixedSizeColumns()
{
    if (cursors.empty())
        return;

    size_t num_columns = chunks[0].getNumColumns();
    for (size_t position = 0; position < num_columns; ++position)
    {
        const IColumn & first = *cursors[0].all_columns[position];
        /// `ColumnVector`, `ColumnDecimal` and `ColumnFixedString`: padded arrays written with `insertRawUninitialized`.
        bool is_fixed_size = dynamic_cast<const ColumnFixedSizeHelper *>(&first) != nullptr;

        FixedSizeColumn column{.position = position, .value_size = is_fixed_size ? first.sizeOfValueIfFixed() : 0, .chunk_data = {}};
        if (is_fixed_size)
        {
            column.chunk_data.reserve(cursors.size());
            for (const auto & cursor : cursors)
            {
                const IColumn & chunk_column = *cursor.all_columns[position];
                if (!first.structureEquals(chunk_column))
                {
                    is_fixed_size = false;
                    break;
                }
                column.chunk_data.push_back(chunk_column.getRawData().data());
            }
        }

        if (is_fixed_size)
            fixed_size_columns.push_back(std::move(column));
        else
            other_columns.push_back(position);
    }

    fixed_size_output.resize(fixed_size_columns.size());
}


size_t MergeSorter::calculateMaxMergedBlockSize(
    size_t max_block_rows, size_t preferred_block_bytes, size_t input_rows, size_t input_bytes)
{
    chassert(input_rows);
    if (!preferred_block_bytes)
        return max_block_rows;

    /// Smaller spill blocks reduce the decoded memory held by each merge input. Average row size
    /// gives a byte target rather than a strict bound for uneven rows. The minimum of 128 rows
    /// avoids excessive per-block overhead, while the explicit row limit still takes precedence.
    /// Compact column representations can use less than one allocated byte per row.
    const size_t average_row_bytes = std::max<size_t>(1, input_bytes / input_rows);
    return std::min(max_block_rows, std::max<size_t>(128, preferred_block_bytes / average_row_bytes));
}

Chunk MergeSorter::read()
{
    if (chunks.empty())
        return Chunk();

    if (chunks.size() == 1 && chunks.front().getNumRows() <= max_merged_block_size
        && (!limit || chunks.front().getNumRows() <= limit))
    {
        auto res = std::move(chunks[0]);
        chunks.clear();
        return res;
    }

    Chunk result = queue_variants.callOnBatchVariant([&](auto & queue)
    {
        if (mode == Mode::MergeUniqueChunks)
            return mergeBatchImpl<Mode::MergeUniqueChunks>(queue);
        return mergeBatchImpl<Mode::PreserveRows>(queue);
    });

    return result;
}


template <MergeSorter::Mode merge_mode, typename TSortingQueue>
Chunk MergeSorter::mergeBatchImpl(TSortingQueue & queue)
{
    MutableColumns merged_columns = createMergedColumns();

    /// Reserve at most one output block because reserved capacity counts toward tracked memory.
    size_t size_to_reserve = std::min(static_cast<size_t>(chunks[0].getNumRows()), max_merged_block_size);
    for (auto & column : merged_columns)
        column->reserve(size_to_reserve);

    /// Fixed-size columns expose their capacity and are written in place below the smallest one. A run that does
    /// not fit is inserted as into the other columns, so these columns grow exactly as the others do.
    size_t fixed_size_capacity = 0;
    auto expose_fixed_size_capacity = [&]
    {
        fixed_size_capacity = std::numeric_limits<size_t>::max();
        for (size_t i = 0; i < fixed_size_columns.size(); ++i)
        {
            IColumn & column = *merged_columns[fixed_size_columns[i].position];
            const size_t size = column.size();
            const size_t capacity = column.capacity();
            fixed_size_output[i] = column.insertRawUninitialized(capacity - size).data() - size * fixed_size_columns[i].value_size;
            fixed_size_capacity = std::min(fixed_size_capacity, capacity);
        }
    };
    auto drop_unwritten_rows = [&](size_t rows)
    {
        for (const auto & column : fixed_size_columns)
        {
            IColumn & merged = *merged_columns[column.position];
            if (merged.size() > rows)
                merged.popBack(merged.size() - rows);
        }
    };
    if (!fixed_size_columns.empty())
        expose_fixed_size_capacity();

    size_t consumed_rows = 0;
    size_t merged_rows = 0;
    bool limit_reached = false;
    while (queue.isValid())
    {
        auto [current_ptr, batch_size] = queue.current();
        auto & current = *current_ptr;
        const size_t first_row = current->getRow();
        batch_size = std::min(batch_size, max_merged_block_size - consumed_rows);

        size_t skipped_rows = 0;
        if constexpr (merge_mode == Mode::MergeUniqueChunks)
        {
            /// Only a different input chunk can repeat the last emitted key.
            if (last_emitted_cursor && last_emitted_cursor != current.impl)
            {
                using Cursor = std::decay_t<decltype(current)>;
                const Cursor previous(last_emitted_cursor);
                /// Keys are nondecreasing, so a key that is not greater is equal. Source order chooses
                /// the first payload but does not participate in key equality.
                skipped_rows = !current.template greaterAt<false>(previous, first_row, last_emitted_row);
            }
        }

        size_t rows_to_insert = batch_size - skipped_rows;
        if (limit && rows_to_insert >= limit - total_merged_rows)
        {
            rows_to_insert = limit - total_merged_rows;
            batch_size = rows_to_insert + skipped_rows;
            limit_reached = true;
        }

        if (rows_to_insert)
        {
            /// Each input batch is internally unique in `MergeUniqueChunks` mode. Only its first row
            /// can repeat the last emitted key, so the remaining rows form one contiguous range.
            const size_t row = first_row + skipped_rows;
            auto insert_rows = [&](size_t position)
            {
                if (rows_to_insert == 1)
                    merged_columns[position]->insertFrom(*current->all_columns[position], row);
                else
                    merged_columns[position]->insertRangeFrom(*current->all_columns[position], row, rows_to_insert);
            };

            if (!fixed_size_columns.empty())
            {
                if (merged_rows + rows_to_insert <= fixed_size_capacity)
                {
                    const size_t chunk_index = current.impl - cursors.data();
                    const bool has_next_row = row + rows_to_insert < current->rows;
                    for (size_t i = 0; i < fixed_size_columns.size(); ++i)
                    {
                        const auto & column = fixed_size_columns[i];
                        const char * from = column.chunk_data[chunk_index] + row * column.value_size;
                        const size_t bytes = rows_to_insert * column.value_size;
                        copyFixedSizeRows(fixed_size_output[i] + merged_rows * column.value_size, from, bytes);

                        /// Every chunk is read sequentially, but the merge alternates between all chunks, which is more
                        /// streams than the hardware prefetcher follows. This cursor usually comes back only after runs
                        /// of the other chunks, so its next cache line can be requested now.
                        if (has_next_row)
                            __builtin_prefetch(from + bytes + 64);
                    }
                }
                else
                {
                    drop_unwritten_rows(merged_rows);
                    for (const auto & column : fixed_size_columns)
                        insert_rows(column.position);
                    expose_fixed_size_capacity();
                }
            }

            for (size_t position : other_columns)
                insert_rows(position);

            if constexpr (merge_mode == Mode::MergeUniqueChunks)
            {
                last_emitted_cursor = current.impl;
                last_emitted_row = first_row + batch_size - 1;
            }
        }

        total_merged_rows += rows_to_insert;
        merged_rows += rows_to_insert;
        consumed_rows += batch_size;

        if (limit_reached)
            break;

        queue.next(batch_size);

        /// Bound work by consumed rows so duplicate-only batches return control to the caller.
        if (consumed_rows == max_merged_block_size)
            break;
    }

    drop_unwritten_rows(merged_rows);

    if (limit_reached || !queue.isValid())
    {
        last_emitted_cursor = nullptr;
        chunks.clear();
    }

    if (merged_rows == 0 && chunks.empty())
        return {};

    for (auto & column : merged_columns)
        finishMergedReplicatedColumn(column);

    return Chunk(std::move(merged_columns), merged_rows);
}

MutableColumns MergeSorter::createMergedColumns() const
{
    size_t num_columns = chunks[0].getNumColumns();
    VectorWithMemoryTracking<bool> is_replicated(num_columns, false);
    for (const auto & chunk : chunks)
    {
        for (size_t i = 0; i != chunk.getNumColumns(); ++i)
            is_replicated[i] = is_replicated[i] || chunk.getColumns()[i]->isReplicated();
    }
    MutableColumns merged_columns = chunks[0].cloneEmptyColumns();
    for (size_t i = 0; i != num_columns; ++i)
    {
        if (is_replicated[i] && !merged_columns[i]->isReplicated())
            merged_columns[i] = ColumnReplicated::create(std::move(merged_columns[i]));
    }

    return merged_columns;
}


void MergeSorterSource::cancel(CancelReason reason) noexcept
{
    /// A partial result must finish processing data already read into the in-memory tail.
    if (reason == CancelReason::PartialResult)
        return;

    ISource::cancel(reason);
}

SortingTransform::SortingTransform(
    SharedHeader header,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    UInt64 limit_,
    bool increase_sort_description_compile_attempts)
    : IProcessor({header}, {header})
    , description(description_)
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
{
    const auto & sample = inputs.front().getHeader();

    /// Remove constants from header and map old indexes to new.
    size_t num_columns = sample.columns();
    ColumnNumbers map(num_columns, num_columns);
    const_columns_to_remove.assign(num_columns, true);
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        const auto & column = sample.getByPosition(pos);
        if (!(column.column && isColumnConst(*column.column)))
        {
            map[pos] = header_without_constants.columns();
            header_without_constants.insert(column);
            const_columns_to_remove[pos] = false;
        }
    }

    DataTypes sort_description_types;
    sort_description_types.reserve(description.size());

    /// Remove constants from column_description and remap positions.
    SortDescription description_without_constants;
    description_without_constants.reserve(description.size());
    for (const auto & column_description : description)
    {
        auto old_pos = header->getPositionByName(column_description.column_name);
        auto new_pos = map[old_pos];

        if (new_pos < num_columns)
        {
            sort_description_types.emplace_back(sample.safeGetByPosition(old_pos).type);
            description_without_constants.push_back(column_description);
        }
    }

    description.swap(description_without_constants);

    if (SortQueueVariants(sort_description_types, description).variantSupportJITCompilation())
        compileSortDescriptionIfNeeded(description, sort_description_types, increase_sort_description_compile_attempts /*increase_compile_attempts*/);
}

SortingTransform::~SortingTransform() = default;

IProcessor::Status SortingTransform::prepare()
{
    if (stage == Stage::Serialize)
    {
        if (!processors.empty())
            return Status::UpdatePipeline;

        auto status = prepareSerialize();
        if (status != Status::Finished)
            return status;

        stage = Stage::Consume;
    }

    if (stage == Stage::Consume)
    {
        auto status = prepareConsume();
        if (status != Status::Finished)
            return status;

        stage = Stage::Generate;
    }

    /// stage == Stage::Generate

    if (!generated_prefix || !chunks.empty())
        return Status::Ready;

    if (!processors.empty())
        return Status::UpdatePipeline;

    return prepareGenerate();
}

IProcessor::Status SortingTransform::prepareConsume()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (generated_chunk)
        output.push(std::move(generated_chunk));

    /// Check can input.
    if (!current_chunk)
    {
        if (input.isFinished())
            return Status::Finished;

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        current_chunk = input.pull(true);
    }

    /// Now consume.
    return Status::Ready;
}

IProcessor::Status SortingTransform::prepareSerialize()
{
    auto & output = outputs.back();

    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (current_chunk)
        output.push(std::move(current_chunk));

    if (merge_sorter)
        return Status::Ready;

    output.finish();
    return Status::Finished;
}

IProcessor::Status SortingTransform::prepareGenerate()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (merge_sorter)
    {
        if (!generated_chunk)
            return Status::Ready;

        output.push(std::move(generated_chunk));
        return Status::PortFull;
    }

    auto & input = inputs.back();

    if (generated_chunk)
        output.push(std::move(generated_chunk));

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    auto chunk = input.pull();
    enrichChunkWithConstants(chunk);
    output.push(std::move(chunk));
    return Status::PortFull;
}

void SortingTransform::work()
{
    if (stage == Stage::Consume)
        consume(std::move(current_chunk));

    if (stage == Stage::Serialize)
        serialize();

    if (stage == Stage::Generate)
        generate();
}

void SortingTransform::removeConstColumns(Chunk & chunk)
{
    size_t num_columns = chunk.getNumColumns();
    size_t num_rows = chunk.getNumRows();

    if (num_columns != const_columns_to_remove.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block has different number of columns with header: {} vs {}",
                        num_columns, const_columns_to_remove.size());

    auto columns = chunk.detachColumns();
    Columns column_without_constants;
    column_without_constants.reserve(header_without_constants.columns());

    for (size_t position = 0; position < num_columns; ++position)
    {
        if (!const_columns_to_remove[position])
            column_without_constants.push_back(std::move(columns[position]));
    }

    chunk.setColumns(std::move(column_without_constants), num_rows);
}

void SortingTransform::enrichChunkWithConstants(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    size_t num_result_columns = const_columns_to_remove.size();

    auto columns = chunk.detachColumns();
    Columns column_with_constants;
    column_with_constants.reserve(num_result_columns);

    const auto & header = inputs.front().getHeader();

    size_t next_non_const_column = 0;
    for (size_t i = 0; i < num_result_columns; ++i)
    {
        if (const_columns_to_remove[i])
            column_with_constants.emplace_back(header.getByPosition(i).column->cloneResized(num_rows));
        else
        {
            if (next_non_const_column >= columns.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't enrich chunk with constants because run out of non-constant columns.");

            column_with_constants.emplace_back(std::move(columns[next_non_const_column]));
            ++next_non_const_column;
        }
    }

    chunk.setColumns(std::move(column_with_constants), num_rows);
}

void SortingTransform::serialize()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'serialize' is not implemented for {} processor", getName());
}

}
