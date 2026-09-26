#include <Processors/Transforms/SortingTransform.h>

#include <algorithm>
#include <type_traits>

#include <Columns/ColumnReplicated.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

MergeSorter::MergeSorter(
    SharedHeader header, Chunks chunks_, const SortDescription & description,
    size_t max_merged_block_size_, UInt64 limit_, Mode mode_)
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

    queue_variants.callOnBatchVariant([&](auto & queue)
    {
        using QueueType = std::decay_t<decltype(queue)>;
        queue = QueueType(cursors);
    });
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
    size_t num_columns = chunks[0].getNumColumns();
    MutableColumns merged_columns = createMergedColumns();

    size_t size_to_reserve = 0;
    for (const auto & chunk : chunks)
        size_to_reserve += chunk.getNumRows();

    /// Reserve at most one output block because reserved capacity counts toward tracked memory.
    size_to_reserve = std::min(size_to_reserve, max_merged_block_size);
    for (auto & column : merged_columns)
        column->reserve(size_to_reserve);

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
            for (size_t i = 0; i < num_columns; ++i)
            {
                if (rows_to_insert == 1)
                    merged_columns[i]->insertFrom(*current->all_columns[i], first_row + skipped_rows);
                else
                    merged_columns[i]->insertRangeFrom(*current->all_columns[i], first_row + skipped_rows, rows_to_insert);
            }

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

    if (limit_reached || !queue.isValid())
    {
        last_emitted_cursor = nullptr;
        chunks.clear();
    }

    if (merged_rows == 0 && chunks.empty())
        return {};

    return Chunk(std::move(merged_columns), merged_rows);
}

MutableColumns MergeSorter::createMergedColumns() const
{
    size_t num_columns = chunks[0].getNumColumns();
    std::vector<bool> is_replicated(num_columns, false);
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
