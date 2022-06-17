#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

MergingSortedAlgorithm::MergingSortedAlgorithm(
    Block header_,
    size_t num_inputs,
    SortDescription description_,
    size_t max_block_size,
    UInt64 limit_,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes)
    : header(std::move(header_))
    , merged_data(header.cloneEmptyColumns(), use_average_block_sizes, max_block_size)
    , description(std::move(description_))
    , limit(limit_)
    , out_row_sources_buf(out_row_sources_buf_)
    , current_inputs(num_inputs)
    , cursors(num_inputs)
{
    DataTypes sort_description_types;
    sort_description_types.reserve(description.size());

    /// Replace column names in description to positions.
    for (auto & column_description : description)
    {
        has_collation |= column_description.collator != nullptr;
        sort_description_types.emplace_back(header.getByName(column_description.column_name).type);
    }

    queue_variants = SortQueueVariants(sort_description_types, description);
    if (queue_variants.variantSupportJITCompilation())
        compileSortDescriptionIfNeeded(description, sort_description_types, true /*increase_compile_attempts*/);
}

void MergingSortedAlgorithm::addInput()
{
    current_inputs.emplace_back();
    cursors.emplace_back();
}

static void prepareChunk(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);
}

void MergingSortedAlgorithm::initialize(Inputs inputs)
{
    current_inputs = std::move(inputs);

    for (size_t source_num = 0; source_num < current_inputs.size(); ++source_num)
    {
        auto & chunk = current_inputs[source_num].chunk;

        if (!chunk)
            continue;

        prepareChunk(chunk);
        cursors[source_num] = SortCursorImpl(header, chunk.getColumns(), description, source_num);
    }

    queue_variants.callOnBatchVariant([&](auto & queue)
    {
        using QueueType = std::decay_t<decltype(queue)>;
        queue = QueueType(cursors);
    });
}

void MergingSortedAlgorithm::consume(Input & input, size_t source_num)
{
    prepareChunk(input.chunk);
    current_inputs[source_num].swap(input);
    cursors[source_num].reset(current_inputs[source_num].chunk.getColumns(), header);

    queue_variants.callOnBatchVariant([&](auto & queue)
    {
        queue.push(cursors[source_num]);
    });
}

IMergingAlgorithm::Status MergingSortedAlgorithm::merge()
{
    IMergingAlgorithm::Status result = queue_variants.callOnBatchVariant([&](auto & queue)
    {
        return mergeBatchImpl(queue);
    });

    return result;
}


template <typename TSortingQueue>
IMergingAlgorithm::Status MergingSortedAlgorithm::mergeBatchImpl(TSortingQueue & queue)
{
    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        if (merged_data.hasEnoughRows())
            return Status(merged_data.pull());

        auto [current_ptr, initial_batch_size] = queue.current();
        auto current = *current_ptr;

        bool batch_skip_last_row = false;
        if (current.impl->isLast(initial_batch_size) && current_inputs[current.impl->order].skip_last_row)
        {
            batch_skip_last_row = true;

            if (initial_batch_size == 1)
            {
                /// Get the next block from the corresponding source, if there is one.
                queue.removeTop();
                return Status(current.impl->order);
            }
        }

        UInt64 merged_rows = merged_data.mergedRows();
        size_t updated_batch_size = initial_batch_size;

        if (merged_rows + updated_batch_size > merged_data.maxBlockSize())
        {
            batch_skip_last_row = false;
            updated_batch_size -= merged_rows + updated_batch_size - merged_data.maxBlockSize();
        }

        bool limit_reached = false;
        if (limit && merged_rows + updated_batch_size > limit)
        {
            batch_skip_last_row = false;
            updated_batch_size -= merged_rows + updated_batch_size - limit;
            limit_reached = true;
        }

        if (unlikely(current.impl->isFirst() && current.impl->isLast(initial_batch_size)))
        {
            /** This is special optimization if current cursor is totally less than next cursor.
              * We want to insert current cursor chunk directly in merged data.
              *
              * First if merged_data is not empty we need to flush it.
              * We will get into the same condition on next mergeBatch call.
              *
              * Then we can insert chunk directly in merged data.
              */
            if (merged_data.mergedRows() != 0)
                return Status(merged_data.pull());

            size_t source_num = current.impl->order;
            size_t insert_rows_size = initial_batch_size - static_cast<size_t>(batch_skip_last_row);
            merged_data.insertChunk(std::move(current_inputs[source_num].chunk), insert_rows_size);
            current_inputs[source_num].chunk = Chunk();

            if (out_row_sources_buf)
            {
                RowSourcePart row_source(current.impl->order);

                for (size_t i = 0; i < insert_rows_size; ++i)
                    out_row_sources_buf->write(row_source.data);
            }

            if (limit_reached)
                break;

            /// We will get the next block from the corresponding source, if there is one.
            queue.removeTop();

            auto result = Status(merged_data.pull(), limit_reached);
            if (!limit_reached)
                result.required_source = source_num;

            return result;
        }

        size_t insert_rows_size = updated_batch_size - static_cast<size_t>(batch_skip_last_row);
        merged_data.insertRows(current->all_columns, current->getRow(), insert_rows_size, current->rows);

        if (out_row_sources_buf)
        {
            RowSourcePart row_source(current.impl->order);

            for (size_t i = 0; i < insert_rows_size; ++i)
                out_row_sources_buf->write(row_source.data);
        }

        if (limit_reached)
            break;

        if (!current->isLast(updated_batch_size))
        {
            queue.next(updated_batch_size);
        }
        else
        {
            /// We will get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    return Status(merged_data.pull(), true);
}

}
