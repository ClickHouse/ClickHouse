#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

MergingSortedAlgorithm::MergingSortedAlgorithm(
    SharedHeader header_,
    size_t num_inputs,
    const SortDescription & description_,
    size_t max_block_size_,
    size_t max_block_size_bytes_,
    std::optional<size_t> max_dynamic_subcolumns_,
    SortingQueueStrategy sorting_queue_strategy_,
    UInt64 limit_,
    WriteBuffer * out_row_sources_buf_,
    const std::optional<String> & filter_column_name_,
    bool use_average_block_sizes,
    bool apply_virtual_row_conversions_)
    : header(std::move(header_))
    , merged_data(use_average_block_sizes, max_block_size_, max_block_size_bytes_, max_dynamic_subcolumns_)
    , description(description_)
    , limit(limit_)
    , out_row_sources_buf(out_row_sources_buf_)
    , filter_column_position(filter_column_name_ ? header->getPositionByName(filter_column_name_.value()) : -1)
    , apply_virtual_row_conversions(apply_virtual_row_conversions_)
    , current_inputs(num_inputs)
    , sorting_queue_strategy(sorting_queue_strategy_)
    , cursors(num_inputs)
{
    if (filter_column_position != -1)
    {
        const auto & filter_type = header->getByPosition(filter_column_position).type;
        if (!WhichDataType(filter_type).isUInt8())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER, "Illegal type {} of column for filter. Must be UInt8", filter_type->getName());
    }

    DataTypes sort_description_types;
    sort_description_types.reserve(description.size());

    /// Replace column names in description to positions.
    for (const auto & column_description : description)
    {
        has_collation |= column_description.collator != nullptr;
        sort_description_types.emplace_back(header->getByName(column_description.column_name).type);
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

void MergingSortedAlgorithm::initialize(Inputs inputs)
{
    for (auto & input : inputs)
    {
        if (!isVirtualRow(input.chunk))
            continue;

        setVirtualRow(input.chunk, *header, apply_virtual_row_conversions);
        input.skip_last_row = true;
    }

    removeReplicatedFromSortingColumns(header, inputs, description);
    removeConstAndSparse(inputs);
    merged_data.initialize(*header, inputs);
    current_inputs = std::move(inputs);

    for (size_t source_num = 0; source_num < current_inputs.size(); ++source_num)
    {
        auto & chunk = current_inputs[source_num].chunk;
        if (!chunk)
            continue;

        cursors[source_num] = SortCursorImpl(*header, chunk.getColumns(), chunk.getNumRows(), description, source_num);
    }

    if (sorting_queue_strategy == SortingQueueStrategy::Default)
    {
        queue_variants.callOnVariant([&](auto & queue)
        {
            using QueueType = std::decay_t<decltype(queue)>;
            queue = QueueType(cursors);
        });
    }
    else
    {
        queue_variants.callOnBatchVariant([&](auto & queue)
        {
            using QueueType = std::decay_t<decltype(queue)>;
            queue = QueueType(cursors);
        });
    }
}

void MergingSortedAlgorithm::consume(Input & input, size_t source_num)
{
    removeReplicatedFromSortingColumns(header, input, description);
    removeConstAndSparse(input);
    current_inputs[source_num].swap(input);
    cursors[source_num].reset(current_inputs[source_num].chunk.getColumns(), *header, current_inputs[source_num].chunk.getNumRows());

    if (sorting_queue_strategy == SortingQueueStrategy::Default)
    {
        queue_variants.callOnVariant([&](auto & queue)
        {
            queue.push(cursors[source_num]);
        });
    }
    else
    {
        queue_variants.callOnBatchVariant([&](auto & queue)
        {
            queue.push(cursors[source_num]);
        });
    }
}

IMergingAlgorithm::Status MergingSortedAlgorithm::merge()
{
    if (sorting_queue_strategy == SortingQueueStrategy::Default)
    {
        IMergingAlgorithm::Status result = queue_variants.callOnVariant([&](auto & queue)
        {
            return mergeImpl(queue);
        });

        return result;
    }

    IMergingAlgorithm::Status result = queue_variants.callOnBatchVariant([&](auto & queue)
    {
        return mergeBatchImpl(queue);
    });

    return result;
}

void MergingSortedAlgorithm::insertRow(const SortCursorImpl & current)
{
    auto write_row_source = [&](bool skipped)
    {
        if (out_row_sources_buf)
        {
            RowSourcePart row_source(current.order, skipped);
            out_row_sources_buf->write(row_source.data);
        }
    };

    size_t current_row = current.getRow();

    if (hasFilter())
    {
        const auto & filter_column = current.all_columns[filter_column_position];
        const auto & filter_data = assert_cast<const ColumnUInt8 &>(*filter_column).getData();

        if (filter_data[current_row])
            merged_data.insertRow(current.all_columns, current_row, current.rows);

        write_row_source(!filter_data[current_row]);
    }
    else
    {
        merged_data.insertRow(current.all_columns, current_row, current.rows);
        write_row_source(false);
    }
}

void MergingSortedAlgorithm::insertRows(const SortCursorImpl & current, size_t num_rows)
{
    if (hasFilter())
    {
        const auto & filter_column = current.all_columns[filter_column_position];
        const auto & filter_data = assert_cast<const ColumnUInt8 &>(*filter_column).getData();

        size_t start_index = current.getRow();
        RowSourcePart row_source(current.order, false);
        RowSourcePart row_source_skipped(current.order, true);

        for (size_t i = start_index; i < start_index + num_rows; ++i)
        {
            if (filter_data[i])
            {
                merged_data.insertRow(current.all_columns, i, current.rows);
                out_row_sources_buf->write(row_source.data);
            }
            else
            {
                out_row_sources_buf->write(row_source_skipped.data);
            }
        }
    }
    else
    {
        merged_data.insertRows(current.all_columns, current.getRow(), num_rows, current.rows);

        if (out_row_sources_buf)
        {
            RowSourcePart row_source(current.order);

            for (size_t i = 0; i < num_rows; ++i)
                out_row_sources_buf->write(row_source.data);
        }
    }
}

void MergingSortedAlgorithm::insertChunk(size_t source_num)
{
    Chunk chunk = std::move(current_inputs[source_num].chunk);
    size_t chunk_num_rows = chunk.getNumRows();

    if (hasFilter())
    {
        auto columns = chunk.detachColumns();

        const auto & filter_column = columns[filter_column_position];
        const auto & filter_data = assert_cast<const ColumnUInt8 &>(*filter_column).getData();

        if (out_row_sources_buf)
        {
            RowSourcePart row_source(source_num, false);
            RowSourcePart row_source_skipped(source_num, true);

            for (size_t i = 0; i < chunk_num_rows; ++i)
                out_row_sources_buf->write(filter_data[i] ? row_source.data : row_source_skipped.data);
        }

        for (auto & column : columns)
        {
            column = column->filter(filter_data, -1);
        }

        chunk_num_rows = columns.empty() ? 0 : columns.front()->size();
        merged_data.insertChunk(Chunk(std::move(columns), chunk_num_rows), chunk_num_rows);
    }
    else
    {
        if (out_row_sources_buf)
        {
            RowSourcePart row_source(source_num);
            for (size_t i = 0; i < chunk_num_rows; ++i)
                out_row_sources_buf->write(row_source.data);
        }

        merged_data.insertChunk(std::move(chunk), chunk_num_rows);
    }
}

template <typename TSortingHeap>
IMergingAlgorithm::Status MergingSortedAlgorithm::mergeImpl(TSortingHeap & queue)
{
    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        if (merged_data.hasEnoughRows())
            return Status(merged_data.pull());

        auto current = queue.current();

        if (current.impl->isLast() && current_inputs[current.impl->order].skip_last_row)
        {
            /// Get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }

        if (current.impl->isFirst()
            && !current_inputs[current.impl->order].skip_last_row /// Ignore optimization if last row should be skipped.
            && (queue.size() == 1
                || (queue.size() >= 2 && current.totallyLessOrEquals(queue.nextChild()))))
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

            /// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
            size_t source_num = current.impl->order;

            auto chunk_num_rows = current_inputs[source_num].chunk.getNumRows();

            UInt64 total_merged_rows_after_insertion = merged_data.mergedRows() + chunk_num_rows;
            bool limit_reached = limit && total_merged_rows_after_insertion >= limit;

            if (limit && total_merged_rows_after_insertion > limit)
                chunk_num_rows -= total_merged_rows_after_insertion - limit;

            insertChunk(source_num);

            /// We will get the next block from the corresponding source, if there is one.
            queue.removeTop();

            auto status = Status(merged_data.pull(), limit_reached);

            if (!limit_reached)
                status.required_source = source_num;

            return status;
        }

        insertRow(*current.impl);

        if (limit && merged_data.totalMergedRows() >= limit)
            return Status(merged_data.pull(), true);

        if (!current->isLast())
        {
            queue.next();
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

        if (limit && merged_rows + updated_batch_size >= limit && !batch_skip_last_row)
        {
            updated_batch_size -= merged_rows + updated_batch_size - limit;
            limit_reached = true;
        }
        else if (limit && merged_rows + updated_batch_size > limit)
        {
            batch_skip_last_row = false;
            updated_batch_size -= merged_rows + updated_batch_size - limit;
            limit_reached = true;
        }

        if (unlikely(current.impl->isFirst() &&
            current.impl->isLast(initial_batch_size) &&
            !current_inputs[current.impl->order].skip_last_row))
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

            insertChunk(current.impl->order);

            /// We will get the next block from the corresponding source, if there is one.
            queue.removeTop();

            auto result = Status(merged_data.pull(), limit_reached);
            if (!limit_reached)
                result.required_source = current.impl->order;

            return result;
        }

        size_t insert_rows_size = updated_batch_size - static_cast<size_t>(batch_skip_last_row);
        insertRows(*current.impl, insert_rows_size);

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
