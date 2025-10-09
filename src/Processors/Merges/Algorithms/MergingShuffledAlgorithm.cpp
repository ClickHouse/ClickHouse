#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Merges/Algorithms/MergingShuffledAlgorithm.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

MergingShuffledAlgorithm::MergingShuffledAlgorithm(
    SharedHeader header_,
    size_t num_inputs,
    size_t max_block_size_,
    size_t max_block_size_bytes_,
    UInt64 limit_,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes,
    bool apply_virtual_row_conversions_)
    : header(std::move(header_))
    , merged_data(use_average_block_sizes, max_block_size_, max_block_size_bytes_)
    , limit(limit_)
    , out_row_sources_buf(out_row_sources_buf_)
    , apply_virtual_row_conversions(apply_virtual_row_conversions_)
    , current_inputs(num_inputs)
    , cursors(num_inputs)
{}

void MergingShuffledAlgorithm::addInput()
{
    current_inputs.emplace_back();
    cursors.emplace_back();
}

void MergingShuffledAlgorithm::initialize(Inputs inputs)
{
    for (auto & input : inputs)
    {
        if (!isVirtualRow(input.chunk))
            continue;

        setVirtualRow(input.chunk, *header, apply_virtual_row_conversions);
        input.skip_last_row = true;
    }

    removeConstAndSparse(inputs);
    merged_data.initialize(*header, inputs);
    current_inputs = std::move(inputs);

    for (size_t source_num = 0; source_num < current_inputs.size(); ++source_num)
    {
        auto & chunk = current_inputs[source_num].chunk;
        if (!chunk)
            continue;

        cursors[source_num] = ShuffleCursor(*header, chunk.getColumns(), chunk.getNumRows(), source_num);
    }

    queue = ShufflingQueue(cursors);
}

void MergingShuffledAlgorithm::consume(Input & input, size_t source_num)
{
    removeConstAndSparse(input);
    current_inputs[source_num].swap(input);
    cursors[source_num].reset(current_inputs[source_num].chunk.getColumns(), *header, current_inputs[source_num].chunk.getNumRows());

    queue.push(&cursors[source_num]);
}

IMergingAlgorithm::Status MergingShuffledAlgorithm::merge()
{
    return mergeImpl();
}

IMergingAlgorithm::Status MergingShuffledAlgorithm::mergeImpl()
{
    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        if (merged_data.hasEnoughRows())
            return Status(merged_data.pull());

        auto [current_ptr, index_in_queue] = queue.current();
        auto current = *current_ptr;

        if (current.isLast() && current_inputs[current.order].skip_last_row)
        {
            /// Get the next block from the corresponding source, if there is one.
            queue.removeFromIndex(index_in_queue);
            return Status(current.order);
        }

        merged_data.insertRow(current.all_columns, current.getRow(), current.rows);

        if (out_row_sources_buf)
        {
            RowSourcePart row_source(current.order);
            out_row_sources_buf->write(row_source.data);
        }

        if (limit && merged_data.totalMergedRows() >= limit)
            return Status(merged_data.pull(), true);

        /// If there are still rows in current cursor, then increment its pos
        if (!current.isLast())
        {
            queue.incrementCursorPosByIndex(index_in_queue);
        }
        /// Otherwise remove it from the ShufflingQueue
        else
        {
            /// We will get the next block from the corresponding source, if there is one.

            queue.removeFromIndex(index_in_queue);

            return Status(current.order);
        }
    }

    return Status(merged_data.pull(), true);
}

}
