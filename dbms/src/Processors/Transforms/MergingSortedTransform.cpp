#include <Processors/Transforms/MergingSortedTransform.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteBuffer.h>
#include <DataStreams/materializeBlock.h>

namespace DB
{

MergingSortedTransform::MergingSortedTransform(
    const Block & header,
    size_t num_inputs,
    const SortDescription & description_,
    size_t max_block_size_,
    UInt64 limit_,
    bool quiet_,
    bool have_all_inputs_)
    : IProcessor(InputPorts(num_inputs, header), {materializeBlock(header)})
    , description(description_), max_block_size(max_block_size_), limit(limit_), quiet(quiet_)
    , have_all_inputs(have_all_inputs_)
    , merged_data(header), source_chunks(num_inputs), cursors(num_inputs)
{
    auto & sample = outputs.front().getHeader();
    /// Replace column names in description to positions.
    for (auto & column_description : description)
    {
        has_collation |= column_description.collator != nullptr;
        if (!column_description.column_name.empty())
        {
            column_description.column_number = sample.getPositionByName(column_description.column_name);
            column_description.column_name.clear();
        }
    }
}

void MergingSortedTransform::addInput()
{
    if (have_all_inputs)
        throw Exception("MergingSortedTransform already have all inputs.", ErrorCodes::LOGICAL_ERROR);

    inputs.emplace_back(outputs.front().getHeader(), this);
    source_chunks.emplace_back();
    cursors.emplace_back();
}

void MergingSortedTransform::setHaveAllInputs()
{
    if (have_all_inputs)
        throw Exception("MergingSortedTransform already have all inputs.", ErrorCodes::LOGICAL_ERROR);

    have_all_inputs = true;
}

IProcessor::Status MergingSortedTransform::prepare()
{
    if (!have_all_inputs)
        return Status::NeedData;

    auto & output = outputs.front();

    /// Special case for no inputs.
    if (inputs.empty())
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.

    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();

        return Status::Finished;
    }

    if (!output.isNeeded())
    {
        for (auto & in : inputs)
            in.setNotNeeded();

        return Status::PortFull;
    }

    if (output.hasData())
        return Status::PortFull;

    /// Special case for single input.
    if (inputs.size() == 1)
    {
        auto & input = inputs.front();
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();
        if (input.hasData())
            output.push(input.pull());

        return Status::NeedData;
    }

    /// Push if has data.
    if (merged_data.mergedRows())
        output.push(merged_data.pull());

    if (!is_initialized)
    {
        /// Check for inputs we need.
        bool all_inputs_has_data = true;
        auto it = inputs.begin();
        for (size_t i = 0; it != inputs.end(); ++i, ++it)
        {
            auto & input = *it;
            if (input.isFinished())
                continue;

            if (!cursors[i].empty())
            {
                input.setNotNeeded();
                continue;
            }

            input.setNeeded();

            if (!input.hasData())
            {
                all_inputs_has_data = false;
                continue;
            }

            auto chunk = input.pull();
            if (chunk.hasNoRows())
            {
                all_inputs_has_data = false;
                continue;
            }

            updateCursor(std::move(chunk), i);
        }

        if (!all_inputs_has_data)
            return Status::NeedData;

        if (has_collation)
            initQueue(queue_with_collation);
        else
            initQueue(queue_without_collation);

        is_initialized = true;
        return Status::Ready;
    }
    else
    {
        if (is_finished)
        {
            for (auto & input : inputs)
                input.close();

            outputs.front().finish();

            return Status::Finished;
        }

        if (need_data)
        {

            auto & input = *std::next(inputs.begin(), next_input_to_read);
            if (!input.isFinished())
            {
                input.setNeeded();

                if (!input.hasData())
                    return Status::NeedData;

                auto chunk = input.pull();
                if (chunk.hasNoRows())
                    return Status::NeedData;

                updateCursor(std::move(chunk), next_input_to_read);
                pushToQueue(next_input_to_read);
                need_data = false;
            }
        }

        return Status::Ready;
    }
}

void MergingSortedTransform::work()
{
    if (has_collation)
        merge(queue_with_collation);
    else
        merge(queue_without_collation);
}

template <typename TSortCursor>
void MergingSortedTransform::merge(std::priority_queue<TSortCursor> & queue)
{
    /// Returns MergeStatus which we should return if we are going to finish now.
    auto can_read_another_row = [&, this]()
    {
        if (limit && merged_data.totalMergedRows() >= limit)
        {
            //std::cerr << "Limit reached\n";
            is_finished = true;
            return false;
        }

        if (merged_data.mergedRows() >= max_block_size)
        {
            //std::cerr << "max_block_size reached\n";
            return false;
        }

        return true;
    };

    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (!queue.empty())
    {
        /// Shouldn't happen at first iteration, but check just in case.
        if (!can_read_another_row())
            return;

        TSortCursor current = queue.top();
        queue.pop();
        bool first_iteration = true;

        while (true)
        {
            if (!first_iteration && !can_read_another_row())
            {
                queue.push(current);
                return;
            }
            first_iteration = false;

            /** And what if the block is totally less or equal than the rest for the current cursor?
              * Or is there only one data source left in the queue? Then you can take the entire block on current cursor.
              */
            if (current.impl->isFirst() && (queue.empty() || current.totallyLessOrEquals(queue.top())))
            {
                //std::cerr << "current block is totally less or equals\n";

                /// If there are already data in the current block, we first return it. We'll get here again the next time we call the merge function.
                if (merged_data.mergedRows() != 0)
                {
                    //std::cerr << "merged rows is non-zero\n";
                    queue.push(current);
                    return;
                }

                /// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
                size_t source_num = current.impl->order;
                insertFromChunk(source_num);
                return;
            }

            //std::cerr << "total_merged_rows: " << total_merged_rows << ", merged_rows: " << merged_rows << "\n";
            //std::cerr << "Inserting row\n";
            merged_data.insertRow(current->all_columns, current->pos);

            if (out_row_sources_buf)
            {
                /// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
                RowSourcePart row_source(current.impl->order);
                out_row_sources_buf->write(row_source.data);
            }

            if (current->isLast())
            {
                need_data = true;
                next_input_to_read = current.impl->order;

                if (limit && merged_data.totalMergedRows() >= limit)
                    is_finished = true;

                return;
            }

            //std::cerr << "moving to next row\n";
            current->next();

            if (!queue.empty() && current.greater(queue.top()))
            {
                //std::cerr << "next row is not least, pushing back to queue\n";
                queue.push(current);
                break;
            }
        }
    }
    is_finished = true;
}

void MergingSortedTransform::insertFromChunk(size_t source_num)
{
    if (source_num >= cursors.size())
        throw Exception("Logical error in MergingSortedTrandform", ErrorCodes::LOGICAL_ERROR);

    //std::cerr << "copied columns\n";

    auto num_rows = source_chunks[source_num]->getNumRows();

    UInt64 total_merged_rows_after_insertion = merged_data.mergedRows() + num_rows;
    if (limit && total_merged_rows_after_insertion > limit)
    {
        num_rows = total_merged_rows_after_insertion - limit;
        merged_data.insertFromChunk(std::move(*source_chunks[source_num]), num_rows);
        is_finished = true;
    }
    else
    {
        merged_data.insertFromChunk(std::move(*source_chunks[source_num]), 0);
        need_data = true;
        next_input_to_read = source_num;
    }

    if (out_row_sources_buf)
    {
        RowSourcePart row_source(source_num);
        for (size_t i = 0; i < num_rows; ++i)
            out_row_sources_buf->write(row_source.data);
    }
}


}
