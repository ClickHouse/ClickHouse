#include <Processors/Transforms/WindowTransform.h>

#include <Interpreters/ExpressionActions.h>

#include <Common/Arena.h>

namespace DB
{

WindowTransform::WindowTransform(const Block & input_header_,
        const Block & output_header_,
        const WindowDescription & window_description_,
        const std::vector<WindowFunctionDescription> & functions)
    : IProcessor({input_header_}, {output_header_})
    , input(inputs.front())
    , output(outputs.front())
    , input_header(input_header_)
    , window_description(window_description_)
{
    workspaces.reserve(functions.size());
    for (const auto & f : functions)
    {
        WindowFunctionWorkspace workspace;
        workspace.window_function = f;

        const auto & aggregate_function
            = workspace.window_function.aggregate_function;
        if (!arena && aggregate_function->allocatesMemoryInArena())
        {
            arena = std::make_unique<Arena>();
        }

        workspace.argument_column_indices.reserve(
            workspace.window_function.argument_names.size());
        for (const auto & argument_name : workspace.window_function.argument_names)
        {
            workspace.argument_column_indices.push_back(
                input_header.getPositionByName(argument_name));
        }

        workspace.aggregate_function_state.reset(aggregate_function->sizeOfData(),
            aggregate_function->alignOfData());
        aggregate_function->create(workspace.aggregate_function_state.data());

        workspaces.push_back(std::move(workspace));
    }

    partition_by_indices.reserve(window_description.partition_by.size());
    for (const auto & column : window_description.partition_by)
    {
        partition_by_indices.push_back(
            input_header.getPositionByName(column.column_name));
    }

    order_by_indices.reserve(window_description.order_by.size());
    for (const auto & column : window_description.order_by)
    {
        order_by_indices.push_back(
            input_header.getPositionByName(column.column_name));
    }
}

WindowTransform::~WindowTransform()
{
    // Some states may be not created yet if the creation failed.
    for (auto & ws : workspaces)
    {
        ws.window_function.aggregate_function->destroy(
            ws.aggregate_function_state.data());
    }
}

void WindowTransform::advancePartitionEnd()
{
    if (partition_ended)
    {
        return;
    }

    const RowNumber end = blocksEnd();

    // If we're at the total end of data, we must end the partition. This is the
    // only place in calculations where we need special handling for end of data,
    // other places will work as usual based on `partition_ended` = true, because
    // end of data is logically the same as any other end of partition.
    // We must check this first, because other calculations might not be valid
    // when we're at the end of data.
    // FIXME not true, we also handle it elsewhere
    if (input_is_finished)
    {
        partition_ended = true;
        partition_end = end;
        return;
    }

    // Try to advance the partition end pointer.
    const size_t n = partition_by_indices.size();
    if (n == 0)
    {
//        fmt::print(stderr, "no partition by\n");
        // No PARTITION BY. All input is one partition, which will end when the
        // input ends.
        partition_end = end;
        return;
    }

    // The partition ends when the PARTITION BY columns change. We need an array
    // of reference columns for comparison. We might have already dropped the
    // blocks where the partition starts, but any row in the partition will do.
    // Use group_start -- it's always in the valid region, because it points to
    // the start of the current group, which we haven't fully processed yet, and
    // hence cannot drop.
    auto reference_row = group_start;
    if (reference_row == partition_end)
    {
        // This is for the very first partition and its first row. Try to get
        // rid of this logic.
        advanceRowNumber(partition_end);
    }
    assert(reference_row < blocksEnd());
    assert(reference_row.block >= first_block_number);
    Columns reference_partition_by;
    for (const auto i : partition_by_indices)
    {
        reference_partition_by.push_back(inputAt(reference_row)[i]);
    }

//    fmt::print(stderr, "{} cols to compare, reference at {}\n", n, group_start);

    for (; partition_end < end; advanceRowNumber(partition_end))
    {
        // Check for partition end.
        size_t i = 0;
        for (; i < n; i++)
        {
            const auto * c = inputAt(partition_end)[partition_by_indices[i]].get();
            if (c->compareAt(partition_end.row,
                    group_start.row, *reference_partition_by[i],
                    1 /* nan_direction_hint */) != 0)
            {
                break;
            }
        }

        if (i < n)
        {
//            fmt::print(stderr, "col {} doesn't match at {}: ref {}, val {}\n",
//                i, partition_end, inputAt(partition_end)[i]);
            partition_ended = true;
            return;
        }
    }

    // Went until the end of data and didn't find the new partition.
    assert(!partition_ended && partition_end == blocksEnd());
}

void WindowTransform::advanceGroupEnd()
{
    if (group_ended)
    {
        return;
    }

    switch (window_description.frame.type)
    {
        case WindowFrame::FrameType::Groups:
            advanceGroupEndGroups();
            break;
        case WindowFrame::FrameType::Rows:
            advanceGroupEndRows();
            break;
        case WindowFrame::FrameType::Range:
            assert(false);
            break;
    }
}

void WindowTransform::advanceGroupEndRows()
{
    // ROWS mode, peer groups always contains only the current row.
    // We cannot advance the groups if the group start is already beyond the
    // end of partition.
    assert(group_start < partition_end);
    group_end = group_start;
    advanceRowNumber(group_end);
    group_ended = true;
}

void WindowTransform::advanceGroupEndGroups()
{
    const size_t n = order_by_indices.size();
    if (n == 0)
    {
        // No ORDER BY, so all rows are the same group. The group will end
        // with the partition.
        group_end = partition_end;
        group_ended = partition_ended;
    }

    Columns reference_order_by;
    for (const auto i : order_by_indices)
    {
        reference_order_by.push_back(inputAt(group_start)[i]);
    }

    // `partition_end` is either end of partition or end of data.
    for (; group_end < partition_end; advanceRowNumber(group_end))
    {
        // Check for group end.
        size_t i = 0;
        for (; i < n; i++)
        {
            const auto * c = inputAt(partition_end)[partition_by_indices[i]].get();
            if (c->compareAt(group_end.row,
                    group_start.row, *reference_order_by[i],
                    1 /* nan_direction_hint */) != 0)
            {
                break;
            }
        }

        if (i < n)
        {
            group_ended = true;
            return;
        }
    }

    assert(group_end == partition_end);
    if (partition_ended)
    {
        // A corner case -- the ORDER BY columns were the same, but the group
        // still ended because the partition has ended.
        group_ended = true;
    }
}

void WindowTransform::advanceFrameStart()
{
    // Frame start is always UNBOUNDED PRECEDING for now, so we don't have to
    // move it. It is initialized when the new partition starts.
}

void WindowTransform::advanceFrameEnd()
{
    // This should be called when we know the boundaries of the group (probably
    // not a fundamental requirement, but currently it's written this way).
    assert(group_ended);

    const auto frame_end_before = frame_end;

    // Frame end is always the current group end, for now.
    // In ROWS mode the group is going to contain only the current row.
    frame_end = group_end;
    frame_ended = true;

    // Add the columns over which we advanced the frame to the aggregate function
    // states.
    std::vector<const IColumn *> argument_columns;
    for (auto & ws : workspaces)
    {
        const auto & f = ws.window_function;
        const auto * a = f.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        // We use two explicit loops here instead of using advanceRowNumber(),
        // because we want to cache the argument columns array per block. Later
        // we also use batch add.
        // Unfortunately this leads to tricky loop conditions, because the
        // frame_end might be either a past-the-end block, or a valid block, in
        // which case we also have to process its head.
        // And we also have to remember to reset the row number when moving to
        // the next block.

        uint64_t past_the_end_block;
        // Note that the past-the-end row is not in the past-the-end block, but
        // in the block before it.
        uint32_t past_the_end_row;

        if (frame_end.block < first_block_number + blocks.size())
        {
            // The past-the-end row is in some valid block.
            past_the_end_block = frame_end.block + 1;
            past_the_end_row = frame_end.row;
        }
        else
        {
            // The past-the-end row is at the total end of data.
            past_the_end_block = first_block_number + blocks.size();
            // It's in the previous block!
            past_the_end_row = blocks.back().numRows();
        }
        for (auto r = frame_end_before;
            r.block < past_the_end_block;
            ++r.block, r.row = 0)
        {
            const auto & block = blocks[r.block - first_block_number];

            argument_columns.clear();
            for (const auto i : ws.argument_column_indices)
            {
                argument_columns.push_back(block.input_columns[i].get());
            }

            // We process all rows of intermediate blocks, and the head of the
            // last block.
            const auto end = ((r.block + 1) == past_the_end_block)
                ? past_the_end_row
                : block.numRows();
            for (; r.row < end; ++r.row)
            {
                a->add(buf,
                    argument_columns.data(),
                    r.row,
                    arena.get());
            }
        }
    }
}

void WindowTransform::writeOutGroup()
{
//    fmt::print(stderr, "write out group [{}..{})\n",
//        group_start, group_end);

    // Empty groups don't make sense.
    assert(group_start < group_end);

    std::vector<const IColumn *> argument_columns;
    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];
        const auto & f = ws.window_function;
        const auto * a = f.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        // Need to use a tricky loop to be able to batch per-block (but we don't
        // do it yet...). See the comments to the similar loop in
        // advanceFrameEnd() above.
        uint64_t past_the_end_block;
        uint32_t past_the_end_row;
        if (frame_end.block < first_block_number + blocks.size())
        {
            past_the_end_block = frame_end.block + 1;
            past_the_end_row = frame_end.row;
        }
        else
        {
            past_the_end_block = first_block_number + blocks.size();
            past_the_end_row = blocks.back().numRows();
        }
        for (auto r = group_start;
            r.block < past_the_end_block;
            ++r.block, r.row = 0)
        {
            const auto & block = blocks[r.block - first_block_number];

            argument_columns.clear();
            for (const auto ai : ws.argument_column_indices)
            {
                argument_columns.push_back(block.input_columns[ai].get());
            }

            // We process all rows of intermediate blocks, and the head of the
            // last block.
            const auto end = ((r.block + 1) == past_the_end_block)
                ? past_the_end_row
                : block.numRows();
            for (; r.row < end; ++r.row)
            {
                // FIXME does it also allocate the result on the arena?
                // We'll have to pass it out with blocks then...
                a->insertResultInto(buf,
                    *block.output_columns[wi],
                    arena.get());
            }
        }
    }

    first_not_ready_row = group_end;
}

void WindowTransform::appendChunk(Chunk & chunk)
{
//    fmt::print(stderr, "new chunk, {} rows, finished={}\n", chunk.getNumRows(),
//        input_is_finished);

    // First, prepare the new input block and add it to the queue. We might not
    // have it if it's end of data, though.
    if (!input_is_finished)
    {
        blocks.push_back({});
        auto & block = blocks.back();
        block.input_columns = chunk.detachColumns();

        for (auto & ws : workspaces)
        {
            // Aggregate functions can't work with constant columns, so we have to
            // materialize them like the Aggregator does.
            for (const auto column_index : ws.argument_column_indices)
            {
                block.input_columns[column_index]
                    = std::move(block.input_columns[column_index])
                        ->convertToFullColumnIfConst();
            }

            block.output_columns.push_back(ws.window_function.aggregate_function
                ->getReturnType()->createColumn());
        }
    }

    // Start the calculations. First, advance the partition end.
    for (;;)
    {
        advancePartitionEnd();

        // Either we ran out of data or we found the end of partition (maybe
        // both, but this only happens at the total end of data).
        assert(partition_ended || partition_end == blocksEnd());
        if (partition_ended && partition_end == blocksEnd())
        {
            assert(input_is_finished);
        }

//        fmt::print(stderr, "partition end '{}', {}\n", partition_end,
//            partition_ended);

        // After that, advance the peer groups. We can advance peer groups until
        // the end of partition or current end of data, which is precisely the
        // description of `partition_end`.
        while (group_end < partition_end)
        {
            group_start = group_end;
            advanceGroupEnd();

//            fmt::print(stderr, "group end '{}'\n", group_end);

            // If the group didn't end yet, wait.
            if (!group_ended)
            {
                return;
            }

            // The group ended.
            // Advance the frame start, updating the state of the aggregate
            // functions.
            advanceFrameStart();
            // Advance the frame end, updating the state of the aggregate
            // functions.
            advanceFrameEnd();

            if (!frame_ended)
            {
                return;
            }

            // Write out the aggregation results
            writeOutGroup();

            // Move to the next group.
            // The frame will have to be recalculated.
            frame_ended = false;

            // Move to the next group. Don't advance group_start yet, it's
            // convenient to use it as the PARTITION BY etalon.
            group_ended = false;

            if (group_end == partition_end)
            {
                break;
            }
            assert(group_end < partition_end);
        }

        if (!partition_ended)
        {
            // We haven't encountered the end of the partition yet, need more
            // data.
            assert(partition_end == blocksEnd());
            break;
        }

        if (input_is_finished)
        {
            // why?
            return;
        }

        // Start the next partition.
        const auto new_partition_start = partition_end;
        advanceRowNumber(partition_end);
        partition_ended = false;
        // We have to reset the frame when the new partition starts. This is not a
        // generally correct way to do so, but we don't really support moving frame
        // for now.
        frame_start = new_partition_start;
        frame_end = new_partition_start;
        group_start = new_partition_start;
        group_end = new_partition_start;
        // The group pointers are already reset to the partition start, see the
        // above loop.

//        fmt::print(stderr, "reinitialize agg data at start of {}\n",
//            new_partition_start);
        // Reinitialize the aggregate function states because the new partition
        // has started.
        for (auto & ws : workspaces)
        {
            const auto & f = ws.window_function;
            const auto * a = f.aggregate_function.get();
            auto * buf = ws.aggregate_function_state.data();

            a->destroy(buf);
        }

        // Release the arena we use for aggregate function states, so that it
        // doesn't grow without limit. Not sure if it's actually correct, maybe
        // it allocates the return values in the Arena as well...
        if (arena)
        {
            arena = std::make_unique<Arena>();
        }

        for (auto & ws : workspaces)
        {
            const auto & f = ws.window_function;
            const auto * a = f.aggregate_function.get();
            auto * buf = ws.aggregate_function_state.data();

            a->create(buf);
        }
    }
}

IProcessor::Status WindowTransform::prepare()
{
//    fmt::print(stderr, "prepare, next output {}, not ready row {}, first block {}, hold {} blocks\n",
//        next_output_block_number, first_not_ready_row, first_block_number,
//        blocks.size());

    if (output.isFinished())
    {
        // The consumer asked us not to continue (or we decided it ourselves),
        // so we abort.
        input.close();
        return Status::Finished;
    }

//    // Technically the past-the-end next_output_block_number is also valid if
//    // we haven't yet received the corresponding input block.
//    assert(next_output_block_number < first_block_number + blocks.size()
//        || blocks.empty());

    assert(first_not_ready_row.block >= first_block_number);
    // Might be past-the-end, so equality also valid.
    assert(first_not_ready_row.block <= first_block_number + blocks.size());
    assert(next_output_block_number >= first_block_number);

    // Output the ready data prepared by work().
    // We inspect the calculation state and create the output chunk right here,
    // because this is pretty lightweight.
    if (next_output_block_number < first_not_ready_row.block)
    {
        if (output.canPush())
        {
            // Output the ready block.
//            fmt::print(stderr, "output block {}\n", next_output_block_number);
            const auto i = next_output_block_number - first_block_number;
            ++next_output_block_number;
            auto & block = blocks[i];
            auto columns = block.input_columns;
            for (auto & res : block.output_columns)
            {
                columns.push_back(ColumnPtr(std::move(res)));
            }
            output_data.chunk.setColumns(columns, block.numRows());

            output.pushData(std::move(output_data));
        }
        else
        {
            // Not sure what this branch means. The output port is full and we
            // apply backoff pressure on the input?
            input.setNotNeeded();
        }

        return Status::PortFull;
    }

    if (input_is_finished)
    {
        // The input data ended at the previous prepare() + work() cycle,
        // and we don't have ready output data (checked above). We must be
        // finished.
        assert(next_output_block_number == first_block_number + blocks.size());
        assert(first_not_ready_row == blocksEnd());

        // FIXME do we really have to do this?
        output.finish();

        return Status::Finished;
    }

    // Consume input data if we have any ready.
    if (!has_input && input.hasData())
    {
        input_data = input.pullData(true /* set_not_needed */);
        has_input = true;

        // Now we have new input and can try to generate more output in work().
        return Status::Ready;
    }

    // We 1) don't have any ready output (checked above),
    // 2) don't have any more input (also checked above).
    // Will we get any more input?
    if (input.isFinished())
    {
        // We won't, time to finalize the calculation in work(). We should only
        // do this once.
        assert(!input_is_finished);
        input_is_finished = true;
        return Status::Ready;
    }

    // We have to wait for more input.
    input.setNeeded();
    return Status::NeedData;
}

void WindowTransform::work()
{
    if (input_data.exception)
    {
        /// Skip transform in case of exception.
        output_data = std::move(input_data);
        has_input = false;
        has_output = true;
        return;
    }

    assert(has_input || input_is_finished);

    try
    {
        has_input = false;
        appendChunk(input_data.chunk);
    }
    catch (DB::Exception &)
    {
        output_data.exception = std::current_exception();
        has_output = true;
        has_input = false;
        return;
    }

    // We don't really have to keep the entire partition, and it can be big, so
    // we want to drop the starting blocks to save memory.
    // We can drop the old blocks if we already returned them as output, and the
    // frame and group are already past them. Note that the frame start can be
    // further than group start for some frame specs, so we have to check both.
    const auto first_used_block = std::min(next_output_block_number,
            std::min(frame_start.block, group_start.block));
    if (first_block_number < first_used_block)
    {
//        fmt::print(stderr, "will drop blocks from {} to {}\n", first_block_number,
//            first_used_block);

        blocks.erase(blocks.begin(),
            blocks.begin() + first_used_block - first_block_number);
        first_block_number = first_used_block;

        assert(next_output_block_number >= first_block_number);
        assert(frame_start.block >= first_block_number);
        assert(group_start.block >= first_block_number);
    }
}


}
