#include <Processors/Transforms/WindowTransform.h>

#include <Interpreters/ExpressionActions.h>

#include <Common/Arena.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

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

//    fmt::print(stderr, "end {}, partition_end {}\n", end, partition_end);

    // If we're at the total end of data, we must end the partition. This is one
    // of the few places in calculations where we need special handling for end
    // of data, other places will work as usual based on
    // `partition_ended` = true, because end of data is logically the same as
    // any other end of partition.
    // We must check this first, because other calculations might not be valid
    // when we're at the end of data.
    if (input_is_finished)
    {
        partition_ended = true;
        // We receive empty chunk at the end of data, so the partition_end must
        // be already at the end of data.
        assert(partition_end == end);
        return;
    }

    // If we got to the end of the block already, but we are going to get more
    // input data, wait for it.
    if (partition_end == end)
    {
        return;
    }

    // We process one block at a time, but we can process each block many times,
    // if it contains multiple partitions. The `partition_end` is a
    // past-the-end pointer, so it must be already in the "next" block we haven't
    // processed yet. This is also the last block we have.
    // The exception to this rule is end of data, for which we checked above.
    assert(end.block == partition_end.block + 1);

    // Try to advance the partition end pointer.
    const size_t n = partition_by_indices.size();
    if (n == 0)
    {
        // No PARTITION BY. All input is one partition, which will end when the
        // input ends.
        partition_end = end;
        return;
    }

    // Check for partition end.
    // The partition ends when the PARTITION BY columns change. We need
    // some reference columns for comparison. We might have already
    // dropped the blocks where the partition starts, but any row in the
    // partition will do. We use the current_row for this. It might be the same
    // as the partition_end if we're at the first row of the first partition, so
    // we will compare it to itself, but it still works correctly.
    const auto block_rows = blockRowsNumber(partition_end);
    for (; partition_end.row < block_rows; ++partition_end.row)
    {
        size_t i = 0;
        for (; i < n; i++)
        {
            const auto * ref = inputAt(current_row)[partition_by_indices[i]].get();
            const auto * c = inputAt(partition_end)[partition_by_indices[i]].get();
            if (c->compareAt(partition_end.row,
                    current_row.row, *ref,
                    1 /* nan_direction_hint */) != 0)
            {
                break;
            }
        }

        if (i < n)
        {
            partition_ended = true;
            return;
        }
    }

    // Went until the end of block, go to the next.
    assert(partition_end.row == block_rows);
    ++partition_end.block;
    partition_end.row = 0;

    // Went until the end of data and didn't find the new partition.
    assert(!partition_ended && partition_end == blocksEnd());
}

auto WindowTransform::moveRowNumberNoCheck(const RowNumber & _x, int offset) const
{
    RowNumber x = _x;

    if (offset > 0)
    {
        for (;;)
        {
            assertValid(x);
            assert(offset >= 0);

            const auto block_rows = blockRowsNumber(x);
            x.row += offset;
            if (x.row >= block_rows)
            {
                offset = x.row - block_rows;
                x.row = 0;
                x.block++;

                if (x == blocksEnd())
                {
                    break;
                }
            }
            else
            {
                offset = 0;
                break;
            }
        }
    }
    else if (offset < 0)
    {
        for (;;)
        {
            assertValid(x);
            assert(offset <= 0);

            if (x.row >= static_cast<uint64_t>(-offset))
            {
                x.row -= -offset;
                offset = 0;
                break;
            }

            // Move to the first row in current block. Note that the offset is
            // negative.
            offset += x.row;
            x.row = 0;

            // Move to the last row of the previous block, if we are not at the
            // first one. Offset also is incremented by one, because we pass over
            // the first row of this block.
            if (x.block == first_block_number)
            {
                break;
            }

            --x.block;
            offset += 1;
            x.row = blockRowsNumber(x) - 1;
        }
    }

    return std::tuple{x, offset};
}

auto WindowTransform::moveRowNumber(const RowNumber & _x, int offset) const
{
    auto [x, o] = moveRowNumberNoCheck(_x, offset);

#ifndef NDEBUG
    // Check that it was reversible.
    auto [xx, oo] = moveRowNumberNoCheck(x, -(offset - o));

//    fmt::print(stderr, "{} -> {}, result {}, {}, new offset {}, twice {}, {}\n",
//        _x, offset, x, o, -(offset - o), xx, oo);
    assert(xx == _x);
    assert(oo == 0);
#endif

    return std::tuple{x, o};
}


void WindowTransform::advanceFrameStartRowsOffset()
{
    // Just recalculate it each time by walking blocks.
    const auto [moved_row, offset_left] = moveRowNumber(current_row,
        window_description.frame.begin_offset);

    frame_start = moved_row;

    assertValid(frame_start);

//    fmt::print(stderr, "frame start {} left {} partition start {}\n",
//        frame_start, offset_left, partition_start);

    if (frame_start <= partition_start)
    {
        // Got to the beginning of partition and can't go further back.
        frame_start = partition_start;
        frame_started = true;
        return;
    }

    if (partition_end <= frame_start)
    {
        // A FOLLOWING frame start ran into the end of partition.
        frame_start = partition_end;
        frame_started = partition_ended;
        return;
    }

    // Handled the equality case above. Now the frame start is inside the
    // partition, if we walked all the offset, it's final.
    assert(partition_start < frame_start);
    frame_started = offset_left == 0;

    // If we ran into the start of data (offset left is negative), we won't be
    // able to make progress. Should have handled this case above.
    assert(offset_left >= 0);
}

void WindowTransform::advanceFrameStartChoose()
{
    switch (window_description.frame.begin_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            // UNBOUNDED PRECEDING, just mark it valid. It is initialized when
            // the new partition starts.
            frame_started = true;
            return;
        case WindowFrame::BoundaryType::Current:
            // CURRENT ROW differs between frame types only in how the peer
            // groups are accounted.
            assert(partition_start <= peer_group_start);
            assert(peer_group_start < partition_end);
            assert(peer_group_start <= current_row);
            frame_start = peer_group_start;
            frame_started = true;
            return;
        case WindowFrame::BoundaryType::Offset:
            switch (window_description.frame.type)
            {
                case WindowFrame::FrameType::Rows:
                    advanceFrameStartRowsOffset();
                    return;
                default:
                    // Fallthrough to the "not implemented" error.
                    break;
            }
            break;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Frame start type '{}' for frame '{}' is not implemented",
        WindowFrame::toString(window_description.frame.begin_type),
        WindowFrame::toString(window_description.frame.type));
}

void WindowTransform::advanceFrameStart()
{
    if (frame_started)
    {
        return;
    }

    const auto frame_start_before = frame_start;
    advanceFrameStartChoose();
    assert(frame_start_before <= frame_start);
    if (frame_start == frame_start_before)
    {
        // If the frame start didn't move, this means we validated that the frame
        // starts at the point we reached earlier but were unable to validate.
        // This probably only happens in degenerate cases where the frame start
        // is further than the end of partition, and the partition ends at the
        // last row of the block, but we can only tell for sure after a new
        // block arrives. We still have to update the state of aggregate
        // functions when the frame start becomes valid, so we continue.
        assert(frame_started);
    }

    assert(partition_start <= frame_start);
    assert(frame_start <= partition_end);
    if (partition_ended && frame_start == partition_end)
    {
        // Check that if the start of frame (e.g. FOLLOWING) runs into the end
        // of partition, it is marked as valid -- we can't advance it any
        // further.
        assert(frame_started);
    }
}

bool WindowTransform::arePeers(const RowNumber & x, const RowNumber & y) const
{
    if (x == y)
    {
        // For convenience, a row is always its own peer.
        return true;
    }

    if (window_description.frame.type == WindowFrame::FrameType::Rows)
    {
        // For ROWS frame, row is only peers with itself (checked above);
        return false;
    }

    // For RANGE and GROUPS frames, rows that compare equal w/ORDER BY are peers.
    assert(window_description.frame.type == WindowFrame::FrameType::Range);
    const size_t n = order_by_indices.size();
    if (n == 0)
    {
        // No ORDER BY, so all rows are peers.
        return true;
    }

    size_t i = 0;
    for (; i < n; i++)
    {
        const auto * column_x = inputAt(x)[order_by_indices[i]].get();
        const auto * column_y = inputAt(y)[order_by_indices[i]].get();
        if (column_x->compareAt(x.row, y.row, *column_y,
                1 /* nan_direction_hint */) != 0)
        {
            return false;
        }
    }

    return true;
}

void WindowTransform::advanceFrameEndCurrentRow()
{
//    fmt::print(stderr, "starting from frame_end {}\n", frame_end);

    // We only process one block here, and frame_end must be already in it: if
    // we didn't find the end in the previous block, frame_end is now the first
    // row of the current block. We need this knowledge to write a simpler loop
    // (only loop over rows and not over blocks), that should hopefully be more
    // efficient.
    // partition_end is either in this new block or past-the-end.
    assert(frame_end.block  == partition_end.block
        || frame_end.block + 1 == partition_end.block);

    if (frame_end == partition_end)
    {
        // The case when we get a new block and find out that the partition has
        // ended.
        assert(partition_ended);
        frame_ended = partition_ended;
        return;
    }

    // We advance until the partition end. It's either in the current block or
    // in the next one, which is also the past-the-end block. Figure out how
    // many rows we have to process.
    uint64_t rows_end;
    if (partition_end.row == 0)
    {
        assert(partition_end == blocksEnd());
        rows_end = blockRowsNumber(frame_end);
    }
    else
    {
        assert(frame_end.block == partition_end.block);
        rows_end = partition_end.row;
    }
    // Equality would mean "no data to process", for which we checked above.
    assert(frame_end.row < rows_end);

//    fmt::print(stderr, "first row {} last {}\n", frame_end.row, rows_end);

    // Advance frame_end while it is still peers with the current row.
    for (; frame_end.row < rows_end; ++frame_end.row)
    {
        if (!arePeers(current_row, frame_end))
        {
//            fmt::print(stderr, "{} and {} don't match\n", reference, frame_end);
            frame_ended = true;
            return;
        }
    }

    // Might have gotten to the end of the current block, have to properly
    // update the row number.
    if (frame_end.row == blockRowsNumber(frame_end))
    {
        ++frame_end.block;
        frame_end.row = 0;
    }

    // Got to the end of partition (frame ended as well then) or end of data.
    assert(frame_end == partition_end);
    frame_ended = partition_ended;
}

void WindowTransform::advanceFrameEndUnbounded()
{
    // The UNBOUNDED FOLLOWING frame ends when the partition ends.
    frame_end = partition_end;
    frame_ended = partition_ended;
}

void WindowTransform::advanceFrameEndRowsOffset()
{
    // Walk the specified offset from the current row. The "+1" is needed
    // because the frame_end is a past-the-end pointer.
    const auto [moved_row, offset_left] = moveRowNumber(current_row,
        window_description.frame.end_offset + 1);

    if (partition_end <= moved_row)
    {
        // Clamp to the end of partition. It might not have ended yet, in which
        // case wait for more data.
        frame_end = partition_end;
        frame_ended = partition_ended;
        return;
    }

    if (moved_row <= partition_start)
    {
        // Clamp to the start of partition.
        frame_end = partition_start;
        frame_ended = true;
        return;
    }

    // Frame end inside partition, if we walked all the offset, it's final.
    frame_end = moved_row;
    frame_ended = offset_left == 0;

    // If we ran into the start of data (offset left is negative), we won't be
    // able to make progress. Should have handled this case above.
    assert(offset_left >= 0);
}

void WindowTransform::advanceFrameEnd()
{
    // No reason for this function to be called again after it succeeded.
    assert(!frame_ended);

    const auto frame_end_before = frame_end;

    switch (window_description.frame.end_type)
    {
        case WindowFrame::BoundaryType::Current:
            advanceFrameEndCurrentRow();
            break;
        case WindowFrame::BoundaryType::Unbounded:
            advanceFrameEndUnbounded();
            break;
        case WindowFrame::BoundaryType::Offset:
            switch (window_description.frame.type)
            {
                case WindowFrame::FrameType::Rows:
                    advanceFrameEndRowsOffset();
                    break;
                default:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "The frame end type '{}' is not implemented",
                        WindowFrame::toString(window_description.frame.end_type));
            }
            break;
    }

//    fmt::print(stderr, "frame_end {} -> {}\n", frame_end_before, frame_end);

    // We might not have advanced the frame end if we found out we reached the
    // end of input or the partition, or if we still don't know the frame start.
    if (frame_end_before == frame_end)
    {
        return;
    }
}

// Update the aggregation states after the frame has changed.
void WindowTransform::updateAggregationState()
{
//    fmt::print(stderr, "update agg states [{}, {}) -> [{}, {})\n",
//        prev_frame_start, prev_frame_end, frame_start, frame_end);

    // Assert that the frame boundaries are known, have proper order wrt each
    // other, and have not gone back wrt the previous frame.
    assert(frame_started);
    assert(frame_ended);
    assert(frame_start <= frame_end);
    assert(prev_frame_start <= prev_frame_end);
    assert(prev_frame_start <= frame_start);
    assert(prev_frame_end <= frame_end);

    // We might have to reset aggregation state and/or add some rows to it.
    // Figure out what to do.
    bool reset_aggregation = false;
    RowNumber rows_to_add_start;
    RowNumber rows_to_add_end;
    if (frame_start == prev_frame_start)
    {
        // The frame start didn't change, add the tail rows.
        reset_aggregation = false;
        rows_to_add_start = prev_frame_end;
        rows_to_add_end = frame_end;
    }
    else
    {
        // The frame start changed, reset the state and aggregate over the
        // entire frame. This can be made per-function after we learn to
        // subtract rows from some types of aggregation states, but for now we
        // always have to reset when the frame start changes.
        reset_aggregation = true;
        rows_to_add_start = frame_start;
        rows_to_add_end = frame_end;
    }

    for (auto & ws : workspaces)
    {
        const auto * a = ws.window_function.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        if (reset_aggregation)
        {
//            fmt::print(stderr, "(2) reset aggregation\n");
            a->destroy(buf);
            a->create(buf);
        }

        for (auto row = rows_to_add_start; row < rows_to_add_end;
            advanceRowNumber(row))
        {
            if (row.block != ws.cached_block_number)
            {
                const auto & block
                    = blocks[row.block - first_block_number];
                ws.argument_columns.clear();
                for (const auto i : ws.argument_column_indices)
                {
                    ws.argument_columns.push_back(block.input_columns[i].get());
                }
                ws.cached_block_number = row.block;
            }

//            fmt::print(stderr, "(2) add row {}\n", row);
            auto * columns = ws.argument_columns.data();
            a->add(buf, columns, row.row, arena.get());
        }
    }

    prev_frame_start = frame_start;
    prev_frame_end = frame_end;
}

void WindowTransform::writeOutCurrentRow()
{
    assert(current_row < partition_end);
    assert(current_row.block >= first_block_number);

    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];
        const auto & f = ws.window_function;
        const auto * a = f.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        IColumn * result_column = outputAt(current_row)[wi].get();
        // FIXME does it also allocate the result on the arena?
        // We'll have to pass it out with blocks then...
        a->insertResultInto(buf, *result_column, arena.get());
    }
}

void WindowTransform::appendChunk(Chunk & chunk)
{
//    fmt::print(stderr, "new chunk, {} rows, finished={}\n", chunk.getNumRows(),
//        input_is_finished);

    // First, prepare the new input block and add it to the queue. We might not
    // have it if it's end of data, though.
    if (!input_is_finished)
    {
        assert(chunk.hasRows());
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
//        fmt::print(stderr, "partition [{}, {}), {}\n",
//            partition_start, partition_end, partition_ended);

        // Either we ran out of data or we found the end of partition (maybe
        // both, but this only happens at the total end of data).
        assert(partition_ended || partition_end == blocksEnd());
        if (partition_ended && partition_end == blocksEnd())
        {
            assert(input_is_finished);
        }

        // After that, try to calculate window functions for each next row.
        // We can continue until the end of partition or current end of data,
        // which is precisely the definition of `partition_end`.
        while (current_row < partition_end)
        {
//            fmt::print(stderr, "(1) row {} frame [{}, {}) {}, {}\n",
//                current_row, frame_start, frame_end,
//                frame_started, frame_ended);

            // We now know that the current row is valid, so we can update the
            // peer group start.
            if (!arePeers(peer_group_start, current_row))
            {
                peer_group_start = current_row;
            }

            // Advance the frame start.
            advanceFrameStart();

            if (!frame_started)
            {
                // Wait for more input data to find the start of frame.
                assert(!input_is_finished);
                assert(!partition_ended);
                return;
            }

            // frame_end must be greater or equal than frame_start, so if the
            // frame_start is already past the current frame_end, we can start
            // from it to save us some work.
            if (frame_end < frame_start)
            {
                frame_end = frame_start;
            }

            // Advance the frame end.
            advanceFrameEnd();

            if (!frame_ended)
            {
                // Wait for more input data to find the end of frame.
                assert(!input_is_finished);
                assert(!partition_ended);
                return;
            }

//            fmt::print(stderr, "(2) row {} frame [{}, {}) {}, {}\n",
//                current_row, frame_start, frame_end,
//                frame_started, frame_ended);

            // The frame can be empty sometimes, e.g. the boundaries coincide
            // or the start is after the partition end. But hopefully start is
            // not after end.
            assert(frame_started);
            assert(frame_ended);
            assert(frame_start <= frame_end);

            // Now that we know the new frame boundaries, update the aggregation
            // states. Theoretically we could do this simultaneously with moving
            // the frame boundaries, but it would require some care not to
            // perform unnecessary work while we are still looking for the frame
            // start, so do it the simple way for now.
            updateAggregationState();

            // Write out the aggregation results.
            writeOutCurrentRow();

            // Move to the next row. The frame will have to be recalculated.
            // The peer group start is updated at the beginning of the loop,
            // because current_row might now be past-the-end.
            advanceRowNumber(current_row);
            first_not_ready_row = current_row;
            frame_ended = false;
            frame_started = false;
        }

        if (input_is_finished)
        {
            // We finalized the last partition in the above loop, and don't have
            // to do anything else.
            return;
        }

        if (!partition_ended)
        {
            // Wait for more input data to find the end of partition.
            // Assert that we processed all the data we currently have, and that
            // we are going to receive more data.
            assert(partition_end == blocksEnd());
            assert(!input_is_finished);
            break;
        }

        // Start the next partition.
        partition_start = partition_end;
        advanceRowNumber(partition_end);
        partition_ended = false;
        // We have to reset the frame when the new partition starts. This is not a
        // generally correct way to do so, but we don't really support moving frame
        // for now.
        frame_start = partition_start;
        frame_end = partition_start;
        prev_frame_start = partition_start;
        prev_frame_end = partition_start;
        assert(current_row == partition_start);
        peer_group_start = partition_start;

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

    if (output_data.exception)
    {
        // An exception occurred during processing.
        output.pushData(std::move(output_data));
        output.finish();
        input.close();
        return Status::Finished;
    }

    assert(first_not_ready_row.block >= first_block_number);
    // The first_not_ready_row might be past-the-end if we have already
    // calculated the window functions for all input rows. That's why the
    // equality is also valid here.
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

        // We don't need input.setNotNeeded() here, because we already pull with
        // the set_not_needed flag.
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
        // Pulling with set_not_needed = true and using an explicit setNeeded()
        // later is somewhat more efficient, because after the setNeeded(), the
        // required input block will be generated in the same thread and passed
        // to our prepare() + work() methods in the same thread right away, so
        // hopefully we will work on hot (cached) data.
        input_data = input.pullData(true /* set_not_needed */);

        // If we got an exception from input, just return it and mark that we're
        // finished.
        if (input_data.exception)
        {
            output.pushData(std::move(input_data));
            output.finish();

            return Status::PortFull;
        }

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
    // Exceptions should be skipped in prepare().
    assert(!input_data.exception);

    assert(has_input || input_is_finished);

    try
    {
        has_input = false;
        appendChunk(input_data.chunk);
    }
    catch (DB::Exception &)
    {
        output_data.exception = std::current_exception();
        has_input = false;
        return;
    }

    // We don't really have to keep the entire partition, and it can be big, so
    // we want to drop the starting blocks to save memory.
    // We can drop the old blocks if we already returned them as output, and the
    // frame and the current row are already past them. Note that the frame
    // start can be further than current row for some frame specs (e.g. EXCLUDE
    // CURRENT ROW), so we have to check both.
    const auto first_used_block = std::min(next_output_block_number,
        std::min(frame_start.block, current_row.block));

    if (first_block_number < first_used_block)
    {
//        fmt::print(stderr, "will drop blocks from {} to {}\n", first_block_number,
//            first_used_block);

        blocks.erase(blocks.begin(),
            blocks.begin() + (first_used_block - first_block_number));
        first_block_number = first_used_block;

        assert(next_output_block_number >= first_block_number);
        assert(frame_start.block >= first_block_number);
        assert(current_row.block >= first_block_number);
        assert(peer_group_start.block >= first_block_number);
    }
}


}
