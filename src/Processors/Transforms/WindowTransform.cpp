#include <Processors/Transforms/WindowTransform.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/Arena.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

// Interface for true window functions. It's not much of an interface, they just
// accept the guts of WindowTransform and do 'something'. Given a small number of
// true window functions, and the fact that the WindowTransform internals are
// pretty much well defined in domain terms (e.g. frame boundaries), this is
// somewhat acceptable.
class IWindowFunction
{
public:
    virtual ~IWindowFunction() = default;

    // Must insert the result for current_row.
    virtual void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) = 0;
};

// Compares ORDER BY column values at given rows to find the boundaries of frame:
// [compared] with [reference] +/- offset. Return value is -1/0/+1, like in
// sorting predicates -- -1 means [compared] is less than [reference] +/- offset.
template <typename ColumnType>
static int compareValuesWithOffset(const IColumn * _compared_column,
    size_t compared_row, const IColumn * _reference_column,
    size_t reference_row,
    const Field & _offset,
    bool offset_is_preceding)
{
    // Casting the columns to the known type here makes it faster, probably
    // because the getData call can be devirtualized.
    const auto * compared_column = assert_cast<const ColumnType *>(
        _compared_column);
    const auto * reference_column = assert_cast<const ColumnType *>(
        _reference_column);
    // Note that the storage type of offset returned by get<> is different, so
    // we need to specify the type explicitly.
    const typename ColumnType::ValueType offset
            = _offset.get<typename ColumnType::ValueType>();
    assert(offset >= 0);

    const auto compared_value_data = compared_column->getDataAt(compared_row);
    assert(compared_value_data.size == sizeof(typename ColumnType::ValueType));
    auto compared_value = unalignedLoad<typename ColumnType::ValueType>(
        compared_value_data.data);

    const auto reference_value_data = reference_column->getDataAt(reference_row);
    assert(reference_value_data.size == sizeof(typename ColumnType::ValueType));
    auto reference_value = unalignedLoad<typename ColumnType::ValueType>(
        reference_value_data.data);

    bool is_overflow;
    if (offset_is_preceding)
    {
        is_overflow = __builtin_sub_overflow(reference_value, offset,
            &reference_value);
    }
    else
    {
        is_overflow = __builtin_add_overflow(reference_value, offset,
            &reference_value);
    }

//    fmt::print(stderr,
//        "compared [{}] = {}, old ref {}, shifted ref [{}] = {}, offset {} preceding {} overflow {} to negative {}\n",
//        compared_row, toString(compared_value),
//        // fmt doesn't like char8_t.
//        static_cast<Int64>(unalignedLoad<typename ColumnType::ValueType>(reference_value_data.data)),
//        reference_row, toString(reference_value),
//        toString(offset), offset_is_preceding,
//        is_overflow, offset_is_preceding);

    if (is_overflow)
    {
        if (offset_is_preceding)
        {
            // Overflow to the negative, [compared] must be greater.
            // We know that because offset is >= 0.
            return 1;
        }
        else
        {
            // Overflow to the positive, [compared] must be less.
            return -1;
        }
    }
    else
    {
        // No overflow, compare normally.
        return compared_value < reference_value ? -1
            : compared_value == reference_value ? 0 : 1;
    }
}

// A specialization of compareValuesWithOffset for floats.
template <typename ColumnType>
static int compareValuesWithOffsetFloat(const IColumn * _compared_column,
    size_t compared_row, const IColumn * _reference_column,
    size_t reference_row,
    const Field & _offset,
    bool offset_is_preceding)
{
    // Casting the columns to the known type here makes it faster, probably
    // because the getData call can be devirtualized.
    const auto * compared_column = assert_cast<const ColumnType *>(
        _compared_column);
    const auto * reference_column = assert_cast<const ColumnType *>(
        _reference_column);
    const auto offset = _offset.get<typename ColumnType::ValueType>();
    assert(offset >= 0);

    const auto compared_value_data = compared_column->getDataAt(compared_row);
    assert(compared_value_data.size == sizeof(typename ColumnType::ValueType));
    auto compared_value = unalignedLoad<typename ColumnType::ValueType>(
        compared_value_data.data);

    const auto reference_value_data = reference_column->getDataAt(reference_row);
    assert(reference_value_data.size == sizeof(typename ColumnType::ValueType));
    auto reference_value = unalignedLoad<typename ColumnType::ValueType>(
        reference_value_data.data);

    // Floats overflow to Inf and the comparison will work normally, so we don't
    // have to do anything.
    if (offset_is_preceding)
    {
        reference_value -= offset;
    }
    else
    {
        reference_value += offset;
    }

    const auto result =  compared_value < reference_value ? -1
        : compared_value == reference_value ? 0 : 1;

//    fmt::print(stderr, "compared {}, offset {}, reference {}, result {}\n",
//        compared_value, offset, reference_value, result);

    return result;
}

// Helper macros to dispatch on type of the ORDER BY column
#define APPLY_FOR_ONE_TYPE(FUNCTION, TYPE) \
else if (typeid_cast<const TYPE *>(column)) \
{ \
    /* clang-tidy you're dumb, I can't put FUNCTION in braces here. */ \
    compare_values_with_offset = FUNCTION<TYPE>; /* NOLINT */ \
}

#define APPLY_FOR_TYPES(FUNCTION) \
if (false) /* NOLINT */ \
{ \
    /* Do nothing, a starter condition. */ \
} \
APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<UInt8>) \
APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<UInt16>) \
APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<UInt32>) \
APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<UInt64>) \
\
APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int8>) \
APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int16>) \
APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int32>) \
APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int64>) \
APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int128>) \
\
APPLY_FOR_ONE_TYPE(FUNCTION##Float, ColumnVector<Float32>) \
APPLY_FOR_ONE_TYPE(FUNCTION##Float, ColumnVector<Float64>) \
\
else \
{ \
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, \
        "The RANGE OFFSET frame for '{}' ORDER BY column is not implemented", \
        demangle(typeid(*column).name())); \
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
        workspace.aggregate_function = f.aggregate_function;
        const auto & aggregate_function = workspace.aggregate_function;
        if (!arena && aggregate_function->allocatesMemoryInArena())
        {
            arena = std::make_unique<Arena>();
        }

        workspace.argument_column_indices.reserve(f.argument_names.size());
        for (const auto & argument_name : f.argument_names)
        {
            workspace.argument_column_indices.push_back(
                input_header.getPositionByName(argument_name));
        }
        workspace.argument_columns.assign(f.argument_names.size(), nullptr);

        workspace.window_function_impl = aggregate_function->asWindowFunction();
        if (!workspace.window_function_impl)
        {
            workspace.aggregate_function_state.reset(
                aggregate_function->sizeOfData(),
                aggregate_function->alignOfData());
            aggregate_function->create(workspace.aggregate_function_state.data());
        }

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

    // Choose a row comparison function for RANGE OFFSET frame based on the
    // type of the ORDER BY column.
    if (window_description.frame.type == WindowFrame::FrameType::Range
        && (window_description.frame.begin_type
                == WindowFrame::BoundaryType::Offset
            || window_description.frame.end_type
                == WindowFrame::BoundaryType::Offset))
    {
        assert(order_by_indices.size() == 1);
        const auto & entry = input_header.getByPosition(order_by_indices[0]);
        const IColumn * column = entry.column.get();
        APPLY_FOR_TYPES(compareValuesWithOffset)

        // Convert the offsets to the ORDER BY column type. We can't just check
        // that the type matches, because e.g. the int literals are always
        // (U)Int64, but the column might be Int8 and so on.
        if (window_description.frame.begin_type
            == WindowFrame::BoundaryType::Offset)
        {
            window_description.frame.begin_offset = convertFieldToTypeOrThrow(
                window_description.frame.begin_offset,
                *entry.type);

            if (applyVisitor(FieldVisitorAccurateLess{},
                window_description.frame.begin_offset, Field(0)))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Window frame start offset must be nonnegative, {} given",
                    window_description.frame.begin_offset);
            }
        }
        if (window_description.frame.end_type
            == WindowFrame::BoundaryType::Offset)
        {
            window_description.frame.end_offset = convertFieldToTypeOrThrow(
                window_description.frame.end_offset,
                *entry.type);

            if (applyVisitor(FieldVisitorAccurateLess{},
                window_description.frame.end_offset, Field(0)))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Window frame start offset must be nonnegative, {} given",
                    window_description.frame.end_offset);
            }
        }
    }
}

WindowTransform::~WindowTransform()
{
    // Some states may be not created yet if the creation failed.
    for (auto & ws : workspaces)
    {
        if (!ws.window_function_impl)
        {
            ws.aggregate_function->destroy(
                ws.aggregate_function_state.data());
        }
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

            // abs(offset) is less than INT_MAX, as checked in the parser, so
            // this negation should always work.
            assert(offset >= -INT_MAX);
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
        window_description.frame.begin_offset.get<UInt64>()
            * (window_description.frame.begin_preceding ? -1 : 1));

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


void WindowTransform::advanceFrameStartRangeOffset()
{
    // See the comment for advanceFrameEndRangeOffset().
    const int direction = window_description.order_by[0].direction;
    const bool preceding = window_description.frame.begin_preceding
        == (direction > 0);
    const auto * reference_column
        = inputAt(current_row)[order_by_indices[0]].get();
    for (; frame_start < partition_end; advanceRowNumber(frame_start))
    {
        // The first frame value is [current_row] with offset, so we advance
        // while [frames_start] < [current_row] with offset.
        const auto * compared_column
            = inputAt(frame_start)[order_by_indices[0]].get();
        if (compare_values_with_offset(compared_column, frame_start.row,
            reference_column, current_row.row,
            window_description.frame.begin_offset,
            preceding)
                * direction >= 0)
        {
            frame_started = true;
            return;
        }
    }

    frame_started = partition_ended;
}

void WindowTransform::advanceFrameStart()
{
    if (frame_started)
    {
        return;
    }

    const auto frame_start_before = frame_start;

    switch (window_description.frame.begin_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            // UNBOUNDED PRECEDING, just mark it valid. It is initialized when
            // the new partition starts.
            frame_started = true;
            break;
        case WindowFrame::BoundaryType::Current:
            // CURRENT ROW differs between frame types only in how the peer
            // groups are accounted.
            assert(partition_start <= peer_group_start);
            assert(peer_group_start < partition_end);
            assert(peer_group_start <= current_row);
            frame_start = peer_group_start;
            frame_started = true;
            break;
        case WindowFrame::BoundaryType::Offset:
            switch (window_description.frame.type)
            {
                case WindowFrame::FrameType::Rows:
                    advanceFrameStartRowsOffset();
                    break;
                case WindowFrame::FrameType::Range:
                    advanceFrameStartRangeOffset();
                    break;
                default:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Frame start type '{}' for frame '{}' is not implemented",
                        WindowFrame::toString(window_description.frame.begin_type),
                        WindowFrame::toString(window_description.frame.type));
            }
            break;
    }

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
        window_description.frame.end_offset.get<UInt64>()
            * (window_description.frame.end_preceding ? -1 : 1)
            + 1);

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

void WindowTransform::advanceFrameEndRangeOffset()
{
    // PRECEDING/FOLLOWING change direction for DESC order.
    // See CD 9075-2:201?(E) 7.14 <window clause> p. 429.
    const int direction = window_description.order_by[0].direction;
    const bool preceding = window_description.frame.end_preceding
        == (direction > 0);
    const auto * reference_column
        = inputAt(current_row)[order_by_indices[0]].get();
    for (; frame_end < partition_end; advanceRowNumber(frame_end))
    {
        // The last frame value is current_row with offset, and we need a
        // past-the-end pointer, so we advance while
        // [frame_end] <= [current_row] with offset.
        const auto * compared_column
            = inputAt(frame_end)[order_by_indices[0]].get();
        if (compare_values_with_offset(compared_column, frame_end.row,
            reference_column, current_row.row,
            window_description.frame.end_offset,
            preceding)
                * direction > 0)
        {
            frame_ended = true;
            return;
        }
    }

    frame_ended = partition_ended;
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
                case WindowFrame::FrameType::Range:
                    advanceFrameEndRangeOffset();
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
        if (ws.window_function_impl)
        {
            // No need to do anything for true window functions.
            continue;
        }

        const auto * a = ws.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        if (reset_aggregation)
        {
//            fmt::print(stderr, "(2) reset aggregation\n");
            a->destroy(buf);
            a->create(buf);
        }

        // To achieve better performance, we will have to loop over blocks and
        // rows manually, instead of using advanceRowNumber().
        // For this purpose, the past-the-end block can be different than the
        // block of the past-the-end row (it's usually the next block).
        const auto past_the_end_block = rows_to_add_end.row == 0
            ? rows_to_add_end.block
            : rows_to_add_end.block + 1;

        for (auto block_number = rows_to_add_start.block;
             block_number < past_the_end_block;
             ++block_number)
        {
            auto & block = blockAt(block_number);

            if (ws.cached_block_number != block_number)
            {
                for (size_t i = 0; i < ws.argument_column_indices.size(); ++i)
                {
                    ws.argument_columns[i] = block.input_columns[
                        ws.argument_column_indices[i]].get();
                }
                ws.cached_block_number = block_number;
            }

            // First and last blocks may be processed partially, and other blocks
            // are processed in full.
            const auto first_row = block_number == rows_to_add_start.block
                ? rows_to_add_start.row : 0;
            const auto past_the_end_row = block_number == rows_to_add_end.block
                ? rows_to_add_end.row : block.rows;

            // We should add an addBatch analog that can accept a starting offset.
            // For now, add the values one by one.
            auto * columns = ws.argument_columns.data();
            // Removing arena.get() from the loop makes it faster somehow...
            auto * arena_ptr = arena.get();
            for (auto row = first_row; row < past_the_end_row; ++row)
            {
                a->add(buf, columns, row, arena_ptr);
            }
        }
    }

    prev_frame_start = frame_start;
    prev_frame_end = frame_end;
}

void WindowTransform::writeOutCurrentRow()
{
    assert(current_row < partition_end);
    assert(current_row.block >= first_block_number);

    const auto & block = blockAt(current_row);
    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];

        if (ws.window_function_impl)
        {
            ws.window_function_impl->windowInsertResultInto(this, wi);
        }
        else
        {
            IColumn * result_column = block.output_columns[wi].get();
            const auto * a = ws.aggregate_function.get();
            auto * buf = ws.aggregate_function_state.data();
            // FIXME does it also allocate the result on the arena?
            // We'll have to pass it out with blocks then...
            a->insertResultInto(buf, *result_column, arena.get());
        }
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
        // Use the number of rows from the Chunk, because it is correct even in
        // the case where the Chunk has no columns. Not sure if this actually
        // happens, because even in the case of `count() over ()` we have a dummy
        // input column.
        block.rows = chunk.getNumRows();
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

            block.output_columns.push_back(ws.aggregate_function->getReturnType()
                ->createColumn());
            block.output_columns.back()->reserve(block.rows);
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
                peer_group_start_row_number = current_row_number;
                ++peer_group_number;
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
            ++current_row_number;
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
        // We have to reset the frame and other pointers when the new partition
        // starts.
        frame_start = partition_start;
        frame_end = partition_start;
        prev_frame_start = partition_start;
        prev_frame_end = partition_start;
        assert(current_row == partition_start);
        current_row_number = 1;
        peer_group_start = partition_start;
        peer_group_start_row_number = 1;
        peer_group_number = 1;

//        fmt::print(stderr, "reinitialize agg data at start of {}\n",
//            new_partition_start);
        // Reinitialize the aggregate function states because the new partition
        // has started.
        for (auto & ws : workspaces)
        {
            if (ws.window_function_impl)
            {
                continue;
            }

            const auto * a = ws.aggregate_function.get();
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
            if (ws.window_function_impl)
            {
                continue;
            }

            const auto * a = ws.aggregate_function.get();
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
            const auto i = next_output_block_number - first_block_number;
            auto & block = blocks[i];
            auto columns = block.input_columns;
            for (auto & res : block.output_columns)
            {
                columns.push_back(ColumnPtr(std::move(res)));
            }
            output_data.chunk.setColumns(columns, block.rows);

//            fmt::print(stderr, "output block {} as chunk '{}'\n",
//                next_output_block_number,
//                output_data.chunk.dumpStructure());

            ++next_output_block_number;

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

// A basic implementation for a true window function. It pretends to be an
// aggregate function, but refuses to work as such.
struct WindowFunction
    : public IAggregateFunctionHelper<WindowFunction>
    , public IWindowFunction
{
    std::string name;

    WindowFunction(const std::string & name_, const DataTypes & argument_types_,
               const Array & parameters_)
        : IAggregateFunctionHelper<WindowFunction>(argument_types_, parameters_)
        , name(name_)
    {}

    IWindowFunction * asWindowFunction() override { return this; }

    [[noreturn]] void fail() const
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The function '{}' can only be used as a window function, not as an aggregate function",
            getName());
    }

    String getName() const override { return name; }
    void create(AggregateDataPtr __restrict) const override { fail(); }
    void destroy(AggregateDataPtr __restrict) const noexcept override {}
    bool hasTrivialDestructor() const override { return true; }
    size_t sizeOfData() const override { return 0; }
    size_t alignOfData() const override { return 1; }
    void add(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override { fail(); }
    void merge(AggregateDataPtr __restrict, ConstAggregateDataPtr, Arena *) const override { fail(); }
    void serialize(ConstAggregateDataPtr __restrict, WriteBuffer &) const override { fail(); }
    void deserialize(AggregateDataPtr __restrict, ReadBuffer &, Arena *) const override { fail(); }
    void insertResultInto(AggregateDataPtr __restrict, IColumn &, Arena *) const override { fail(); }
};

struct WindowFunctionRank final : public WindowFunction
{
    WindowFunctionRank(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : WindowFunction(name_, argument_types_, parameters_)
    {}

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) override
    {
        IColumn & to = *transform->blockAt(transform->current_row)
            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
            transform->peer_group_start_row_number);
    }
};

struct WindowFunctionDenseRank final : public WindowFunction
{
    WindowFunctionDenseRank(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : WindowFunction(name_, argument_types_, parameters_)
    {}

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) override
    {
        IColumn & to = *transform->blockAt(transform->current_row)
            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
            transform->peer_group_number);
    }
};

struct WindowFunctionRowNumber final : public WindowFunction
{
    WindowFunctionRowNumber(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : WindowFunction(name_, argument_types_, parameters_)
    {}

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) override
    {
        IColumn & to = *transform->blockAt(transform->current_row)
            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
            transform->current_row_number);
    }
};

// ClickHouse-specific variant of lag/lead that respects the window frame.
template <bool is_lead>
struct WindowFunctionLagLeadInFrame final : public WindowFunction
{
    WindowFunctionLagLeadInFrame(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : WindowFunction(name_, argument_types_, parameters_)
    {
        if (!parameters.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} cannot be parameterized", name_);
        }

        if (argument_types.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} takes at least one argument", name_);
        }

        if (argument_types.size() == 1)
        {
            return;
        }

        if (!isInt64FieldType(argument_types[1]->getDefault().getType()))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Offset must be an integer, '{}' given",
                argument_types[1]->getName());
        }

        if (argument_types.size() == 2)
        {
            return;
        }

        if (!getLeastSupertype({argument_types[0], argument_types[2]}))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The default value type '{}' is not convertible to the argument type '{}'",
                argument_types[2]->getName(),
                argument_types[0]->getName());
        }

        if (argument_types.size() > 3)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function '{}' accepts at most 3 arguments, {} given",
                name, argument_types.size());
        }
    }

    DataTypePtr getReturnType() const override
    { return argument_types[0]; }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) override
    {
        const auto & current_block = transform->blockAt(transform->current_row);
        IColumn & to = *current_block.output_columns[function_index];
        const auto & workspace = transform->workspaces[function_index];

        int offset = 1;
        if (argument_types.size() > 1)
        {
            offset = (*current_block.input_columns[
                    workspace.argument_column_indices[1]])[
                        transform->current_row.row].get<Int64>();
            if (offset < 0)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "The offset for function {} must be nonnegative, {} given",
                    getName(), offset);
            }
            if (offset > INT_MAX)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "The offset for function {} must be less than {}, {} given",
                    getName(), INT_MAX, offset);
            }
        }

        const auto [target_row, offset_left] = transform->moveRowNumber(
            transform->current_row, offset * (is_lead ? 1 : -1));

        if (offset_left != 0
            || target_row < transform->frame_start
            || transform->frame_end <= target_row)
        {
            // Offset is outside the frame.
            if (argument_types.size() > 2)
            {
                // Column with default values is specified.
                to.insertFrom(*current_block.input_columns[
                            workspace.argument_column_indices[2]],
                    transform->current_row.row);
            }
            else
            {
                to.insertDefault();
            }
        }
        else
        {
            // Offset is inside the frame.
            to.insertFrom(*transform->blockAt(target_row).input_columns[
                    workspace.argument_column_indices[0]],
                target_row.row);
        }
    }
};

void registerWindowFunctions(AggregateFunctionFactory & factory)
{
    // Why didn't I implement lag/lead yet? Because they are a mess. I imagine
    // they are from the older generation of window functions, when the concept
    // of frame was not yet invented, so they ignore the frame and use the
    // partition instead. This means we have to track a separate frame for
    // these functions, which would  make the window transform completely
    // impenetrable to human mind. We can't just get away with materializing
    // the whole partition like Postgres does, because using a linear amount
    // of additional memory is not an option when we have a lot of data. We must
    // be able to process at least the lag/lead in streaming fashion.
    // A partial solution for constant offsets is rewriting, say `lag(value, offset)
    // to `any(value) over (rows between offset preceding and offset preceding)`.
    // We also implement non-standard functions `lag/leadInFrame`, that are
    // analogous to `lag/lead`, but respect the frame.
    // Functions like cume_dist() do require materializing the entire
    // partition, but it's probably also simpler to implement them by rewriting
    // to a (rows between unbounded preceding and unbounded following) frame,
    // instead of adding separate logic for them.

    factory.registerFunction("rank", [](const std::string & name,
            const DataTypes & argument_types, const Array & parameters)
        {
            return std::make_shared<WindowFunctionRank>(name, argument_types,
                parameters);
        });

    factory.registerFunction("dense_rank", [](const std::string & name,
            const DataTypes & argument_types, const Array & parameters)
        {
            return std::make_shared<WindowFunctionDenseRank>(name, argument_types,
                parameters);
        });

    factory.registerFunction("row_number", [](const std::string & name,
            const DataTypes & argument_types, const Array & parameters)
        {
            return std::make_shared<WindowFunctionRowNumber>(name, argument_types,
                parameters);
        });

    factory.registerFunction("lagInFrame", [](const std::string & name,
            const DataTypes & argument_types, const Array & parameters)
        {
            return std::make_shared<WindowFunctionLagLeadInFrame<false>>(
                name, argument_types, parameters);
        });

    factory.registerFunction("leadInFrame", [](const std::string & name,
            const DataTypes & argument_types, const Array & parameters)
        {
            return std::make_shared<WindowFunctionLagLeadInFrame<true>>(
                name, argument_types, parameters);
        });
}

}
