#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Core/SortCursor.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/convertFieldToType.h>
#include <Processors/Transforms/WindowTransform.h>
#include <base/arithmeticOverflow.h>
#include <Common/Arena.h>
#include <Common/FieldAccurateComparison.h>

#include <algorithm>
#include <limits>


/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <>
struct fmt::formatter<DB::RowNumber>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw fmt::format_error("Invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::RowNumber & x, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}:{}", x.block, x.row);
    }
};


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

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

    using ValueType = typename ColumnType::ValueType;
    // Note that the storage type of offset returned by get<> is different, so
    // we need to specify the type explicitly.
    const ValueType offset = static_cast<ValueType>(_offset.safeGet<ValueType>());
    chassert(offset >= 0);

    const auto compared_value_data = compared_column->getDataAt(compared_row);
    chassert(compared_value_data.size() == sizeof(ValueType));
    auto compared_value = unalignedLoad<ValueType>(
        compared_value_data.data());

    const auto reference_value_data = reference_column->getDataAt(reference_row);
    chassert(reference_value_data.size() == sizeof(ValueType));
    auto reference_value = unalignedLoad<ValueType>(
        reference_value_data.data());

    bool is_overflow = false;
    if (offset_is_preceding)
        is_overflow = common::subOverflow(reference_value, offset, reference_value);
    else
        is_overflow = common::addOverflow(reference_value, offset, reference_value);

    if (is_overflow)
    {
        if (offset_is_preceding)
        {
            // Overflow to the negative, [compared] must be greater.
            // We know that because offset is >= 0.
            return 1;
        }

        // Overflow to the positive, [compared] must be less.
        return -1;
    }

    // No overflow, compare normally.
    return compared_value < reference_value ? -1 : compared_value == reference_value ? 0 : 1;
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
    const auto offset = _offset.safeGet<typename ColumnType::ValueType>();
    chassert(offset >= 0);

    const auto compared_value_data = compared_column->getDataAt(compared_row);
    chassert(compared_value_data.size() == sizeof(typename ColumnType::ValueType));
    auto compared_value = unalignedLoad<typename ColumnType::ValueType>(
        compared_value_data.data());

    const auto reference_value_data = reference_column->getDataAt(reference_row);
    chassert(reference_value_data.size() == sizeof(typename ColumnType::ValueType));
    auto reference_value = unalignedLoad<typename ColumnType::ValueType>(
        reference_value_data.data());

    /// Floats overflow to Inf and the comparison will work normally, so we don't have to do anything.
    if (offset_is_preceding)
        reference_value -= static_cast<typename ColumnType::ValueType>(offset);
    else
        reference_value += static_cast<typename ColumnType::ValueType>(offset);

    const auto result =  compared_value < reference_value ? -1
        : (compared_value == reference_value ? 0 : 1);

    return result;
}

// Helper macros to dispatch on type of the ORDER BY column
#define APPLY_FOR_ONE_NEST_TYPE(FUNCTION, TYPE) \
else if (typeid_cast<const TYPE *>(nest_compared_column.get())) \
{ \
    /* clang-tidy you're dumb, I can't put FUNCTION in braces here. */ \
    nest_compare_function = FUNCTION<TYPE>; /* NOLINT */ \
}

#define APPLY_FOR_NEST_TYPES(FUNCTION) \
if (false) /* NOLINT */ \
{ \
    /* Do nothing, a starter condition. */ \
} \
APPLY_FOR_ONE_NEST_TYPE(FUNCTION, ColumnVector<UInt8>) \
APPLY_FOR_ONE_NEST_TYPE(FUNCTION, ColumnVector<UInt16>) \
APPLY_FOR_ONE_NEST_TYPE(FUNCTION, ColumnVector<UInt32>) \
APPLY_FOR_ONE_NEST_TYPE(FUNCTION, ColumnVector<UInt64>) \
\
APPLY_FOR_ONE_NEST_TYPE(FUNCTION, ColumnVector<Int8>) \
APPLY_FOR_ONE_NEST_TYPE(FUNCTION, ColumnVector<Int16>) \
APPLY_FOR_ONE_NEST_TYPE(FUNCTION, ColumnVector<Int32>) \
APPLY_FOR_ONE_NEST_TYPE(FUNCTION, ColumnVector<Int64>) \
APPLY_FOR_ONE_NEST_TYPE(FUNCTION, ColumnVector<Int128>) \
\
APPLY_FOR_ONE_NEST_TYPE(FUNCTION##Float, ColumnVector<Float32>) \
APPLY_FOR_ONE_NEST_TYPE(FUNCTION##Float, ColumnVector<Float64>) \
\
else \
{ \
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, \
        "The RANGE OFFSET frame for '{}' ORDER BY nest column is not implemented", \
        demangle(typeid(nest_compared_column).name())); \
}

// A specialization of compareValuesWithOffset for nullable.
template <typename ColumnType>
static int compareValuesWithOffsetNullable(const IColumn * _compared_column,
    size_t compared_row, const IColumn * _reference_column,
    size_t reference_row,
    const Field & _offset,
    bool offset_is_preceding)
{
    const auto * compared_column = assert_cast<const ColumnType *>(
        _compared_column);
    const auto * reference_column = assert_cast<const ColumnType *>(
        _reference_column);

    if (compared_column->isNullAt(compared_row) && !reference_column->isNullAt(reference_row))
    {
        return -1;
    }
    if (compared_column->isNullAt(compared_row) && reference_column->isNullAt(reference_row))
    {
        return 0;
    }
    if (!compared_column->isNullAt(compared_row) && reference_column->isNullAt(reference_row))
    {
        return 1;
    }

    ColumnPtr nest_compared_column = compared_column->getNestedColumnPtr();
    ColumnPtr nest_reference_column = reference_column->getNestedColumnPtr();

    std::function<int(
        const IColumn * compared_column, size_t compared_row,
        const IColumn * reference_column, size_t reference_row,
        const Field & offset,
        bool offset_is_preceding)> nest_compare_function;
    APPLY_FOR_NEST_TYPES(compareValuesWithOffset)
    return nest_compare_function(nest_compared_column.get(), compared_row,
        nest_reference_column.get(), reference_row, _offset, offset_is_preceding);
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
APPLY_FOR_ONE_TYPE(FUNCTION##Nullable, ColumnNullable) \
else \
{ \
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, \
        "The RANGE OFFSET frame for '{}' ORDER BY column is not implemented", \
        demangle(typeid(*column).name())); \
}

WindowTransform::WindowTransform(SharedHeader input_header_,
        SharedHeader output_header_,
        const WindowDescription & window_description_,
        const std::vector<WindowFunctionDescription> & functions)
    : IProcessor({input_header_}, {output_header_})
    , input(inputs.front())
    , output(outputs.front())
    , input_header(*input_header_)
    , window_description(window_description_)
{
    // Materialize all columns in header, because we materialize all columns
    // in chunks and it's convenient if they match.
    auto input_columns = input_header.getColumns();
    for (auto & column : input_columns)
    {
        column = std::move(column)->convertToFullColumnIfConst();
    }
    input_header.setColumns(input_columns);

    resolveColumnIndices(functions);
    initWorkspaces(functions);
    setupRangeOffsetComparison();
}

void WindowTransform::initWorkspaces(const std::vector<WindowFunctionDescription> & functions)
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

        /// Currently we have slightly wrong mixup of the interfaces of Window and Aggregate functions.
        workspace.window_function_impl = dynamic_cast<IWindowFunction *>(const_cast<IAggregateFunction *>(aggregate_function.get()));

        /// Some functions may have non-standard default frame.
        /// Use it if it's the only function over the current window.
        if (window_description.frame.is_default && functions.size() == 1 && workspace.window_function_impl)
        {
            auto custom_default_frame = workspace.window_function_impl->getDefaultFrame();
            if (custom_default_frame)
                window_description.frame = *custom_default_frame;
        }

        if (workspace.window_function_impl && !workspace.window_function_impl->checkWindowFrameType(this))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported window frame type for function '{}'", workspace.aggregate_function->getName());

        workspace.is_aggregate_function_state = workspace.aggregate_function->isState();
        workspace.aggregate_function_state.reset(
            aggregate_function->sizeOfData(),
            aggregate_function->alignOfData());
        aggregate_function->create(workspace.aggregate_function_state.data());

        workspaces.push_back(std::move(workspace));
    }
}

void WindowTransform::resolveColumnIndices(const std::vector<WindowFunctionDescription> & functions)
{
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

    // We only need to materialize (remove Const/LowCardinality/Sparse from) the columns we actually
    // read while computing the window functions: the PARTITION BY and ORDER BY keys and the function
    // arguments. Everything else is passed through to the output untouched.
    should_materialize.assign(input_header.columns(), 0);
    for (const auto index : partition_by_indices)
        should_materialize[index] = 1;

    for (const auto index : order_by_indices)
        should_materialize[index] = 1;

    for (const auto & f : functions)
        for (const auto & argument_name : f.argument_names)
            should_materialize[input_header.getPositionByName(argument_name)] = 1;
}

void WindowTransform::setupRangeOffsetComparison()
{
    auto & frame = window_description.frame;
    const bool begin_is_offset = frame.begin_type == WindowFrame::BoundaryType::Offset;
    const bool end_is_offset = frame.end_type == WindowFrame::BoundaryType::Offset;
    const bool is_range_offset_frame = frame.type == WindowFrame::FrameType::RANGE && (begin_is_offset || end_is_offset);
    if (!is_range_offset_frame)
        return;

    // Choose a row comparison function for RANGE OFFSET frame based on the
    // type of the ORDER BY column.
    chassert(order_by_indices.size() == 1);
    const auto & entry = input_header.getByPosition(order_by_indices[0]);
    const IColumn * column = entry.column.get();
    APPLY_FOR_TYPES(compareValuesWithOffset)

    // Convert the offsets to the ORDER BY column type. We can't just check
    // that the type matches, because e.g. the int literals are always
    // (U)Int64, but the column might be Int8 and so on.
    auto convert_offset = [&](Field & offset, std::string_view bound_name)
    {
        offset = convertFieldToTypeOrThrow(offset, *entry.type, nullptr, {}, /*convert_inexact_floats=*/true);
        if (accurateLess(offset, Field(0)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window frame {} offset must be nonnegative, {} given", bound_name, offset);
    };

    if (begin_is_offset)
        convert_offset(frame.begin_offset, "start");
    if (end_is_offset)
        convert_offset(frame.end_offset, "end");
}

WindowTransform::~WindowTransform()
{
    // Some states may be not created yet if the creation failed.
    for (auto & ws : workspaces)
    {
        ws.aggregate_function->destroy(
            ws.aggregate_function_state.data());
    }
}

Columns & WindowTransform::inputAt(const RowNumber & x)
{
    chassert(x.block >= first_block_number);
    chassert(x.block - first_block_number < blocks.size());
    return blocks[x.block - first_block_number].input_columns;
}

WindowTransformBlock & WindowTransform::blockAt(const UInt64 block_number)
{
    chassert(block_number >= first_block_number);
    chassert(block_number - first_block_number < blocks.size());
    return blocks[block_number - first_block_number];
}

MutableColumns & WindowTransform::outputAt(const RowNumber & x)
{
    chassert(x.block >= first_block_number);
    chassert(x.block - first_block_number < blocks.size());
    return blocks[x.block - first_block_number].output_columns;
}

void WindowTransform::advancePartitionEnd()
{
    if (partition_ended)
    {
        return;
    }

    const RowNumber end = blocksEnd();

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
        chassert(partition_end == end);
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
    chassert(end.block == partition_end.block + 1);

    // Try to advance the partition end pointer.
    const size_t partition_by_columns = partition_by_indices.size();
    if (partition_by_columns == 0)
    {
        // No PARTITION BY. All input is one partition, which will end when the
        // input ends.
        partition_end = end;
        return;
    }

    // Check for partition end.
    // The partition ends when the PARTITION BY columns change. We need
    // some reference columns for comparison. We might have already
    // dropped the blocks where the partition starts, but any other row in the
    // partition will do. We can't use frame_start or frame_end or current_row (the next row
    // for which we are calculating the window functions), because they all might be
    // past the end of the partition. prev_frame_start is suitable, because it
    // is a pointer to the first row of the previous frame that must have been
    // valid, or to the first row of the partition, and we make sure not to drop
    // its block.
    chassert(partition_start <= prev_frame_start);
    // The frame start should be inside the prospective partition, except the
    // case when it still has no rows.
    chassert(prev_frame_start < partition_end || partition_start == partition_end);
    chassert(first_block_number <= prev_frame_start.block);
    const auto block_rows = blockRowsNumber(partition_end);

    // First, check whether the first unprocessed row already belongs to the next partition, by
    // comparing it against the reference row (prev_frame_start, which may live in another block, so
    // we can't fold it into the equal-range scan below).
    {
        size_t i = 0;
        for (; i < partition_by_columns; ++i)
        {
            const auto * reference_column
                = inputAt(prev_frame_start)[partition_by_indices[i]].get();
            const auto * compared_column
                = inputAt(partition_end)[partition_by_indices[i]].get();

            if (compared_column->compareAt(partition_end.row,
                    prev_frame_start.row, *reference_column,
                    1 /* nan_direction_hint */) != 0)
            {
                break;
            }
        }

        if (i < partition_by_columns)
        {
            partition_ended = true;
            return;
        }
    }

    // partition_end.row matches the reference on all PARTITION BY keys, so the partition extends over
    // the contiguous run of rows equal to it. The input is sorted by PARTITION BY, so we find that
    // run's end within this block with a fast equal-range scan.
    const size_t partition_end_row = getEqualRangeEndAssumeSorted(
        inputAt(partition_end), partition_by_indices, partition_end.row, block_rows, 1 /* nan_direction_hint */);

    if (partition_end_row < block_rows)
    {
        // Found the partition boundary inside this block.
        partition_end.row = partition_end_row;
        partition_ended = true;
        return;
    }

    // The partition runs to the end of this block, go to the next.
    ++partition_end.block;
    partition_end.row = 0;

    // Went until the end of data and didn't find the new partition.
    chassert(!partition_ended && partition_end == blocksEnd());
}

MovedRow WindowTransform::moveRowNumberNoCheck(const RowNumber & original_row_number, Int64 offset) const
{
    RowNumber moved_row_number = original_row_number;

    if (offset > 0 && moved_row_number != blocksEnd())
    {
        for (;;)
        {
            assertValid(moved_row_number);
            chassert(offset >= 0);

            const auto block_rows = blockRowsNumber(moved_row_number);
            moved_row_number.row += offset;
            if (moved_row_number.row >= block_rows)
            {
                offset = moved_row_number.row - block_rows;
                moved_row_number.row = 0;
                ++moved_row_number.block;

                if (moved_row_number == blocksEnd())
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
            assertValid(moved_row_number);
            chassert(offset <= 0);

            chassert(offset >= -INT64_MAX);
            if (moved_row_number.row >= -static_cast<UInt64>(offset))
            {
                moved_row_number.row -= -static_cast<UInt64>(offset);
                offset = 0;
                break;
            }

            // Move to the first row in current block. Note that the offset is
            // negative.
            offset += moved_row_number.row;
            moved_row_number.row = 0;

            // Move to the last row of the previous block, if we are not at the
            // first one. Offset also is incremented by one, because we pass over
            // the first row of this block.
            if (moved_row_number.block == first_block_number)
            {
                break;
            }

            --moved_row_number.block;
            offset += 1;
            moved_row_number.row = blockRowsNumber(moved_row_number) - 1;
        }
    }

    return {moved_row_number, offset};
}

MovedRow WindowTransform::moveRowNumber(const RowNumber & original_row_number, Int64 offset) const
{
    const MovedRow moved = moveRowNumberNoCheck(original_row_number, offset);

#ifndef NDEBUG
    /// Check that it was reversible. If we move back, we get the original row number with zero offset.
    const MovedRow moved_back = moveRowNumberNoCheck(moved.row, -(offset - moved.offset_left));
    chassert(moved_back.row == original_row_number);
    chassert(0 == moved_back.offset_left);
#endif

    return moved;
}


void WindowTransform::advanceFrameStartRowsOffset()
{
    // Just recalculate it each time by walking blocks.
    const auto [moved_row, offset_left] = moveRowNumber(current_row,
        window_description.frame.begin_offset.safeGet<UInt64>()
            * (window_description.frame.begin_preceding ? -1 : 1));

    frame_start = moved_row;

    assertValid(frame_start);

    // When moving backwards (PRECEDING) and we hit the start of available data
    // (offset_left < 0), the logical position is before partition_start.
    // We must check offset_left < 0 first because partition_start might point
    // to a block that has already been freed, making the comparison unreliable.
    if (frame_start <= partition_start
        || (window_description.frame.begin_preceding && offset_left < 0))
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
    frame_started = offset_left == 0;
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
            // partition_start is in the first group.
            frame_start_group_number = 1;
            frame_started = true;
            break;
        case WindowFrame::BoundaryType::Current:
            // CURRENT ROW differs between frame types only in how the peer
            // groups are accounted.
            chassert(partition_start <= peer_group_start);
            chassert(peer_group_start < partition_end);
            chassert(peer_group_start <= current_row);
            frame_start = peer_group_start;
            // peer_group_start is in the current group.
            frame_start_group_number = peer_group_number;
            frame_started = true;
            break;
        case WindowFrame::BoundaryType::Offset:
            switch (window_description.frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    advanceFrameStartRowsOffset();
                    break;
                case WindowFrame::FrameType::RANGE:
                    advanceFrameStartRangeOffset();
                    break;
                case WindowFrame::FrameType::GROUPS:
                    advanceFrameStartGroupsOffset();
                    break;
            }
            break;
    }

    chassert(frame_start_before <= frame_start);
    if (frame_start == frame_start_before)
    {
        // The frame start didn't move. Usually this means we re-validated a
        // position reached on an earlier call, so the frame is now started.
        // This happens in degenerate cases where the frame start is further than
        // the end of partition, and the partition ends at the last row of the
        // block, but we can only tell for sure after a new block arrives.
        // A GROUPS frame with a FOLLOWING-offset start is the exception: it can
        // leave frame_start at its previous position when it still needs more
        // input to locate the target peer group. Then the frame is not started
        // yet and the partition cannot have ended -- the main loop waits for
        // more data and retries.
        chassert(frame_started || !partition_ended);
    }

    chassert(partition_start <= frame_start);
    chassert(frame_start <= partition_end);
    if (partition_ended && frame_start == partition_end)
    {
        // Check that if the start of frame (e.g. FOLLOWING) runs into the end
        // of partition, it is marked as valid -- we can't advance it any
        // further.
        chassert(frame_started);
    }
}

bool WindowTransform::arePeers(const RowNumber & x, const RowNumber & y) const
{
    if (x == y)
    {
        // For convenience, a row is always its own peer.
        return true;
    }

    switch (window_description.frame.type)
    {
        case WindowFrame::FrameType::ROWS:
            // For a ROWS frame a row is only a peer with itself (checked above).
            return false;
        case WindowFrame::FrameType::RANGE:
        case WindowFrame::FrameType::GROUPS:
            // For RANGE and GROUPS frames, rows that compare equal on the ORDER
            // BY key are peers.
            break;
    }

    const size_t n = order_by_indices.size();
    if (n == 0)
    {
        // No ORDER BY, so all rows are peers.
        return true;
    }

    size_t i = 0;
    for (; i < n; ++i)
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
    // We only process one block here, and frame_end must be already in it: if
    // we didn't find the end in the previous block, frame_end is now the first
    // row of the current block. We need this knowledge to write a simpler loop
    // (only loop over rows and not over blocks), that should hopefully be more
    // efficient.
    // partition_end is either in this new block or past-the-end.
    chassert(frame_end.block  == partition_end.block
        || frame_end.block + 1 == partition_end.block);

    if (frame_end == partition_end)
    {
        // The case when we get a new block and find out that the partition has
        // ended.
        chassert(partition_ended);
        frame_ended = partition_ended;
        return;
    }

    // We advance until the partition end. It's either in the current block or
    // in the next one, which is also the past-the-end block. Figure out how
    // many rows we have to process.
    UInt64 rows_end = 0;
    if (partition_end.row == 0)
    {
        chassert(partition_end == blocksEnd());
        rows_end = blockRowsNumber(frame_end);
    }
    else
    {
        chassert(frame_end.block == partition_end.block);
        rows_end = partition_end.row;
    }
    // Equality would mean "no data to process", for which we checked above.
    chassert(frame_end.row < rows_end);

    // Advance frame_end to the end of the current row's peer group.
    if (window_description.frame.type != WindowFrame::FrameType::ROWS)
    {
        // RANGE/GROUPS: peers are the rows whose ORDER BY values equal current_row's (or all rows if
        // there is no ORDER BY). The input is sorted by ORDER BY within the partition, so we find the
        // peer group's end with a fast equal-range scan.
        // First check whether frame_end is still a peer of current_row -- the reference (current_row)
        // may be in a different block, so we compare against it directly.
        const size_t order_by_columns = order_by_indices.size();
        size_t i = 0;
        for (; i < order_by_columns; ++i)
        {
            const auto * reference_column = inputAt(current_row)[order_by_indices[i]].get();
            const auto * compared_column = inputAt(frame_end)[order_by_indices[i]].get();
            if (compared_column->compareAt(frame_end.row, current_row.row, *reference_column, 1 /* nan_direction_hint */) != 0)
            {
                break;
            }
        }

        if (i < order_by_columns)
        {
            // frame_end is already past the current row's peer group.
            frame_ended = true;
            return;
        }

        // frame_end is a peer; extend over the run of equal ORDER BY values within this block,
        // narrowing key by key (the data is sorted lexicographically). With no ORDER BY, all rows are peers,
        // so the scan will just return the end of the block.
        const UInt64 peer_group_end_row
            = getEqualRangeEndAssumeSorted(inputAt(frame_end), order_by_indices, frame_end.row, rows_end, 1 /* nan_direction_hint */);

        if (peer_group_end_row < rows_end)
        {
            frame_end.row = peer_group_end_row;
            frame_ended = true;
            return;
        }
        frame_end.row = rows_end;
    }
    else
    {
        // ROWS frame: a row is only its own peer, so the peer group is just current_row, and
        // frame_end sits at current_row on entry -- advancing it one row reaches the peer group's
        // end.
        if (frame_end == current_row)
            ++frame_end.row;

        if (frame_end.row < rows_end)
        {
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
    chassert(frame_end == partition_end);
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
        window_description.frame.end_offset.safeGet<UInt64>()
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

    // When moving backwards (PRECEDING) and we hit the start of available data
    // (offset_left < 0), the logical position is before partition_start.
    // We must check offset_left < 0 first because partition_start might point
    // to a block that has already been freed, making the comparison unreliable.
    if (moved_row <= partition_start
        || (window_description.frame.end_preceding && offset_left < 0))
    {
        // Clamp to the start of partition.
        frame_end = partition_start;
        frame_ended = true;
        return;
    }

    // Frame end inside partition, if we walked all the offset, it's final.
    frame_end = moved_row;
    frame_ended = offset_left == 0;
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

RowNumber WindowTransform::findPeerGroupEnd(const RowNumber & start, RowNumber & scan_frontier, bool & need_more_data) const
{
    need_more_data = false;

    if (start == partition_end)
        return partition_end;

    // Resume from the frontier of a previous, unfinished scan of the same peer group: every row in
    // [start, scan_frontier] is already known to be a peer of `start`. A frontier before `start` is
    // stale (the boundary has moved to another group or partition since the last scan).
    if (scan_frontier < start)
        scan_frontier = start;

    // Walk forward block by block while the peer group keeps extending.
    const UInt64 blocks_end_block = first_block_number + blocks.size();
    for (RowNumber cur = scan_frontier; cur.block < blocks_end_block; cur = RowNumber{cur.block + 1, 0})
    {
        const size_t block_rows = blockRowsNumber(cur);
        const bool partition_ends_in_block = partition_end.block == cur.block;
        const size_t end_bound = partition_ends_in_block ? partition_end.row : block_rows;

        // `cur` is a valid row inside the partition, so the equal-range search has at least one row.
        chassert(cur.row < end_bound);

        // Try to jump over the whole peer group at once: the end of the run of rows equal to `cur` across
        // all ORDER BY columns, within the sorted, partition-bounded range [cur.row, end_bound).
        const size_t run_end = getEqualRangeEndAssumeSorted(
            inputAt(cur), order_by_indices, cur.row, end_bound, 1 /* nan_direction_hint */);

        if (run_end < end_bound)
            return RowNumber{cur.block, run_end};   // a real peer-group boundary inside this block

        // No earlier boundary, so the run of peers reached the bound. getEqualRangeEndAssumeSorted
        // never returns past `end_bound`, so the group extends exactly to the end of what we scanned
        // in this block -- the precondition for both the partition-end and cross-block cases below.
        chassert(run_end == end_bound);

        if (partition_ends_in_block)
            return partition_end;                   // the peer group reaches the partition end

        // The group extends to the end of `cur`'s block. It continues into the next block only if
        // that block is buffered, is still in this partition, and its first row is a peer.
        const RowNumber next_block_start{cur.block + 1, 0};

        // We cannot extend the scan into the next block when it has not arrived yet, or when the next
        // row is the partition boundary (a peer group never crosses partitions). In both cases the
        // group's end depends on whether the partition has ended, which is decided after the loop.
        // Remember the proven scan progress so a retry does not rescan the group from its first row.
        if (next_block_start.block >= blocks_end_block || next_block_start == partition_end)
        {
            scan_frontier = RowNumber{cur.block, block_rows - 1};
            break;
        }

        if (!arePeers({cur.block, block_rows - 1}, next_block_start))
            return next_block_start;                // the peer group ends exactly at the block boundary

        // Otherwise the group spans the boundary; the loop advances `cur` into the next block.
    }

    // We broke out because the group either reaches a partition boundary that sits on a block edge,
    // or extends past the rows we can currently resolve. If the partition has ended, the group ends
    // at the partition end.
    if (partition_ended)
        return partition_end;

    // The partition has not ended and we ran past the buffered rows wait for more input.
    chassert(partition_end == blocksEnd());
    need_more_data = true;
    return start;
}

bool WindowTransform::advanceGroupBoundary(RowNumber & pointer, UInt64 & group_counter, RowNumber & scan_frontier, Int64 target_group) const
{
    chassert(target_group >= 1);
    chassert(group_counter <= static_cast<UInt64>(std::numeric_limits<Int64>::max()));

    while (static_cast<Int64>(group_counter) < target_group)
    {
        bool need_more_data = false;
        const RowNumber group_end = findPeerGroupEnd(pointer, scan_frontier, need_more_data);

        if (need_more_data)
        {
            // Leave `pointer` and `group_counter` untouched so we can resume later.
            return false;
        }

        if (group_end == partition_end)
        {
            // The target peer group is past the last group in the partition; clamp to the end.
            pointer = partition_end;
            return true;
        }

        // Move to the first row of the next peer group.
        pointer = group_end;
        ++group_counter;
    }

    return true;
}

void WindowTransform::advanceFrameStartGroupsOffset()
{
    const Int64 offset
        = static_cast<Int64>(window_description.frame.begin_offset.safeGet<UInt64>()) * (window_description.frame.begin_preceding ? -1 : 1);

    // The frame starts at the first row of the peer group `offset` groups away from the current one.
    const Int64 target_group = static_cast<Int64>(peer_group_number) + offset;

    if (target_group <= 1)
    {
        // The target peer group is at or before the first group: clamp to the partition start.
        frame_start = partition_start;
        frame_start_group_number = 1;
        frame_started = true;
        return;
    }

    frame_started = advanceGroupBoundary(frame_start, frame_start_group_number, frame_start_group_scan_frontier, target_group);
}

void WindowTransform::advanceFrameEndGroupsOffset()
{
    if (frame_end == frame_start)
        frame_end_group_number = frame_start_group_number;

    const Int64 offset
        = static_cast<Int64>(window_description.frame.end_offset.safeGet<UInt64>()) * (window_description.frame.end_preceding ? -1 : 1);

    // frame_end is not inclusive, so it must reach the first row of the group after the target one.
    const Int64 target_group = static_cast<Int64>(peer_group_number) + offset + 1;

    if (target_group <= 1)
    {
        // The frame ends before the first peer group: it is empty.
        frame_end = frame_start;
        frame_end_group_number = frame_start_group_number;
        frame_ended = true;
        return;
    }

    frame_ended = advanceGroupBoundary(frame_end, frame_end_group_number, frame_end_group_scan_frontier, target_group);
}

void WindowTransform::advanceFrameEnd()
{
    // No reason for this function to be called again after it succeeded.
    chassert(!frame_ended);

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
                case WindowFrame::FrameType::ROWS:
                    advanceFrameEndRowsOffset();
                    break;
                case WindowFrame::FrameType::RANGE:
                    advanceFrameEndRangeOffset();
                    break;
                case WindowFrame::FrameType::GROUPS:
                    advanceFrameEndGroupsOffset();
                    break;
            }
            break;
    }

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
    // Assert that the frame boundaries are known, have proper order wrt each
    // other, and have not gone back wrt the previous frame.
    chassert(frame_started);
    chassert(frame_ended);
    chassert(frame_start <= frame_end);
    chassert(prev_frame_start <= prev_frame_end);
    chassert(prev_frame_start <= frame_start);
    chassert(prev_frame_end <= frame_end);
    chassert(partition_start <= frame_start);
    chassert(frame_end <= partition_end);

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
            a->addBatchSinglePlace(first_row, past_the_end_row, buf, columns, arena_ptr);
        }
    }
}

void WindowTransform::writeOutCurrentRow()
{
    chassert(current_row < partition_end);
    chassert(current_row.block >= first_block_number);

    // Whether this row's frame equals the previous row's. When current_row_number == 1 it's the first
    // row of the partition, so there's no previous row in this partition (and thus no previous frame)
    // to compare against.
    const bool frame_unchanged = current_row_number > 1 && frame_start == prev_frame_start && frame_end == prev_frame_end;

    const auto & block = blockAt(current_row);
    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];

        if (ws.window_function_impl)
        {
            ws.window_function_impl->windowInsertResultInto(this, wi);
            continue;
        }

        IColumn * result_column = block.output_columns[wi].get();
        const auto * a = ws.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        if (frame_unchanged && !ws.is_aggregate_function_state && current_row.row > 0)
        {
            // Same frame as the previous row -> same result. When that row is in this same block its
            // result is already in result_column one position back, so copy it instead of
            // re-finalizing. We copy the column into itself with insertRangeFrom (not insertFrom):
            // insertRangeFrom appends via resize + memcpy from a disjoint source range, which is
            // self-safe even if the append reallocates and even for nested columns (Array, Variant,
            // Dynamic, JSON) whose sub-columns are not covered by the top-level reserve.
            chassert(result_column->size() == current_row.row);
            result_column->insertRangeFrom(*result_column, current_row.row - 1, 1);
        }
        else if (ws.is_aggregate_function_state)
        {
            /// We should use insertMergeResultInto to insert result into ColumnAggregateFunction
            /// correctly if result contains AggregateFunction's states
            a->insertMergeResultInto(buf, *result_column, arena.get());
        }
        else
        {
            a->insertResultInto(buf, *result_column, arena.get());
        }
    }
}

static void assertSameColumns(const Columns & left_all, const Columns & right_all, const std::vector<UInt8> & columns_to_check)
{
    chassert(left_all.size() == right_all.size());

    for (size_t i = 0; i < left_all.size(); ++i)
    {
        // Only the materialized columns are guaranteed to match the (materialized) header structure;
        // the pass-through columns are left in their original representation.
        if (!columns_to_check[i])
            continue;

        const auto * left_column = left_all[i].get();
        const auto * right_column = right_all[i].get();

        chassert(left_column);
        chassert(right_column);

        if (const auto * left_lc = typeid_cast<const ColumnLowCardinality *>(left_column))
            left_column = left_lc->getDictionary().getNestedColumn().get();

        if (const auto * right_lc = typeid_cast<const ColumnLowCardinality *>(right_column))
            right_column = right_lc->getDictionary().getNestedColumn().get();

        chassert(typeid(*left_column).hash_code()
            == typeid(*right_column).hash_code());

        if (isColumnConst(*left_column))
        {
            Field left_value = assert_cast<const ColumnConst &>(*left_column).getField();
            Field right_value = assert_cast<const ColumnConst &>(*right_column).getField();

            chassert(left_value == right_value);
        }
    }
}

void WindowTransform::addInputBlock(Chunk chunk)
{
    blocks.push_back({});
    auto & block = blocks.back();

    // Use the number of rows from the Chunk, because it is correct even in
    // the case where the Chunk has no columns. Not sure if this actually
    // happens, because even in the case of `count() over ()` we have a dummy
    // input column.
    block.rows = chunk.getNumRows();

    // If we have a (logically) constant column, some Chunks will have a
    // Const column for it, and some -- materialized. Such difference is
    // generated by e.g. MergingSortedAlgorithm, which mostly materializes
    // the constant ORDER BY columns, but in some obscure cases passes them
    // through, unmaterialized. This mix is a pain to work with in Window
    // Transform, because we have to compare columns across blocks, when e.g.
    // searching for peer group boundaries, and each of the four combinations
    // of const and materialized requires different code.
    // Another problem with Const columns is that the aggregate functions
    // can't work with them, so we have to materialize them like the
    // Aggregator does.
    // Likewise, aggregate functions can't work with LowCardinality,
    // so we have to materialize them too.
    // We only materialize the columns we actually read: the PARTITION BY / ORDER BY keys and
    // the function arguments. The other columns are emitted to the output as-is from original_input_columns to
    // avoid paying unnecessary Const/LowCardinality/Sparse cost.
    auto columns = chunk.detachColumns();
    block.original_input_columns = columns;
    for (size_t i = 0; i < columns.size(); ++i)
        if (should_materialize[i])
            columns[i] = recursiveRemoveLowCardinality(std::move(columns[i])->convertToFullIfWrapped());

    block.input_columns = std::move(columns);

    // Initialize output columns.
    for (auto & ws : workspaces)
    {
        block.output_columns.push_back(ws.aggregate_function->getResultType()
            ->createColumn());
        block.output_columns.back()->reserve(block.rows);
    }

    // As a debugging aid, assert that all chunks have the same C++ type of
    // columns, that also matches the input header, because we often have to
    // work across chunks.
    assertSameColumns(input_header.getColumns(), block.input_columns, should_materialize);
}

void WindowTransform::computeReadyRows()
{
    // First, advance the partition end.
    for (;;)
    {
        advancePartitionEnd();
        // Either we ran out of data or we found the end of partition (maybe
        // both, but this only happens at the total end of data).
        chassert(partition_ended || partition_end == blocksEnd());
        if (partition_ended && partition_end == blocksEnd())
        {
            chassert(input_is_finished);
        }

        // After that, try to calculate window functions for each next row.
        // We can continue until the end of partition or current end of data,
        // which is precisely the definition of `partition_end`.
        while (current_row < partition_end)
        {
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
                chassert(!input_is_finished);
                chassert(!partition_ended);
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
                chassert(!input_is_finished);
                chassert(!partition_ended);
                return;
            }

            // The frame can be empty sometimes, e.g. the boundaries coincide
            // or the start is after the partition end. But hopefully start is
            // not after end.
            chassert(frame_started);
            chassert(frame_ended);
            chassert(frame_start <= frame_end);

            // Now that we know the new frame boundaries, update the aggregation
            // states. Theoretically we could do this simultaneously with moving
            // the frame boundaries, but it would require some care not to
            // perform unnecessary work while we are still looking for the frame
            // start, so do it the simple way for now.
            updateAggregationState();

            // Write out the aggregation results.
            writeOutCurrentRow();

            if (isCancelled())
            {
                // Good time to check if the query is cancelled. Checking once
                // per block might not be enough in severe quadratic cases.
                // Just leave the work halfway through and return, the 'prepare'
                // method will figure out what to do. Note that this doesn't
                // handle 'max_execution_time' and other limits, because these
                // limits are only updated between blocks. Eventually we should
                // start updating them in background and canceling the processor,
                // like we do for Ctrl+C handling.
                //
                // This class is final, so the check should hopefully be
                // devirtualized and become a single never-taken branch that is
                // basically free.
                return;
            }

            prev_frame_start = frame_start;
            prev_frame_end = frame_end;

            // Move to the next row. The frame will have to be recalculated.
            // The peer group start is updated at the beginning of the loop,
            // because current_row might now be past-the-end.
            advanceRowNumber(current_row);
            ++current_row_number;
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
            chassert(partition_end == blocksEnd());
            chassert(!input_is_finished);
            return;
        }

        startNextPartition();
    }
}

void WindowTransform::startNextPartition()
{
    partition_start = partition_end;
    advanceRowNumber(partition_end);
    partition_ended = false;
    // We have to reset the frame and other pointers when the new partition
    // starts.
    frame_start = partition_start;
    frame_end = partition_start;
    prev_frame_start = partition_start;
    prev_frame_end = partition_start;
    chassert(current_row == partition_start);
    current_row_number = 1;
    peer_group_start = partition_start;
    peer_group_start_row_number = 1;
    peer_group_number = 1;
    frame_start_group_number = 1;
    frame_end_group_number = 1;

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

    // Replace the arena so that it does not grow across partitions. All states
    // were destroyed above and no result lives in it, see the field comment.
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

IProcessor::Status WindowTransform::prepare()
{
    if (output.isFinished() || isCancelled())
    {
        // output.isFinished(): the consumer closed the port early, e.g. LIMIT is
        // satisfied. isCancelled(): KILL QUERY, a client disconnect or Ctrl+C
        // cancelled the processor. Either way there is nothing more to produce.
        input.close();
        return Status::Finished;
    }

    chassert(current_row.block >= first_block_number);
    // The current_row might be past-the-end if we have already calculated the
    // window functions for all input rows. That's why the equality is also
    // valid here.
    chassert(current_row.block <= first_block_number + blocks.size());
    chassert(next_output_block_number >= first_block_number);

    // Output the ready data prepared by work(). A block is ready when the
    // current row has left it, because rows are computed in order.
    // We inspect the calculation state and create the output chunk right here,
    // because this is pretty lightweight.
    if (next_output_block_number < current_row.block)
    {
        if (output.canPush())
        {
            // Output the ready block.
            const auto i = next_output_block_number - first_block_number;
            auto & block = blocks[i];
            auto columns = block.original_input_columns;
            for (auto & res : block.output_columns)
            {
                columns.push_back(ColumnPtr(std::move(res)));
            }
            Chunk chunk;
            chunk.setColumns(columns, block.rows);

            ++next_output_block_number;

            output.push(std::move(chunk));
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
        chassert(next_output_block_number == first_block_number + blocks.size());
        chassert(current_row == blocksEnd());

        // The consumer learns that the data ended only from the closed output port.
        output.finish();

        return Status::Finished;
    }

    // Consume input data if we have any ready.
    if (!pending_input && input.hasData())
    {
        // Pulling with set_not_needed = true and using an explicit setNeeded()
        // later is somewhat more efficient, because after the setNeeded(), the
        // required input block will be generated in the same thread and passed
        // to our prepare() + work() methods in the same thread right away, so
        // hopefully we will work on hot (cached) data.
        pending_input = input.pull(true /* set_not_needed */);

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
        chassert(!input_is_finished);
        input_is_finished = true;
        return Status::Ready;
    }

    // We have to wait for more input.
    input.setNeeded();
    return Status::NeedData;
}

void WindowTransform::work()
{
    chassert(pending_input || input_is_finished);

    if (pending_input)
    {
        Chunk chunk = std::exchange(pending_input, std::nullopt).value();
        if (!chunk.hasRows())
            return;

        addInputBlock(std::move(chunk));
    }

    computeReadyRows();
    releaseUnusedBlocks();
}

void WindowTransform::releaseUnusedBlocks()
{
    // We don't really have to keep the entire partition, and it can be big, so
    // we want to drop the starting blocks to save memory. We can drop the old
    // blocks if we already returned them as output, and the frame and the
    // current row are already past them. We also need to keep the previous
    // frame start because we use it as the partition etalon. It is always less
    // than the current frame start, so we don't have to check the latter. Note
    // that the frame start can be further than current row for some frame specs
    // (e.g. EXCLUDE CURRENT ROW), so we have to check both.
    // We also keep the start of the current peer group: it can lag behind the
    // current row (its group may have started in an earlier block), and it is
    // dereferenced by arePeers() on the next row. A FOLLOWING frame pushes the
    // frame pointers ahead of the current row, so peer_group_start can be the
    // trailing pointer.
    chassert(prev_frame_start <= frame_start);
    const auto first_used_block = std::min({next_output_block_number, prev_frame_start.block, current_row.block, peer_group_start.block});
    if (first_block_number < first_used_block)
    {
        blocks.erase(blocks.begin(),
            blocks.begin() + (first_used_block - first_block_number));
        first_block_number = first_used_block;

        chassert(next_output_block_number >= first_block_number);
        chassert(frame_start.block >= first_block_number);
        chassert(prev_frame_start.block >= first_block_number);
        chassert(current_row.block >= first_block_number);
        chassert(peer_group_start.block >= first_block_number);
    }
}

}
