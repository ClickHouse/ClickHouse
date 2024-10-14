#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Processors/Transforms/WindowTransform.h>
#include <base/arithmeticOverflow.h>
#include <Common/Arena.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeString.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

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

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
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
    assert(offset >= 0);

    const auto compared_value_data = compared_column->getDataAt(compared_row);
    assert(compared_value_data.size == sizeof(ValueType));
    auto compared_value = unalignedLoad<ValueType>(
        compared_value_data.data);

    const auto reference_value_data = reference_column->getDataAt(reference_row);
    assert(reference_value_data.size == sizeof(ValueType));
    auto reference_value = unalignedLoad<ValueType>(
        reference_value_data.data);

    bool is_overflow;
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
    assert(compared_value_data.size == sizeof(typename ColumnType::ValueType));
    auto compared_value = unalignedLoad<typename ColumnType::ValueType>(
        compared_value_data.data);

    const auto reference_value_data = reference_column->getDataAt(reference_row);
    assert(reference_value_data.size == sizeof(typename ColumnType::ValueType));
    auto reference_value = unalignedLoad<typename ColumnType::ValueType>(
        reference_value_data.data);

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
    // Materialize all columns in header, because we materialize all columns
    // in chunks and it's convenient if they match.
    auto input_columns = input_header.getColumns();
    for (auto & column : input_columns)
    {
        column = std::move(column)->convertToFullColumnIfConst();
    }
    input_header.setColumns(input_columns);

    // Initialize window function workspaces.
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

        workspace.is_aggregate_function_state = workspace.aggregate_function->isState();
        workspace.aggregate_function_state.reset(
            aggregate_function->sizeOfData(),
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

    // Choose a row comparison function for RANGE OFFSET frame based on the
    // type of the ORDER BY column.
    if (window_description.frame.type == WindowFrame::FrameType::RANGE
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

    for (const auto & workspace : workspaces)
    {
        if (workspace.window_function_impl)
        {
            if (!workspace.window_function_impl->checkWindowFrameType(this))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported window frame type for function '{}'",
                    workspace.aggregate_function->getName());
            }
        }

    }
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
    assert(partition_start <= prev_frame_start);
    // The frame start should be inside the prospective partition, except the
    // case when it still has no rows.
    assert(prev_frame_start < partition_end || partition_start == partition_end);
    assert(first_block_number <= prev_frame_start.block);
    const auto block_rows = blockRowsNumber(partition_end);
    for (; partition_end.row < block_rows; ++partition_end.row)
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

    // Went until the end of block, go to the next.
    assert(partition_end.row == block_rows);
    ++partition_end.block;
    partition_end.row = 0;

    // Went until the end of data and didn't find the new partition.
    assert(!partition_ended && partition_end == blocksEnd());
}

auto WindowTransform::moveRowNumberNoCheck(const RowNumber & original_row_number, Int64 offset) const
{
    RowNumber moved_row_number = original_row_number;

    if (offset > 0 && moved_row_number != blocksEnd())
    {
        for (;;)
        {
            assertValid(moved_row_number);
            assert(offset >= 0);

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
            assert(offset <= 0);

            // abs(offset) is less than INT64_MAX, as checked in the parser, so
            // this negation should always work.
            assert(offset >= -INT64_MAX);
            if (moved_row_number.row >= static_cast<UInt64>(-offset))
            {
                moved_row_number.row -= -offset;
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

    return std::tuple<RowNumber, Int64>{moved_row_number, offset};
}

auto WindowTransform::moveRowNumber(const RowNumber & original_row_number, Int64 offset) const
{
    auto [moved_row_number, offset_after_move] = moveRowNumberNoCheck(original_row_number, offset);

#ifndef NDEBUG
    /// Check that it was reversible. If we move back, we get the original row number with zero offset.
    const auto [original_row_number_to_validate, offset_after_move_back]
        = moveRowNumberNoCheck(moved_row_number, -(offset - offset_after_move));

    assert(original_row_number_to_validate == original_row_number);
    assert(0 == offset_after_move_back);
#endif

    return std::tuple<RowNumber, Int64>{moved_row_number, offset_after_move};
}


void WindowTransform::advanceFrameStartRowsOffset()
{
    // Just recalculate it each time by walking blocks.
    const auto [moved_row, offset_left] = moveRowNumber(current_row,
        window_description.frame.begin_offset.safeGet<UInt64>()
            * (window_description.frame.begin_preceding ? -1 : 1));

    frame_start = moved_row;

    assertValid(frame_start);

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
                case WindowFrame::FrameType::ROWS:
                    advanceFrameStartRowsOffset();
                    break;
                case WindowFrame::FrameType::RANGE:
                    advanceFrameStartRangeOffset();
                    break;
                default:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Frame start type '{}' for frame '{}' is not implemented",
                        window_description.frame.begin_type,
                        window_description.frame.type);
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

    if (window_description.frame.type == WindowFrame::FrameType::ROWS)
    {
        // For ROWS frame, row is only peers with itself (checked above);
        return false;
    }

    // For RANGE and GROUPS frames, rows that compare equal w/ORDER BY are peers.
    assert(window_description.frame.type == WindowFrame::FrameType::RANGE);
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
    UInt64 rows_end;
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

    // Advance frame_end while it is still peers with the current row.
    for (; frame_end.row < rows_end; ++frame_end.row)
    {
        if (!arePeers(current_row, frame_end))
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
                case WindowFrame::FrameType::ROWS:
                    advanceFrameEndRowsOffset();
                    break;
                case WindowFrame::FrameType::RANGE:
                    advanceFrameEndRangeOffset();
                    break;
                default:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "The frame end type '{}' is not implemented",
                        window_description.frame.end_type);
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
    assert(frame_started);
    assert(frame_ended);
    assert(frame_start <= frame_end);
    assert(prev_frame_start <= prev_frame_end);
    assert(prev_frame_start <= frame_start);
    assert(prev_frame_end <= frame_end);
    assert(partition_start <= frame_start);
    assert(frame_end <= partition_end);

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

            if (ws.is_aggregate_function_state)
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
}

static void assertSameColumns(const Columns & left_all,
    const Columns & right_all)
{
    assert(left_all.size() == right_all.size());

    for (size_t i = 0; i < left_all.size(); ++i)
    {
        const auto * left_column = left_all[i].get();
        const auto * right_column = right_all[i].get();

        assert(left_column);
        assert(right_column);

        if (const auto * left_lc = typeid_cast<const ColumnLowCardinality *>(left_column))
            left_column = left_lc->getDictionary().getNestedColumn().get();

        if (const auto * right_lc = typeid_cast<const ColumnLowCardinality *>(right_column))
            right_column = right_lc->getDictionary().getNestedColumn().get();

        assert(typeid(*left_column).hash_code()
            == typeid(*right_column).hash_code());

        if (isColumnConst(*left_column))
        {
            Field left_value = assert_cast<const ColumnConst &>(*left_column).getField();
            Field right_value = assert_cast<const ColumnConst &>(*right_column).getField();

            assert(left_value == right_value);
        }
    }
}

void WindowTransform::appendChunk(Chunk & chunk)
{
    // First, prepare the new input block and add it to the queue. We might not
    // have it if it's end of data, though.
    if (!input_is_finished)
    {
        if (!chunk.hasRows())
        {
            // Joins may generate empty input chunks when it's not yet end of
            // input. Just ignore them. They probably shouldn't be sending empty
            // chunks up the pipeline, but oh well.
            return;
        }

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
        // Just materialize everything.
        auto columns = chunk.detachColumns();
        block.original_input_columns = columns;
        for (auto & column : columns)
            column = recursiveRemoveLowCardinality(std::move(column)->convertToFullColumnIfConst()->convertToFullColumnIfSparse());
        block.input_columns = std::move(columns);

        // Initialize output columns.
        for (auto & ws : workspaces)
        {
            block.casted_columns.push_back(ws.window_function_impl ? ws.window_function_impl->castColumn(block.input_columns, ws.argument_column_indices) : nullptr);

            block.output_columns.push_back(ws.aggregate_function->getResultType()
                ->createColumn());
            block.output_columns.back()->reserve(block.rows);
        }

        // As a debugging aid, assert that all chunks have the same C++ type of
        // columns, that also matches the input header, because we often have to
        // work across chunks.
        assertSameColumns(input_header.getColumns(), block.input_columns);
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
    if (output.isFinished() || isCancelled())
    {
        // The consumer asked us not to continue (or we decided it ourselves),
        // so we abort. Not sure what the difference between the two conditions
        // is, but it seemed that output.isFinished() is not enough to cancel on
        // Ctrl+C. Test manually if you change it.
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
            auto columns = block.original_input_columns;
            for (auto & res : block.output_columns)
            {
                columns.push_back(ColumnPtr(std::move(res)));
            }
            output_data.chunk.setColumns(columns, block.rows);

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
    // we want to drop the starting blocks to save memory. We can drop the old
    // blocks if we already returned them as output, and the frame and the
    // current row are already past them. We also need to keep the previous
    // frame start because we use it as the partition etalon. It is always less
    // than the current frame start, so we don't have to check the latter. Note
    // that the frame start can be further than current row for some frame specs
    // (e.g. EXCLUDE CURRENT ROW), so we have to check both.
    assert(prev_frame_start <= frame_start);
    const auto first_used_block = std::min({next_output_block_number, prev_frame_start.block, current_row.block});
    if (first_block_number < first_used_block)
    {
        blocks.erase(blocks.begin(),
            blocks.begin() + (first_used_block - first_block_number));
        first_block_number = first_used_block;

        assert(next_output_block_number >= first_block_number);
        assert(frame_start.block >= first_block_number);
        assert(prev_frame_start.block >= first_block_number);
        assert(current_row.block >= first_block_number);
        assert(peer_group_start.block >= first_block_number);
    }
}

struct WindowFunctionRank final : public StatelessWindowFunction
{
    WindowFunctionRank(const std::string & name_, const DataTypes & argument_types_, const Array & parameters_)
        : StatelessWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeUInt64>())
    {}

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        IColumn & to = *transform->blockAt(transform->current_row)
            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
            transform->peer_group_start_row_number);
    }
};

struct WindowFunctionDenseRank final : public StatelessWindowFunction
{
    WindowFunctionDenseRank(const std::string & name_, const DataTypes & argument_types_, const Array & parameters_)
        : StatelessWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeUInt64>())
    {}

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        IColumn & to = *transform->blockAt(transform->current_row)
            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
            transform->peer_group_number);
    }
};

namespace recurrent_detail
{
    template<typename T> T getValue(const WindowTransform * /*transform*/, size_t /*function_index*/, size_t /*column_index*/, RowNumber /*row*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "recurrent_detail::getValue() is not implemented for {} type", typeid(T).name());
    }

    template<> Float64 getValue<Float64>(const WindowTransform * transform, size_t function_index, size_t column_index, RowNumber row)
    {
        const auto & workspace = transform->workspaces[function_index];
        const auto & column = transform->blockAt(row.block).input_columns[workspace.argument_column_indices[column_index]];
        return column->getFloat64(row.row);
    }

    template<typename T> void setValueToOutputColumn(const WindowTransform * /*transform*/, size_t /*function_index*/, T /*value*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "recurrent_detail::setValueToOutputColumn() is not implemented for {} type", typeid(T).name());
    }

    template<> void setValueToOutputColumn<Float64>(const WindowTransform * transform, size_t function_index, Float64 value)
    {
        auto current_row = transform->current_row;
        const auto & current_block = transform->blockAt(current_row);
        IColumn & to = *current_block.output_columns[function_index];

        assert_cast<ColumnFloat64 &>(to).getData().push_back(value);
    }
}

struct WindowFunctionHelpers
{
    template<typename T>
    static T getValue(const WindowTransform * transform, size_t function_index, size_t column_index, RowNumber row)
    {
        return recurrent_detail::getValue<T>(transform, function_index, column_index, row);
    }

    template<typename T>
    static void setValueToOutputColumn(const WindowTransform * transform, size_t function_index, T value)
    {
        recurrent_detail::setValueToOutputColumn<T>(transform, function_index, value);
    }

    ALWAYS_INLINE static bool checkPartitionEnterFirstRow(const WindowTransform * transform) { return transform->current_row_number == 1; }

    ALWAYS_INLINE static bool checkPartitionEnterLastRow(const WindowTransform * transform)
    {
        /// This is for fast check.
        if (!transform->partition_ended)
            return false;

        auto current_row = transform->current_row;
        /// checkPartitionEnterLastRow is called on each row, also move on current_row.row here.
        current_row.row++;
        const auto & partition_end_row = transform->partition_end;

        /// The partition end is reached, when following is true
        /// - current row is the partition end row,
        /// - or current row is the last row of all input.
        if (current_row != partition_end_row)
        {
            /// when current row is not the partition end row, we need to check whether it's the last
            /// input row.
            if (current_row.row < transform->blockRowsNumber(current_row))
                return false;
            if (partition_end_row.block != current_row.block + 1 || partition_end_row.row)
                return false;
        }
        return true;
    }
};

struct ExponentialTimeDecayedSumState
{
    Float64 previous_time;
    Float64 previous_sum;
};

struct ExponentialTimeDecayedAvgState
{
    Float64 previous_time;
    Float64 previous_sum;
    Float64 previous_count;
};

struct WindowFunctionExponentialTimeDecayedSum final : public StatefulWindowFunction<ExponentialTimeDecayedSumState>
{
    static constexpr size_t ARGUMENT_VALUE = 0;
    static constexpr size_t ARGUMENT_TIME = 1;

    static Float64 getDecayLength(const Array & parameters_, const std::string & name_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one parameter", name_);
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }

    WindowFunctionExponentialTimeDecayedSum(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(getDecayLength(parameters_, name_))
    {
        if (argument_types.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly two arguments", name_);
        }

        if (!isNumber(argument_types[ARGUMENT_VALUE]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be a number, '{}' given",
                ARGUMENT_VALUE,
                argument_types[ARGUMENT_VALUE]->getName());
        }

        if (!isNumber(argument_types[ARGUMENT_TIME]) && !isDateTime(argument_types[ARGUMENT_TIME]) && !isDateTime64(argument_types[ARGUMENT_TIME]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be DateTime, DateTime64 or a number, '{}' given",
                ARGUMENT_TIME,
                argument_types[ARGUMENT_TIME]->getName());
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & workspace = transform->workspaces[function_index];
        auto & state = getState(workspace);

        Float64 result = 0;

        if (transform->frame_start < transform->frame_end)
        {
            RowNumber frame_back = transform->prevRowNumber(transform->frame_end);
            Float64 back_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, frame_back);

            if (transform->prev_frame_start <= transform->frame_start
                && transform->frame_start < transform->prev_frame_end
                && transform->prev_frame_end <= transform->frame_end)
            {
                for (RowNumber i = transform->prev_frame_start; i < transform->frame_start; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);
                    result -= std::exp((prev_t - back_t) / decay_length) * prev_val;
                }
                result += std::exp((state.previous_time - back_t) / decay_length) * state.previous_sum;
                for (RowNumber i = transform->prev_frame_end; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);
                    result += std::exp((prev_t - back_t) / decay_length) * prev_val;
                }
            }
            else
            {
                for (RowNumber i = transform->frame_start; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);
                    result += std::exp((prev_t - back_t) / decay_length) * prev_val;
                }
            }

            state.previous_sum = result;
            state.previous_time = back_t;
        }

        WindowFunctionHelpers::setValueToOutputColumn<Float64>(transform, function_index, result);
    }

    private:
        const Float64 decay_length;
};

struct WindowFunctionExponentialTimeDecayedMax final : public StatelessWindowFunction
{
    static constexpr size_t ARGUMENT_VALUE = 0;
    static constexpr size_t ARGUMENT_TIME = 1;

    static Float64 getDecayLength(const Array & parameters_, const std::string & name_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one parameter", name_);
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }

    WindowFunctionExponentialTimeDecayedMax(const std::string & name_, const DataTypes & argument_types_, const Array & parameters_)
        : StatelessWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(getDecayLength(parameters_, name_))
    {
        if (argument_types.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly two arguments", name_);
        }

        if (!isNumber(argument_types[ARGUMENT_VALUE]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be a number, '{}' given",
                ARGUMENT_VALUE,
                argument_types[ARGUMENT_VALUE]->getName());
        }

        if (!isNumber(argument_types[ARGUMENT_TIME]) && !isDateTime(argument_types[ARGUMENT_TIME]) && !isDateTime64(argument_types[ARGUMENT_TIME]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be DateTime, DateTime64 or a number, '{}' given",
                ARGUMENT_TIME,
                argument_types[ARGUMENT_TIME]->getName());
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        Float64 result = std::numeric_limits<Float64>::quiet_NaN();

        if (transform->frame_start < transform->frame_end)
        {
            result = std::numeric_limits<Float64>::lowest();
            RowNumber frame_back = transform->prevRowNumber(transform->frame_end);
            Float64 back_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, frame_back);

            for (RowNumber i = transform->frame_start; i < transform->frame_end; transform->advanceRowNumber(i))
            {
                Float64 value = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_VALUE, i);
                Float64 t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);

                /// Avoiding extra calls to `exp` and multiplications.
                if (value > result || t > back_t || result < 0)
                {
                    result = std::max(std::exp((t - back_t) / decay_length) * value, result);
                }
            }
        }

        WindowFunctionHelpers::setValueToOutputColumn<Float64>(transform, function_index, result);
    }

    private:
        const Float64 decay_length;
};

struct WindowFunctionExponentialTimeDecayedCount final : public StatefulWindowFunction<ExponentialTimeDecayedSumState>
{
    static constexpr size_t ARGUMENT_TIME = 0;

    static Float64 getDecayLength(const Array & parameters_, const std::string & name_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one parameter", name_);
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }

    WindowFunctionExponentialTimeDecayedCount(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(getDecayLength(parameters_, name_))
    {
        if (argument_types.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one argument", name_);
        }

        if (!isNumber(argument_types[ARGUMENT_TIME]) && !isDateTime(argument_types[ARGUMENT_TIME]) && !isDateTime64(argument_types[ARGUMENT_TIME]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be DateTime, DateTime64 or a number, '{}' given",
                ARGUMENT_TIME,
                argument_types[ARGUMENT_TIME]->getName());
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & workspace = transform->workspaces[function_index];
        auto & state = getState(workspace);

        Float64 result = 0;

        if (transform->frame_start < transform->frame_end)
        {
            RowNumber frame_back = transform->prevRowNumber(transform->frame_end);
            Float64 back_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, frame_back);

            if (transform->prev_frame_start <= transform->frame_start
                && transform->frame_start < transform->prev_frame_end
                && transform->prev_frame_end <= transform->frame_end)
            {
                for (RowNumber i = transform->prev_frame_start; i < transform->frame_start; transform->advanceRowNumber(i))
                {
                    Float64 prev_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);
                    result -= std::exp((prev_t - back_t) / decay_length);
                }
                result += std::exp((state.previous_time - back_t) / decay_length) * state.previous_sum;
                for (RowNumber i = transform->prev_frame_end; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);
                    result += std::exp((prev_t - back_t) / decay_length);
                }
            }
            else
            {
                for (RowNumber i = transform->frame_start; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);
                    result += std::exp((prev_t - back_t) / decay_length);
                }
            }

            state.previous_sum = result;
            state.previous_time = back_t;
        }

        WindowFunctionHelpers::setValueToOutputColumn<Float64>(transform, function_index, result);
    }

    private:
        const Float64 decay_length;
};

struct WindowFunctionExponentialTimeDecayedAvg final : public StatefulWindowFunction<ExponentialTimeDecayedAvgState>
{
    static constexpr size_t ARGUMENT_VALUE = 0;
    static constexpr size_t ARGUMENT_TIME = 1;

    static Float64 getDecayLength(const Array & parameters_, const std::string & name_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one parameter", name_);
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }

    WindowFunctionExponentialTimeDecayedAvg(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(getDecayLength(parameters_, name_))
    {
        if (argument_types.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly two arguments", name_);
        }

        if (!isNumber(argument_types[ARGUMENT_VALUE]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be a number, '{}' given",
                ARGUMENT_VALUE,
                argument_types[ARGUMENT_VALUE]->getName());
        }

        if (!isNumber(argument_types[ARGUMENT_TIME]) && !isDateTime(argument_types[ARGUMENT_TIME]) && !isDateTime64(argument_types[ARGUMENT_TIME]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be DateTime, DateTime64 or a number, '{}' given",
                ARGUMENT_TIME,
                argument_types[ARGUMENT_TIME]->getName());
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & workspace = transform->workspaces[function_index];
        auto & state = getState(workspace);

        Float64 count = 0;
        Float64 sum = 0;
        Float64 result = std::numeric_limits<Float64>::quiet_NaN();

        if (transform->frame_start < transform->frame_end)
        {
            RowNumber frame_back = transform->prevRowNumber(transform->frame_end);
            Float64 back_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, frame_back);

            if (transform->prev_frame_start <= transform->frame_start
                && transform->frame_start < transform->prev_frame_end
                && transform->prev_frame_end <= transform->frame_end)
            {
                for (RowNumber i = transform->prev_frame_start; i < transform->frame_start; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);
                    Float64 decay = std::exp((prev_t - back_t) / decay_length);
                    sum -= decay * prev_val;
                    count -= decay;
                }

                {
                    Float64 decay = std::exp((state.previous_time - back_t) / decay_length);
                    sum += decay * state.previous_sum;
                    count += decay * state.previous_count;
                }

                for (RowNumber i = transform->prev_frame_end; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);
                    Float64 decay = std::exp((prev_t - back_t) / decay_length);
                    sum += decay * prev_val;
                    count += decay;
                }
            }
            else
            {
                for (RowNumber i = transform->frame_start; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIME, i);
                    Float64 decay = std::exp((prev_t - back_t) / decay_length);
                    sum += decay * prev_val;
                    count += decay;
                }
            }

            state.previous_sum = sum;
            state.previous_count = count;
            state.previous_time = back_t;

            result = sum/count;
        }

        WindowFunctionHelpers::setValueToOutputColumn<Float64>(transform, function_index, result);
    }

    private:
        const Float64 decay_length;
};

struct WindowFunctionRowNumber final : public StatelessWindowFunction
{
    WindowFunctionRowNumber(const std::string & name_, const DataTypes & argument_types_, const Array & parameters_)
        : StatelessWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeUInt64>())
    {}

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        IColumn & to = *transform->blockAt(transform->current_row)
            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
            transform->current_row_number);
    }
};

namespace
{
    struct NtileState
    {
        UInt64 buckets = 0;
        RowNumber start_row;
        UInt64 current_partition_rows = 0;
        UInt64 current_partition_inserted_row = 0;

        void windowInsertResultInto(
            const WindowTransform * transform,
            size_t function_index,
            const DataTypes & argument_types);
    };
}

// Usage: ntile(n). n is the number of buckets.
struct WindowFunctionNtile final : public StatefulWindowFunction<NtileState>
{
    WindowFunctionNtile(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction<NtileState>(name_, argument_types_, parameters_, std::make_shared<DataTypeUInt64>())
    {
        if (argument_types.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} takes exactly one argument", name_);

        auto type_id = argument_types[0]->getTypeId();
        if (type_id != TypeIndex::UInt8 && type_id != TypeIndex::UInt16 && type_id != TypeIndex::UInt32 && type_id != TypeIndex::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' argument type must be an unsigned integer (not larger than 64-bit), got {}", name_, argument_types[0]->getName());
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool checkWindowFrameType(const WindowTransform * transform) const override
    {
        if (transform->order_by_indices.empty())
        {
            LOG_ERROR(getLogger("WindowFunctionNtile"), "Window frame for 'ntile' function must have ORDER BY clause");
            return false;
        }

        // We must wait all for the partition end and get the total rows number in this
        // partition. So before the end of this partition, there is no any block could be
        // dropped out.
        bool is_frame_supported = transform->window_description.frame.begin_type == WindowFrame::BoundaryType::Unbounded
            && transform->window_description.frame.end_type == WindowFrame::BoundaryType::Unbounded;
        if (!is_frame_supported)
        {
            LOG_ERROR(
                getLogger("WindowFunctionNtile"),
                "Window frame for function 'ntile' should be 'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING'");
            return false;
        }
        return true;
    }

    std::optional<WindowFrame> getDefaultFrame() const override
    {
        WindowFrame frame;
        frame.type = WindowFrame::FrameType::ROWS;
        frame.end_type = WindowFrame::BoundaryType::Unbounded;
        return frame;
    }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & workspace = transform->workspaces[function_index];
        auto & state = getState(workspace);
        state.windowInsertResultInto(transform, function_index, argument_types);
    }
};

namespace
{
    void NtileState::windowInsertResultInto(
        const WindowTransform * transform,
        size_t function_index,
        const DataTypes & argument_types)
    {
        if (!buckets) [[unlikely]]
        {
            const auto & current_block = transform->blockAt(transform->current_row);
            const auto & workspace = transform->workspaces[function_index];
            const auto & arg_col = *current_block.original_input_columns[workspace.argument_column_indices[0]];
            if (!isColumnConst(arg_col))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of 'ntile' function must be a constant");
            auto type_id = argument_types[0]->getTypeId();
            if (type_id == TypeIndex::UInt8)
                buckets = arg_col[transform->current_row.row].safeGet<UInt8>();
            else if (type_id == TypeIndex::UInt16)
                buckets = arg_col[transform->current_row.row].safeGet<UInt16>();
            else if (type_id == TypeIndex::UInt32)
                buckets = arg_col[transform->current_row.row].safeGet<UInt32>();
            else if (type_id == TypeIndex::UInt64)
                buckets = arg_col[transform->current_row.row].safeGet<UInt64>();

            if (!buckets)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of 'ntile' function must be greater than zero");
            }
        }
        // new partition
        if (WindowFunctionHelpers::checkPartitionEnterFirstRow(transform)) [[unlikely]]
        {
            current_partition_rows = 0;
            current_partition_inserted_row = 0;
            start_row = transform->current_row;
        }
        current_partition_rows++;

        // Only do the action when we meet the last row in this partition.
        if (!WindowFunctionHelpers::checkPartitionEnterLastRow(transform))
            return;

        auto bucket_capacity = current_partition_rows / buckets;
        auto capacity_diff = current_partition_rows - bucket_capacity * buckets;

        // bucket number starts from 1.
        UInt64 bucket_num = 1;
        while (current_partition_inserted_row < current_partition_rows)
        {
            auto current_bucket_capacity = bucket_capacity;
            if (capacity_diff > 0)
            {
                current_bucket_capacity += 1;
                capacity_diff--;
            }
            auto left_rows = current_bucket_capacity;
            while (left_rows)
            {
                auto available_block_rows = transform->blockRowsNumber(start_row) - start_row.row;
                IColumn & to = *transform->blockAt(start_row).output_columns[function_index];
                auto & pod_array = assert_cast<ColumnUInt64 &>(to).getData();
                if (left_rows < available_block_rows)
                {
                    pod_array.resize_fill(pod_array.size() + left_rows, bucket_num);
                    start_row.row += left_rows;
                    left_rows = 0;
                }
                else
                {
                    pod_array.resize_fill(pod_array.size() + available_block_rows, bucket_num);
                    left_rows -= available_block_rows;
                    start_row.block++;
                    start_row.row = 0;
                }
            }
            current_partition_inserted_row += current_bucket_capacity;
            bucket_num += 1;
        }
    }
}

namespace
{
struct PercentRankState
{
    RowNumber start_row;
    UInt64 current_partition_rows = 0;
};
}

struct WindowFunctionPercentRank final : public StatefulWindowFunction<PercentRankState>
{
public:
    WindowFunctionPercentRank(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
    {}

    bool allocatesMemoryInArena() const override { return false; }

    bool checkWindowFrameType(const WindowTransform * transform) const override
    {
        auto default_window_frame = getDefaultFrame();
        if (transform->window_description.frame != default_window_frame)
        {
            LOG_ERROR(
                getLogger("WindowFunctionPercentRank"),
                "Window frame for function 'percent_rank' should be '{}'", default_window_frame->toString());
            return false;
        }
        return true;
    }

    std::optional<WindowFrame> getDefaultFrame() const override
    {
        WindowFrame frame;
        frame.type = WindowFrame::FrameType::RANGE;
        frame.begin_type = WindowFrame::BoundaryType::Unbounded;
        frame.end_type = WindowFrame::BoundaryType::Unbounded;
        return frame;
    }

    void windowInsertResultInto(const WindowTransform * transform, size_t function_index) const override
    {
        auto & state = getWorkspaceState(transform, function_index);
        if (WindowFunctionHelpers::checkPartitionEnterFirstRow(transform))
        {
            state.current_partition_rows = 0;
            state.start_row = transform->current_row;
        }

        insertRankIntoColumn(transform, function_index);
        state.current_partition_rows++;

        if (!WindowFunctionHelpers::checkPartitionEnterLastRow(transform))
        {
            return;
        }

        UInt64 remaining_rows = state.current_partition_rows;
        Float64 percent_rank_denominator = remaining_rows == 1 ? 1 : remaining_rows - 1;

        while (remaining_rows > 0)
        {
            auto block_rows_number = transform->blockRowsNumber(state.start_row);
            auto available_block_rows = block_rows_number - state.start_row.row;
            if (available_block_rows <= remaining_rows)
            {
                /// This partition involves multiple blocks. Finish current block and move on to the
                /// next block.
                auto & to_column = *transform->blockAt(state.start_row).output_columns[function_index];
                auto & data = assert_cast<ColumnFloat64 &>(to_column).getData();
                for (size_t i = state.start_row.row; i < block_rows_number; ++i)
                    data[i] = (data[i] - 1) / percent_rank_denominator;

                state.start_row.block++;
                state.start_row.row = 0;
                remaining_rows -= available_block_rows;
            }
            else
            {
                /// The partition ends in current block.s
                auto & to_column = *transform->blockAt(state.start_row).output_columns[function_index];
                auto & data = assert_cast<ColumnFloat64 &>(to_column).getData();
                for (size_t i = state.start_row.row, n = state.start_row.row + remaining_rows; i < n; ++i)
                {
                    data[i] = (data[i] - 1) / percent_rank_denominator;
                }
                state.start_row.row += remaining_rows;
                remaining_rows = 0;
            }
        }
    }


    inline PercentRankState & getWorkspaceState(const WindowTransform * transform, size_t function_index) const
    {
        const auto & workspace = transform->workspaces[function_index];
        return getState(workspace);
    }

    inline void insertRankIntoColumn(const WindowTransform * transform, size_t function_index) const
    {
        auto & to_column = *transform->blockAt(transform->current_row).output_columns[function_index];
        assert_cast<ColumnFloat64 &>(to_column).getData().push_back(static_cast<Float64>(transform->peer_group_start_row_number));
    }
};

// ClickHouse-specific variant of lag/lead that respects the window frame.
template <bool is_lead>
struct WindowFunctionLagLeadInFrame final : public StatelessWindowFunction
{
    FunctionBasePtr func_cast = nullptr;

    WindowFunctionLagLeadInFrame(const std::string & name_, const DataTypes & argument_types_, const Array & parameters_)
        : StatelessWindowFunction(name_, argument_types_, parameters_, createResultType(argument_types_, name_))
    {
        if (!parameters.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} cannot be parameterized", name_);
        }

        if (argument_types.size() == 1)
        {
            return;
        }

        if (!isInt64OrUInt64FieldType(argument_types[1]->getDefault().getType()))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Offset must be an integer, '{}' given",
                argument_types[1]->getName());
        }

        if (argument_types.size() == 2)
        {
            return;
        }

        if (argument_types.size() > 3)
        {
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Function '{}' accepts at most 3 arguments, {} given",
                name, argument_types.size());
        }

        if (argument_types[0]->equals(*argument_types[2]))
            return;

        const auto supertype = tryGetLeastSupertype(DataTypes{argument_types[0], argument_types[2]});
        if (!supertype)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "There is no supertype for the argument type '{}' and the default value type '{}'",
                argument_types[0]->getName(),
                argument_types[2]->getName());
        }
        if (!argument_types[0]->equals(*supertype))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The supertype '{}' for the argument type '{}' and the default value type '{}' is not the same as the argument type",
                supertype->getName(),
                argument_types[0]->getName(),
                argument_types[2]->getName());
        }

        auto get_cast_func = [from = argument_types[2], to = argument_types[0]]
        {
            return createInternalCast({from, {}}, to, CastType::accurate, {});
        };

        func_cast = get_cast_func();

    }

    ColumnPtr castColumn(const Columns & columns, const std::vector<size_t> & idx) override
    {
        if (!func_cast)
            return nullptr;

        ColumnsWithTypeAndName arguments
        {
            { columns[idx[2]], argument_types[2], "" },
            {
                DataTypeString().createColumnConst(columns[idx[2]]->size(), argument_types[0]->getName()),
                std::make_shared<DataTypeString>(),
                ""
            }
        };

        return func_cast->execute(arguments, argument_types[0], columns[idx[2]]->size());
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_, const std::string & name_)
    {
        if (argument_types_.empty())
        {
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Function {} takes at least one argument", name_);
        }

        return argument_types_[0];
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & current_block = transform->blockAt(transform->current_row);
        IColumn & to = *current_block.output_columns[function_index];
        const auto & workspace = transform->workspaces[function_index];

        Int64 offset = 1;
        if (argument_types.size() > 1)
        {
            offset = (*current_block.input_columns[
                    workspace.argument_column_indices[1]])[
                        transform->current_row.row].safeGet<Int64>();

            /// Either overflow or really negative value, both is not acceptable.
            if (offset < 0)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "The offset for function {} must be in (0, {}], {} given",
                    getName(), INT64_MAX, offset);
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
                const IColumn & default_column =
                    current_block.casted_columns[function_index] ?
                        *current_block.casted_columns[function_index].get() :
                        *current_block.input_columns[workspace.argument_column_indices[2]].get();

                to.insert(default_column[transform->current_row.row]);
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

struct WindowFunctionNthValue final : public StatelessWindowFunction
{
    WindowFunctionNthValue(const std::string & name_, const DataTypes & argument_types_, const Array & parameters_)
        : StatelessWindowFunction(name_, argument_types_, parameters_, createResultType(name_, argument_types_))
    {
        if (!parameters.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} cannot be parameterized", name_);
        }

        if (!isInt64OrUInt64FieldType(argument_types[1]->getDefault().getType()))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Offset must be an integer, '{}' given",
                argument_types[1]->getName());
        }
    }

    static DataTypePtr createResultType(const std::string & name_, const DataTypes & argument_types_)
    {
        if (argument_types_.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly two arguments", name_);
        }

        return argument_types_[0];
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & current_block = transform->blockAt(transform->current_row);
        IColumn & to = *current_block.output_columns[function_index];
        const auto & workspace = transform->workspaces[function_index];

        Int64 offset = (*current_block.input_columns[
                workspace.argument_column_indices[1]])[
            transform->current_row.row].safeGet<Int64>();

        /// Either overflow or really negative value, both is not acceptable.
        if (offset <= 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The offset for function {} must be in (0, {}], {} given",
                getName(), INT64_MAX, offset);
        }

        --offset;
        const auto [target_row, offset_left] = transform->moveRowNumber(transform->frame_start, offset);
        if (offset_left != 0
            || target_row < transform->frame_start
            || transform->frame_end <= target_row)
        {
            // Offset is outside the frame.
            to.insertDefault();
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

struct NonNegativeDerivativeState
{
    Float64 previous_metric = 0;
    Float64 previous_timestamp = 0;
};

struct NonNegativeDerivativeParams
{
    static constexpr size_t ARGUMENT_METRIC = 0;
    static constexpr size_t ARGUMENT_TIMESTAMP = 1;
    static constexpr size_t ARGUMENT_INTERVAL = 2;

    Float64 interval_length = 1;
    bool interval_specified = false;
    Int64 ts_scale_multiplier = 0;

    NonNegativeDerivativeParams(
        const std::string & name_, const DataTypes & argument_types, const Array & parameters)
    {
        if (!parameters.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Function {} cannot be parameterized", name_);
        }

        if (argument_types.size() != 2 && argument_types.size() != 3)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} takes 2 or 3 arguments", name_);
        }

        if (!isNumber(argument_types[ARGUMENT_METRIC]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Argument {} must be a number, '{}' given",
                            ARGUMENT_METRIC,
                            argument_types[ARGUMENT_METRIC]->getName());
        }

        if (!isDateTime(argument_types[ARGUMENT_TIMESTAMP]) && !isDateTime64(argument_types[ARGUMENT_TIMESTAMP]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Argument {} must be DateTime or DateTime64, '{}' given",
                            ARGUMENT_TIMESTAMP,
                            argument_types[ARGUMENT_TIMESTAMP]->getName());
        }

        if (isDateTime64(argument_types[ARGUMENT_TIMESTAMP]))
        {
            const auto & datetime64_type = assert_cast<const DataTypeDateTime64 &>(*argument_types[ARGUMENT_TIMESTAMP]);
            ts_scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(datetime64_type.getScale());
        }

        if (argument_types.size() == 3)
        {
            const DataTypeInterval * interval_datatype = checkAndGetDataType<DataTypeInterval>(argument_types[ARGUMENT_INTERVAL].get());
            if (!interval_datatype)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Argument {} must be an INTERVAL, '{}' given",
                    ARGUMENT_INTERVAL,
                    argument_types[ARGUMENT_INTERVAL]->getName());
            }
            if (!interval_datatype->getKind().isFixedLength())
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The INTERVAL must be a week or shorter, '{}' given",
                    argument_types[ARGUMENT_INTERVAL]->getName());
            }
            interval_length = interval_datatype->getKind().toSeconds();
            interval_specified = true;
        }
    }
};

// nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL 1 SECOND])
struct WindowFunctionNonNegativeDerivative final : public StatefulWindowFunction<NonNegativeDerivativeState>, public NonNegativeDerivativeParams
{
    using Params = NonNegativeDerivativeParams;

    WindowFunctionNonNegativeDerivative(const std::string & name_,
                                            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , NonNegativeDerivativeParams(name, argument_types, parameters)
    {}

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
                                size_t function_index) const override
    {
        const auto & current_block = transform->blockAt(transform->current_row);
        const auto & workspace = transform->workspaces[function_index];
        auto & state = getState(workspace);

        auto interval_duration = interval_specified ? interval_length *
            (*current_block.input_columns[workspace.argument_column_indices[ARGUMENT_INTERVAL]]).getFloat64(0) : 1;

        Float64 curr_metric = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_METRIC, transform->current_row);
        Float64 metric_diff = curr_metric - state.previous_metric;
        Float64 result;

        if (ts_scale_multiplier)
        {
            const auto & column = transform->blockAt(transform->current_row.block).input_columns[workspace.argument_column_indices[ARGUMENT_TIMESTAMP]];
            const auto & curr_timestamp = checkAndGetColumn<DataTypeDateTime64::ColumnType>(*column).getInt(transform->current_row.row);

            Float64 time_elapsed = curr_timestamp - state.previous_timestamp;
            result = (time_elapsed > 0) ? (metric_diff * ts_scale_multiplier / time_elapsed  * interval_duration) : 0;
            state.previous_timestamp = curr_timestamp;
        }
        else
        {
            Float64 curr_timestamp = WindowFunctionHelpers::getValue<Float64>(transform, function_index, ARGUMENT_TIMESTAMP, transform->current_row);
            Float64 time_elapsed = curr_timestamp - state.previous_timestamp;
            result = (time_elapsed > 0) ? (metric_diff / time_elapsed * interval_duration) : 0;
            state.previous_timestamp = curr_timestamp;
        }
        state.previous_metric = curr_metric;

        if (unlikely(!transform->current_row.row))
            result = 0;

        WindowFunctionHelpers::setValueToOutputColumn<Float64>(transform, function_index, result >= 0 ? result : 0);
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

    const AggregateFunctionProperties properties = {
        // By default, if an aggregate function has a null argument, it will be
        // replaced with AggregateFunctionNothing. We don't need this behavior
        // e.g. for lagInFrame(number, 1, null).
        .returns_default_when_only_null = true,
        // This probably doesn't make any difference for window functions because
        // it is an Aggregator-specific setting.
        .is_order_dependent = true,
        .is_window_function = true};

    factory.registerFunction("rank", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionRank>(name, argument_types,
                parameters);
        }, properties}, AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction("denseRank", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionDenseRank>(name, argument_types,
                parameters);
        }, properties});

    factory.registerAlias("dense_rank", "denseRank", AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction("percentRank", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionPercentRank>(name, argument_types,
                parameters);
        }, properties});

    factory.registerAlias("percent_rank", "percentRank", AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction("row_number", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionRowNumber>(name, argument_types,
                parameters);
        }, properties}, AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction("ntile", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionNtile>(name, argument_types,
                parameters);
        }, properties}, AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction("nth_value", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionNthValue>(
                name, argument_types, parameters);
        }, properties}, AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction("lagInFrame", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionLagLeadInFrame<false>>(
                name, argument_types, parameters);
        }, properties});

    factory.registerFunction("leadInFrame", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionLagLeadInFrame<true>>(
                name, argument_types, parameters);
        }, properties});

    factory.registerFunction("exponentialTimeDecayedSum", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionExponentialTimeDecayedSum>(
                name, argument_types, parameters);
        }, properties});

    factory.registerFunction("exponentialTimeDecayedMax", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionExponentialTimeDecayedMax>(
                name, argument_types, parameters);
        }, properties});

    factory.registerFunction("exponentialTimeDecayedCount", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionExponentialTimeDecayedCount>(
                name, argument_types, parameters);
        }, properties});

    factory.registerFunction("exponentialTimeDecayedAvg", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionExponentialTimeDecayedAvg>(
                name, argument_types, parameters);
        }, properties});

    factory.registerFunction("nonNegativeDerivative", {[](const std::string & name,
           const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionNonNegativeDerivative>(
                name, argument_types, parameters);
        }, properties});
}
}
