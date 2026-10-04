#include <Processors/Transforms/Window/WindowTransformParams.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>

#include <Interpreters/convertFieldToType.h>

#include <WindowFunctions/IWindowFunction.h>

#include <Common/Exception.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <base/arithmeticOverflow.h>
#include <base/defines.h>

#include <optional>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// The value moved by the offset, empty when it leaves the type's range.
template <typename T>
std::optional<T> shift(T value, T offset, bool preceding)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        return preceding ? value - offset : value + offset;
    }
    else
    {
        T result{};
        const bool overflow = preceding ? common::subOverflow(value, offset, result) : common::addOverflow(value, offset, result);
        return overflow ? std::nullopt : std::optional(result);
    }
}

template <typename T>
int compareRangeOffset(
    const IColumn * lhs_column, size_t lhs_row,
    const IColumn * rhs_column, size_t rhs_row,
    const Field & offset, bool offset_is_preceding)
{
    const T lhs_value = assert_cast<const ColumnVector<T> &>(*lhs_column).getData()[lhs_row];
    const T rhs_value = assert_cast<const ColumnVector<T> &>(*rhs_column).getData()[rhs_row];
    const std::optional<T> shifted_rhs = shift(rhs_value, static_cast<T>(offset.safeGet<T>()), offset_is_preceding);

    if (!shifted_rhs)
        return offset_is_preceding ? 1 : -1;
    else if (lhs_value < *shifted_rhs)
        return -1;
    else if (lhs_value == *shifted_rhs)
        return 0;
    else
        return 1;
}

// The same over a Nullable column: NULL sorts before every value and equals NULL.
template <typename T>
int compareNullableRangeOffset(
    const IColumn * lhs_column, size_t lhs_row,
    const IColumn * rhs_column, size_t rhs_row,
    const Field & offset, bool offset_is_preceding)
{
    const auto & lhs = assert_cast<const ColumnNullable &>(*lhs_column);
    const auto & rhs = assert_cast<const ColumnNullable &>(*rhs_column);

    const bool lhs_is_null = lhs.isNullAt(lhs_row);
    const bool rhs_is_null = rhs.isNullAt(rhs_row);
    if (lhs_is_null || rhs_is_null)
        return lhs_is_null == rhs_is_null ? 0 : (lhs_is_null ? -1 : 1);

    return compareRangeOffset<T>(&lhs.getNestedColumn(), lhs_row, &rhs.getNestedColumn(), rhs_row, offset, offset_is_preceding);
}

/// The comparator specialised for the value type of `column`, which is the nested column for a Nullable key.
template <bool nullable>
RangeOffsetComparator chooseRangeOffsetComparatorForValueType(const IColumn & column)
{
    switch (column.getDataType())
    {
        case TypeIndex::UInt8: return nullable ? compareNullableRangeOffset<UInt8> : compareRangeOffset<UInt8>;
        case TypeIndex::UInt16: return nullable ? compareNullableRangeOffset<UInt16> : compareRangeOffset<UInt16>;
        case TypeIndex::UInt32: return nullable ? compareNullableRangeOffset<UInt32> : compareRangeOffset<UInt32>;
        case TypeIndex::UInt64: return nullable ? compareNullableRangeOffset<UInt64> : compareRangeOffset<UInt64>;
        case TypeIndex::Int8: return nullable ? compareNullableRangeOffset<Int8> : compareRangeOffset<Int8>;
        case TypeIndex::Int16: return nullable ? compareNullableRangeOffset<Int16> : compareRangeOffset<Int16>;
        case TypeIndex::Int32: return nullable ? compareNullableRangeOffset<Int32> : compareRangeOffset<Int32>;
        case TypeIndex::Int64: return nullable ? compareNullableRangeOffset<Int64> : compareRangeOffset<Int64>;
        case TypeIndex::Int128: return nullable ? compareNullableRangeOffset<Int128> : compareRangeOffset<Int128>;
        case TypeIndex::Float32: return nullable ? compareNullableRangeOffset<Float32> : compareRangeOffset<Float32>;
        case TypeIndex::Float64: return nullable ? compareNullableRangeOffset<Float64> : compareRangeOffset<Float64>;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The RANGE OFFSET frame for '{}' ORDER BY column is not implemented", column.getName());
    }
}

bool isRangeOffsetFrame(const WindowFrame & frame)
{
    const bool has_offset = frame.begin_type == WindowFrame::BoundaryType::Offset || frame.end_type == WindowFrame::BoundaryType::Offset;
    return frame.type == WindowFrame::FrameType::RANGE && has_offset;
}

/// Null for every frame but RANGE OFFSET, which has exactly one ORDER BY key.
RangeOffsetComparator chooseRangeOffsetComparator(const Block & header, const WindowFrame & frame, const std::vector<size_t> & order_by_indices)
{
    if (!isRangeOffsetFrame(frame))
        return nullptr;

    chassert(order_by_indices.size() == 1);
    const IColumn & column = *header.getByPosition(order_by_indices[0]).column;
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
        return chooseRangeOffsetComparatorForValueType<true>(nullable->getNestedColumn());
    else
        return chooseRangeOffsetComparatorForValueType<false>(column);
}

Block materializeHeader(Block header)
{
    const auto materialized_columns = header.getColumns()
        | std::views::transform([](const ColumnPtr & column) { return column->convertToFullColumnIfConst(); })
        | std::ranges::to<Columns>();

    header.setColumns(materialized_columns);
    return header;
}

std::vector<size_t> findPositions(const Block & header, const std::vector<SortDescription> & descriptions)
{
    return descriptions
        | std::views::join
        | std::views::transform([&](const SortColumnDescription & column) { return header.getPositionByName(column.column_name); })
        | std::ranges::to<std::vector<size_t>>();
}

std::vector<bool> markColumnsToMaterialize(
    const Block & header,
    const SortDescription & partition_by,
    const SortDescription & order_by,
    const std::vector<WindowFunctionDescription> & functions)
{
    std::vector<bool> should_materialize(header.columns(), false);

    /// Compared across blocks to find the partition end.
    for (const auto & column : partition_by)
        should_materialize[header.getPositionByName(column.column_name)] = true;

    /// Compared across blocks for peer groups, and cast to a concrete ColumnVector by the RANGE comparator.
    for (const auto & column : order_by)
        should_materialize[header.getPositionByName(column.column_name)] = true;

    /// Fed to aggregate functions, which cannot take wrapped columns.
    for (const auto & f : functions)
        for (const auto & argument_name : f.argument_names)
            should_materialize[header.getPositionByName(argument_name)] = true;

    return should_materialize;
}

WindowFrame applyFunctionDefaultFrame(const WindowFrame & frame, const std::vector<WindowFunctionDescription> & functions)
{
    if (!frame.is_default || functions.size() != 1)
        return frame;

    const auto * window_function = dynamic_cast<const IWindowFunction *>(functions[0].aggregate_function.get());
    if (!window_function)
        return frame;

    return window_function->getDefaultFrame().value_or(frame);
}

/// We need convert offsets to order by type to be able to use them in shift function.
WindowFrame prepareRangeOffsets(const WindowFrame & frame, const DataTypePtr & order_by_type)
{
    auto convert = [&](const Field & offset, std::string_view bound_name)
    {
        const Field converted = convertFieldToTypeOrThrow(offset, *order_by_type, nullptr, {}, /*convert_inexact_floats=*/true);
        if (accurateLess(converted, Field(0)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window frame {} offset must be nonnegative, {} given", bound_name, converted);

        return converted;
    };

    WindowFrame converted = frame;

    if (frame.begin_type == WindowFrame::BoundaryType::Offset)
        converted.begin_offset = convert(frame.begin_offset, "start");

    if (frame.end_type == WindowFrame::BoundaryType::Offset)
        converted.end_offset = convert(frame.end_offset, "end");

    return converted;
}

WindowDescription prepareDescriptionForExecution(
    const Block & header,
    const WindowDescription & description,
    const WindowFrame & frame,
    const std::vector<size_t> & order_by_indices)
{
    WindowDescription prepared = description;
    prepared.frame = frame;

    if (isRangeOffsetFrame(prepared.frame))
        prepared.frame = prepareRangeOffsets(prepared.frame, header.getByPosition(order_by_indices[0]).type);

    return prepared;
}

}

WindowTransformParams WindowTransformParams::create(
    const Block & input_header,
    const WindowDescription & window_description,
    const std::vector<WindowFunctionDescription> & functions)
{
    auto header = materializeHeader(input_header);
    auto partition_by_indices = findPositions(header, {window_description.partition_by});
    auto order_by_indices = findPositions(header, {window_description.order_by});
    auto peer_key_indices = findPositions(header, {window_description.partition_by, window_description.order_by});
    auto should_materialize = markColumnsToMaterialize(header, window_description.partition_by, window_description.order_by, functions);
    auto frame = applyFunctionDefaultFrame(window_description.frame, functions);
    auto range_offset_comparator = chooseRangeOffsetComparator(header, frame, order_by_indices);
    auto description = prepareDescriptionForExecution(header, window_description, frame, order_by_indices);

    return WindowTransformParams{
        std::move(header),
        std::move(description),
        std::move(partition_by_indices),
        std::move(order_by_indices),
        std::move(peer_key_indices),
        std::move(should_materialize),
        std::move(range_offset_comparator),
    };
}

bool WindowTransformParams::arePeers(const Columns & lhs, size_t lhs_row, const Columns & rhs, size_t rhs_row) const
{
    if (window_description.frame.type == WindowFrame::FrameType::ROWS)
        return false;

    // For RANGE and GROUPS frames, rows that compare equal on the ORDER BY key are peers; without ORDER BY all rows are.
    for (const size_t key : order_by_indices)
        if (lhs[key]->compareAt(lhs_row, rhs_row, *rhs[key], /*nan_direction_hint=*/1) != 0)
            return false;

    return true;
}

}
