#include <Processors/Transforms/Window/WindowTransformParams.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/IDataType.h>

#include <Interpreters/convertFieldToType.h>

#include <WindowFunctions/IGeneralWindowFunction.h>

#include <Common/Exception.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <base/arithmeticOverflow.h>
#include <base/defines.h>

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

template <typename T>
int compareRangeOffset(
    const IColumn * lhs_column, size_t lhs_row,
    const IColumn * rhs_column, size_t rhs_row,
    const Field & offset, bool offset_is_preceding)
{
    const T lhs_value = assert_cast<const ColumnVector<T> &>(*lhs_column).getData()[lhs_row];
    const T rhs_value = assert_cast<const ColumnVector<T> &>(*rhs_column).getData()[rhs_row];
    const T offset_value = static_cast<T>(offset.safeGet<T>());

    if constexpr (std::is_floating_point_v<T>)
    {
        const T shifted_rhs = offset_is_preceding ? rhs_value - offset_value : rhs_value + offset_value;
        return lhs_value < shifted_rhs ? -1 : (lhs_value == shifted_rhs ? 0 : 1);
    }
    else
    {
        T shifted_rhs{};
        const bool overflow = offset_is_preceding ? common::subOverflow(rhs_value, offset_value, shifted_rhs) : common::addOverflow(rhs_value, offset_value, shifted_rhs);
        if (overflow)
            return offset_is_preceding ? 1 : -1;

        return lhs_value < shifted_rhs ? -1 : (lhs_value == shifted_rhs ? 0 : 1);
    }
}

/// The comparator specialised for the value type of `column`, which is the nested column for a Nullable key.
RangeOffsetComparator chooseRangeOffsetComparatorForValueType(const IColumn & column)
{
    switch (column.getDataType())
    {
        case TypeIndex::UInt8: return compareRangeOffset<UInt8>;
        case TypeIndex::UInt16: return compareRangeOffset<UInt16>;
        case TypeIndex::UInt32: return compareRangeOffset<UInt32>;
        case TypeIndex::UInt64: return compareRangeOffset<UInt64>;
        case TypeIndex::Int8: return compareRangeOffset<Int8>;
        case TypeIndex::Int16: return compareRangeOffset<Int16>;
        case TypeIndex::Int32: return compareRangeOffset<Int32>;
        case TypeIndex::Int64: return compareRangeOffset<Int64>;
        case TypeIndex::Int128: return compareRangeOffset<Int128>;
        case TypeIndex::Float32: return compareRangeOffset<Float32>;
        case TypeIndex::Float64: return compareRangeOffset<Float64>;
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
RangeOffsetComparator chooseRangeOffsetComparator(const Block & header, const WindowFrame & frame, const VectorWithMemoryTracking<size_t> & order_by_indices)
{
    if (!isRangeOffsetFrame(frame))
        return nullptr;

    chassert(order_by_indices.size() == 1);
    const IColumn & column = *header.getByPosition(order_by_indices[0]).column;
    const auto * nullable = typeid_cast<const ColumnNullable *>(&column);
    const IColumn & values = nullable ? nullable->getNestedColumn() : column;
    return chooseRangeOffsetComparatorForValueType(values);
}

Block materializeHeader(Block header)
{
    const auto materialized_columns = header.getColumns()
        | std::views::transform([](const ColumnPtr & column) { return column->convertToFullColumnIfConst(); })
        | std::ranges::to<Columns>();

    header.setColumns(materialized_columns);
    return header;
}

VectorWithMemoryTracking<size_t> findPositions(const Block & header, const SortDescription & description)
{
    return description
        | std::views::transform([&](const SortColumnDescription & column) { return header.getPositionByName(column.column_name); })
        | std::ranges::to<VectorWithMemoryTracking<size_t>>();
}

VectorWithMemoryTracking<bool> markColumnsToMaterialize(
    const Block & header,
    const SortDescription & partition_by,
    const SortDescription & order_by,
    const VectorWithMemoryTracking<WindowFunctionDescription> & functions)
{
    VectorWithMemoryTracking<bool> should_materialize(header.columns(), false);

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
    const VectorWithMemoryTracking<size_t> & order_by_indices)
{
    WindowDescription prepared = description;
    if (isRangeOffsetFrame(prepared.frame))
        prepared.frame = prepareRangeOffsets(prepared.frame, header.getByPosition(order_by_indices[0]).type);

    return prepared;
}

WindowFunctionRequirements collectRequirements(const WindowDescription & description, const VectorWithMemoryTracking<WindowFunctionDescription> & functions)
{
    WindowFunctionRequirements requirements;
    for (const auto & function : functions)
    {
        if (auto general = asGeneralWindowFunction(function.aggregate_function))
            requirements = unionOf(requirements, general->getRequirements(description));
        else
            requirements = unionOf(requirements, WindowFunctionRequirements{.frame_start = true, .frame_end = true});
    }

    return requirements;
}

bool needsPeerGroupIndexes(const WindowDescription & description, const WindowFunctionRequirements & requirements)
{
    const bool functions_read_peer_groups = requirements.peers_start || requirements.peers_end;
    const bool frame_reads_peer_groups = (requirements.frame_start || requirements.frame_end) && description.frame.type != WindowFrame::FrameType::ROWS;
    return functions_read_peer_groups || frame_reads_peer_groups;
}

bool needsMissingKeys(const Block & header, const VectorWithMemoryTracking<size_t> & order_by_indices, const WindowDescription & description, const WindowFunctionRequirements & requirements)
{
    const auto range_column_type = isRangeOffsetFrame(description.frame) ? header.getByPosition(order_by_indices[0]).type : nullptr;
    const bool functions_read_frame = requirements.frame_start || requirements.frame_end;
    const bool range_key_can_be_missing = range_column_type && (range_column_type->isNullable() || isFloat(range_column_type));
    return functions_read_frame && range_key_can_be_missing;
}

}

WindowTransformParams WindowTransformParams::create(
    const Block & input_header,
    const WindowDescription & window_description,
    const VectorWithMemoryTracking<WindowFunctionDescription> & functions)
{
    auto header = materializeHeader(input_header);
    auto partition_by_indices = findPositions(header, window_description.partition_by);
    auto order_by_indices = findPositions(header, window_description.order_by);
    auto should_materialize = markColumnsToMaterialize(header, window_description.partition_by, window_description.order_by, functions);
    auto range_offset_comparator = chooseRangeOffsetComparator(header, window_description.frame, order_by_indices);
    auto description = prepareDescriptionForExecution(header, window_description, order_by_indices);
    auto requirements = collectRequirements(description, functions);
    auto needs_peer_group_indexes = needsPeerGroupIndexes(description, requirements);
    auto needs_missing_keys = needsMissingKeys(header, order_by_indices, description, requirements);

    return WindowTransformParams{
        std::move(header),
        std::move(description),
        std::move(partition_by_indices),
        std::move(order_by_indices),
        std::move(should_materialize),
        std::move(range_offset_comparator),
        std::move(requirements),
        std::move(needs_peer_group_indexes),
        std::move(needs_missing_keys),
    };
}

}
