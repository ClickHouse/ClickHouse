#include <Processors/Transforms/Window/WindowTransformParams.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>

#include <Interpreters/convertFieldToType.h>

#include <WindowFunctions/IWindowFunction.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
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
    extern const int LOGICAL_ERROR;
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
    const Field & offset, bool offset_is_preceding,
    const DateLUTImpl * /*time_zone*/)
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

/// The same for a calendar INTERVAL: the offset counts months for a Date or Date32 key and days for a DateTime key.
/// The frame search moves the bounds only forward, so the shifted key must not decrease as the key grows.
/// addMonths keeps this, as it saturates the day of month. addDays on a DateTime breaks it only inside a DST gap
/// or overlap hour, where the bound may lag by up to that hour; this matches subtractDays and is accepted.
template <typename T>
int compareCalendarRangeOffset(
    const IColumn * lhs_column, size_t lhs_row,
    const IColumn * rhs_column, size_t rhs_row,
    const Field & offset, bool offset_is_preceding,
    const DateLUTImpl * time_zone)
{
    chassert(time_zone);

    const T lhs_value = assert_cast<const ColumnVector<T> &>(*lhs_column).getData()[lhs_row];
    const T rhs_value = assert_cast<const ColumnVector<T> &>(*rhs_column).getData()[rhs_row];
    const Int64 delta = static_cast<Int64>(offset.safeGet<UInt64>()) * (offset_is_preceding ? -1 : 1);

    /// Int64, because a shifted DateTime may leave the UInt32 range.
    Int64 shifted_rhs = 0;
    if constexpr (std::is_same_v<T, UInt16>)
        shifted_rhs = time_zone->addMonths(DayNum(rhs_value), delta);
    else if constexpr (std::is_same_v<T, Int32>)
        shifted_rhs = time_zone->addMonths(ExtendedDayNum(rhs_value), delta);
    else
    {
        static_assert(std::is_same_v<T, UInt32>);
        shifted_rhs = time_zone->addDays(rhs_value, delta);
    }

    const Int64 lhs = lhs_value;
    if (lhs < shifted_rhs)
        return -1;
    else if (lhs == shifted_rhs)
        return 0;
    else
        return 1;
}

/// A comparator over a Nullable column: NULL sorts before every value and equals NULL.
template <RangeOffsetComparator compare_nested>
int compareNullableRangeOffset(
    const IColumn * lhs_column, size_t lhs_row,
    const IColumn * rhs_column, size_t rhs_row,
    const Field & offset, bool offset_is_preceding,
    const DateLUTImpl * time_zone)
{
    const auto & lhs = assert_cast<const ColumnNullable &>(*lhs_column);
    const auto & rhs = assert_cast<const ColumnNullable &>(*rhs_column);

    const bool lhs_is_null = lhs.isNullAt(lhs_row);
    const bool rhs_is_null = rhs.isNullAt(rhs_row);
    if (lhs_is_null || rhs_is_null)
        return lhs_is_null == rhs_is_null ? 0 : (lhs_is_null ? -1 : 1);

    return compare_nested(&lhs.getNestedColumn(), lhs_row, &rhs.getNestedColumn(), rhs_row, offset, offset_is_preceding, time_zone);
}

template <bool nullable, RangeOffsetComparator compare>
RangeOffsetComparator maybeNullable()
{
    if constexpr (nullable)
        return compareNullableRangeOffset<compare>;
    else
        return compare;
}

/// The comparator specialised for the value type of `column`, which is the nested column for a Nullable key.
template <bool nullable>
RangeOffsetComparator chooseRangeOffsetComparatorForValueType(const IColumn & column)
{
    switch (column.getDataType())
    {
        case TypeIndex::UInt8: return maybeNullable<nullable, compareRangeOffset<UInt8>>();
        case TypeIndex::UInt16: return maybeNullable<nullable, compareRangeOffset<UInt16>>();
        case TypeIndex::UInt32: return maybeNullable<nullable, compareRangeOffset<UInt32>>();
        case TypeIndex::UInt64: return maybeNullable<nullable, compareRangeOffset<UInt64>>();
        case TypeIndex::Int8: return maybeNullable<nullable, compareRangeOffset<Int8>>();
        case TypeIndex::Int16: return maybeNullable<nullable, compareRangeOffset<Int16>>();
        case TypeIndex::Int32: return maybeNullable<nullable, compareRangeOffset<Int32>>();
        case TypeIndex::Int64: return maybeNullable<nullable, compareRangeOffset<Int64>>();
        case TypeIndex::Int128: return maybeNullable<nullable, compareRangeOffset<Int128>>();
        case TypeIndex::Float32: return maybeNullable<nullable, compareRangeOffset<Float32>>();
        case TypeIndex::Float64: return maybeNullable<nullable, compareRangeOffset<Float64>>();
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The RANGE OFFSET frame for '{}' ORDER BY column is not implemented", column.getName());
    }
}

/// The same for a calendar INTERVAL offset, whose key was already checked to be a Date, Date32 or DateTime.
template <bool nullable>
RangeOffsetComparator chooseCalendarRangeOffsetComparatorForValueType(const IColumn & column)
{
    switch (column.getDataType())
    {
        case TypeIndex::UInt16: return maybeNullable<nullable, compareCalendarRangeOffset<UInt16>>();
        case TypeIndex::Int32: return maybeNullable<nullable, compareCalendarRangeOffset<Int32>>();
        case TypeIndex::UInt32: return maybeNullable<nullable, compareCalendarRangeOffset<UInt32>>();
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Calendar interval offset is not supported for ORDER BY column {}", column.getName());
    }
}

RangeOffsetComparator chooseRangeOffsetComparator(const IColumn & column, bool is_calendar)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        const IColumn & nested = nullable->getNestedColumn();
        return is_calendar ? chooseCalendarRangeOffsetComparatorForValueType<true>(nested) : chooseRangeOffsetComparatorForValueType<true>(nested);
    }

    return is_calendar ? chooseCalendarRangeOffsetComparatorForValueType<false>(column) : chooseRangeOffsetComparatorForValueType<false>(column);
}

bool isRangeOffsetFrame(const WindowFrame & frame)
{
    const bool has_offset = frame.begin_type == WindowFrame::BoundaryType::Offset || frame.end_type == WindowFrame::BoundaryType::Offset;
    return frame.type == WindowFrame::FrameType::RANGE && has_offset;
}

Block materializeHeader(Block header)
{
    const auto materialized_columns = header.getColumns()
        | std::views::transform([](const ColumnPtr & column) { return column->convertToFullColumnIfConst(); })
        | std::ranges::to<Columns>();

    header.setColumns(materialized_columns);
    return header;
}

std::vector<size_t> findPositions(const Block & header, const SortDescription & columns)
{
    return columns
        | std::views::transform([&](const auto & column) { return header.getPositionByName(column.column_name); })
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

/// Converts an INTERVAL offset into the units of the ORDER BY key: days for Date/Date32, seconds for DateTime.
/// Kinds without a fixed length set `is_calendar` and are converted to months (MONTH/QUARTER/YEAR, Date keys only)
/// or to days (DAY/WEEK on a DateTime key in a time zone with a variable UTC offset), see compareCalendarRangeOffset.
/// In a fixed-offset time zone a day is always 86400 seconds and the plain arithmetic comparator is used.
Field convertIntervalOffset(const Field & offset, IntervalKind kind, const DataTypePtr & key_type, const DateLUTImpl & time_zone, bool & is_calendar)
{
    WhichDataType which(key_type);
    if (!which.isDate() && !which.isDate32() && !which.isDateTime())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Interval window frame offset requires a Date, Date32 or DateTime ORDER BY column, got {}",
            key_type->getName());

    const Int64 count = offset.safeGet<Int64>();
    if (count < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Window frame offset must be nonnegative, INTERVAL {} {} given", count, kind.toKeyword());

    is_calendar = false;
    UInt64 units_per_interval = 0;
    switch (kind.kind)
    {
        case IntervalKind::Kind::Month:
        case IntervalKind::Kind::Quarter:
        case IntervalKind::Kind::Year:
            if (which.isDateTime())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Interval window frame offset INTERVAL {} {} is not supported for a DateTime ORDER BY column, use a Date key instead",
                    count, kind.toKeyword());
            is_calendar = true;
            units_per_interval = kind.kind == IntervalKind::Kind::Month ? 1 : kind.kind == IntervalKind::Kind::Quarter ? 3 : 12;
            break;
        case IntervalKind::Kind::Day:
        case IntervalKind::Kind::Week:
            if (which.isDateTime() && time_zone.hasFixedOffset())
                units_per_interval = kind.toAvgSeconds();
            else
            {
                is_calendar = which.isDateTime();
                units_per_interval = kind.kind == IntervalKind::Kind::Day ? 1 : 7;
            }
            break;
        default:
            if (!which.isDateTime())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Interval window frame offset INTERVAL {} {} is finer than the resolution of the {} ORDER BY column",
                    count, kind.toKeyword(), key_type->getName());
            if (kind.kind < IntervalKind::Kind::Second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Interval window frame offset INTERVAL {} {} is finer than the resolution of the DateTime ORDER BY column",
                    count, kind.toKeyword());
            units_per_interval = kind.toAvgSeconds();
    }

    UInt64 result = 0;
    if (common::mulOverflow(static_cast<UInt64>(count), units_per_interval, result))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Window frame offset INTERVAL {} {} is too large", count, kind.toKeyword());
    return Field(result);
}

struct RangeOffsets
{
    WindowFrame frame;
    RangeOffsetComparator begin_comparator = nullptr;
    RangeOffsetComparator end_comparator = nullptr;
    const DateLUTImpl * time_zone = nullptr;
};

/// Converts the offsets to the units of the ORDER BY key, so that the comparators can shift the key by them,
/// and chooses a comparator for each bound. Null comparators for every frame but RANGE OFFSET, which has
/// exactly one ORDER BY key.
RangeOffsets prepareRangeOffsets(const Block & header, const WindowFrame & frame, const std::vector<size_t> & order_by_indices)
{
    RangeOffsets result;
    result.frame = frame;
    if (!isRangeOffsetFrame(frame))
        return result;

    chassert(order_by_indices.size() == 1);
    const auto & key = header.getByPosition(order_by_indices[0]);
    const RangeOffsetComparator arithmetic_comparator = chooseRangeOffsetComparator(*key.column, /*is_calendar=*/false);

    const DataTypePtr key_type = removeNullable(key.type);
    /// Calendar arithmetic uses the time zone of the DateTime column.
    const DateLUTImpl & time_zone = WhichDataType(key_type).isDateTime()
        ? assert_cast<const DataTypeDateTime &>(*key_type).getTimeZone()
        : DateLUT::instance();

    auto prepare = [&](Field & offset, const std::optional<IntervalKind> & interval_kind, std::string_view bound_name)
    {
        if (interval_kind)
        {
            bool is_calendar = false;
            offset = convertIntervalOffset(offset, *interval_kind, key_type, time_zone, is_calendar);
            if (!is_calendar)
                return arithmetic_comparator;

            result.time_zone = &time_zone;
            return chooseRangeOffsetComparator(*key.column, /*is_calendar=*/true);
        }

        offset = convertFieldToTypeOrThrow(offset, *key.type, nullptr, {}, /*convert_inexact_floats=*/true);
        if (accurateLess(offset, Field(0)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window frame {} offset must be nonnegative, {} given", bound_name, offset);

        return arithmetic_comparator;
    };

    if (frame.begin_type == WindowFrame::BoundaryType::Offset)
        result.begin_comparator = prepare(result.frame.begin_offset, frame.begin_offset_interval_kind, "start");

    if (frame.end_type == WindowFrame::BoundaryType::Offset)
        result.end_comparator = prepare(result.frame.end_offset, frame.end_offset_interval_kind, "end");

    return result;
}

}

WindowTransformParams WindowTransformParams::create(
    const Block & input_header,
    const WindowDescription & window_description,
    const std::vector<WindowFunctionDescription> & functions)
{
    auto header = materializeHeader(input_header);
    auto partition_by_indices = findPositions(header, window_description.partition_by);
    auto order_by_indices = findPositions(header, window_description.order_by);
    auto should_materialize = markColumnsToMaterialize(header, window_description.partition_by, window_description.order_by, functions);
    auto range_offsets = prepareRangeOffsets(header, applyFunctionDefaultFrame(window_description.frame, functions), order_by_indices);

    WindowDescription description = window_description;
    description.frame = std::move(range_offsets.frame);

    return WindowTransformParams{
        std::move(header),
        std::move(description),
        std::move(partition_by_indices),
        std::move(order_by_indices),
        std::move(should_materialize),
        range_offsets.begin_comparator,
        range_offsets.end_comparator,
        range_offsets.time_zone,
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
