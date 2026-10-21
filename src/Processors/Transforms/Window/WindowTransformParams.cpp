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
#include <Common/NaNUtils.h>
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

/// -1, 0 or 1 for two values of which either may be missing(NULL or NaN).
int compareMissing(bool lhs_is_missing, bool rhs_is_missing, bool missing_is_greater)
{
    if (lhs_is_missing == rhs_is_missing)
        return 0;

    return lhs_is_missing == missing_is_greater ? 1 : -1;
}

template <typename T, bool missing_is_greater>
int compareRangeOffset(
    const IColumn * lhs_column, size_t lhs_row,
    const IColumn * rhs_column, size_t rhs_row,
    const Field & offset, bool offset_is_preceding,
    const DateLUTImpl * /*time_zone*/)
{
    const T lhs_value = assert_cast<const ColumnVector<T> &>(*lhs_column).getData()[lhs_row];
    const T rhs_value = assert_cast<const ColumnVector<T> &>(*rhs_column).getData()[rhs_row];
    if constexpr (std::is_floating_point_v<T>)
    {
        const bool lhs_is_nan = isNaN(lhs_value);
        const bool rhs_is_nan = isNaN(rhs_value);
        if (lhs_is_nan || rhs_is_nan)
            return compareMissing(lhs_is_nan, rhs_is_nan, missing_is_greater);
    }

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

/// The same for a calendar INTERVAL, whose offset counts months of a Date or Date32 key.
/// The frame search moves the bounds only forward, so the shifted key must not decrease as the key grows.
/// addMonths keeps this, as it saturates the day of month.
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

    /// A shifted key may leave the range of the key type, so a Date key is shifted as an ExtendedDayNum too:
    /// the DayNum overload of addMonths saturates below 1970-01-01 and wraps above 2149-06-06.
    static_assert(std::is_same_v<T, UInt16> || std::is_same_v<T, Int32>);
    const ExtendedDayNum rhs_day(rhs_value);

    /// The ExtendedDayNum overload saturates in turn a month outside the representable calendar to its first or
    /// last day, which a Date32 key can equal. Such a bound is below or above every key.
    const UInt32 rhs_yyyymm = time_zone->toNumYYYYMM(rhs_day);
    const Int64 shifted_month_index = static_cast<Int64>(rhs_yyyymm / 100) * 12 + rhs_yyyymm % 100 - 1 + delta;
    if (shifted_month_index < static_cast<Int64>(DATE_LUT_MIN_REPRESENTABLE_YEAR) * 12)
        return 1;
    if (shifted_month_index > static_cast<Int64>(DATE_LUT_MAX_REPRESENTABLE_YEAR) * 12 + 11)
        return -1;

    const Int64 shifted_rhs = time_zone->addMonths(rhs_day, delta);

    const Int64 lhs = lhs_value;
    if (lhs < shifted_rhs)
        return -1;
    else if (lhs == shifted_rhs)
        return 0;
    else
        return 1;
}

/// A comparator over a Nullable column: NULL equals NULL and sorts as the ORDER BY key puts it.
template <RangeOffsetComparator compare_nested, bool missing_is_greater>
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
        return compareMissing(lhs_is_null, rhs_is_null, missing_is_greater);

    return compare_nested(&lhs.getNestedColumn(), lhs_row, &rhs.getNestedColumn(), rhs_row, offset, offset_is_preceding, time_zone);
}

template <bool nullable, bool missing_is_greater, RangeOffsetComparator compare>
RangeOffsetComparator maybeNullable()
{
    if constexpr (nullable)
        return compareNullableRangeOffset<compare, missing_is_greater>;
    else
        return compare;
}

/// The comparator specialised for the value type of `column`, which is the nested column for a Nullable key.
template <bool nullable, bool missing_is_greater>
RangeOffsetComparator chooseRangeOffsetComparatorForValueType(const IColumn & column)
{
    switch (column.getDataType())
    {
        case TypeIndex::UInt8: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<UInt8, missing_is_greater>>();
        case TypeIndex::UInt16: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<UInt16, missing_is_greater>>();
        case TypeIndex::UInt32: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<UInt32, missing_is_greater>>();
        case TypeIndex::UInt64: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<UInt64, missing_is_greater>>();
        case TypeIndex::Int8: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<Int8, missing_is_greater>>();
        case TypeIndex::Int16: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<Int16, missing_is_greater>>();
        case TypeIndex::Int32: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<Int32, missing_is_greater>>();
        case TypeIndex::Int64: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<Int64, missing_is_greater>>();
        case TypeIndex::Int128: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<Int128, missing_is_greater>>();
        case TypeIndex::Float32: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<Float32, missing_is_greater>>();
        case TypeIndex::Float64: return maybeNullable<nullable, missing_is_greater, compareRangeOffset<Float64, missing_is_greater>>();
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The RANGE OFFSET frame for '{}' ORDER BY column is not implemented", column.getName());
    }
}

/// The same for a calendar INTERVAL offset, whose key was already checked to be a Date or Date32.
template <bool nullable, bool missing_is_greater>
RangeOffsetComparator chooseCalendarRangeOffsetComparatorForValueType(const IColumn & column)
{
    switch (column.getDataType())
    {
        case TypeIndex::UInt16: return maybeNullable<nullable, missing_is_greater, compareCalendarRangeOffset<UInt16>>();
        case TypeIndex::Int32: return maybeNullable<nullable, missing_is_greater, compareCalendarRangeOffset<Int32>>();
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Calendar interval offset is not supported for ORDER BY column {}", column.getName());
    }
}

template <bool missing_is_greater>
RangeOffsetComparator chooseRangeOffsetComparatorForMissingOrder(const IColumn & column, bool is_calendar)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        const IColumn & nested = nullable->getNestedColumn();
        return is_calendar ? chooseCalendarRangeOffsetComparatorForValueType<true, missing_is_greater>(nested)
                           : chooseRangeOffsetComparatorForValueType<true, missing_is_greater>(nested);
    }

    return is_calendar ? chooseCalendarRangeOffsetComparatorForValueType<false, missing_is_greater>(column)
                       : chooseRangeOffsetComparatorForValueType<false, missing_is_greater>(column);
}

/// `missing_is_greater` tells whether NULL and NaN keys sort after the other values.
RangeOffsetComparator chooseRangeOffsetComparator(const IColumn & column, bool is_calendar, bool missing_is_greater)
{
    return missing_is_greater ? chooseRangeOffsetComparatorForMissingOrder<true>(column, is_calendar)
                              : chooseRangeOffsetComparatorForMissingOrder<false>(column, is_calendar);
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

std::vector<size_t> findPositions(const Block & header, const SortDescription & description)
{
    return description
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

/// Converts an INTERVAL offset into the units of the ORDER BY key: days for Date/Date32, seconds for DateTime.
/// MONTH/QUARTER/YEAR have no fixed length: they set `is_calendar` and are converted to months, for Date keys only,
/// see compareCalendarRangeOffset.
/// DAY/WEEK on a DateTime key are converted to seconds, which is exact only in a fixed-offset time zone. Elsewhere
/// `ts - INTERVAL 1 DAY` is 23 or 25 hours across a DST switch, and it is not monotone in `ts` around the switch:
/// in Europe/Berlin, 2024-10-27 02:30 CEST minus a day is 2024-10-26 02:30, but the later 2024-10-27 02:00 CET
/// minus a day is 2024-10-26 02:00. The frame search moves the bounds only forward, so such offsets are rejected.
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
    switch (kind.getKind())
    {
        case IntervalKind::Kind::Month:
        case IntervalKind::Kind::Quarter:
        case IntervalKind::Kind::Year:
            if (which.isDateTime())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Interval window frame offset INTERVAL {} {} is not supported for a DateTime ORDER BY column, use a Date key instead",
                    count, kind.toKeyword());
            is_calendar = true;
            units_per_interval = kind.getKind() == IntervalKind::Kind::Month ? 1 : kind.getKind() == IntervalKind::Kind::Quarter ? 3 : 12;
            break;
        case IntervalKind::Kind::Day:
        case IntervalKind::Kind::Week:
            if (!which.isDateTime())
                units_per_interval = kind.getKind() == IntervalKind::Kind::Day ? 1 : 7;
            else if (time_zone.hasFixedOffset())
                units_per_interval = kind.toAvgSeconds();
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Interval window frame offset INTERVAL {} {} is not supported for a DateTime ORDER BY column in time zone '{}', "
                    "where a day is not always 24 hours. Use an offset in hours, a Date key, or a DateTime key in a fixed-offset "
                    "time zone such as UTC",
                    count, kind.toKeyword(), time_zone.getTimeZone());
            break;
        default:
            if (!which.isDateTime())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Interval window frame offset INTERVAL {} {} is finer than the resolution of the {} ORDER BY column",
                    count, kind.toKeyword(), key_type->getName());
            if (kind.getKind() < IntervalKind::Kind::Second)
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
RangeOffsets prepareRangeOffsets(
    const Block & header, const WindowFrame & frame, const SortDescription & order_by, const std::vector<size_t> & order_by_indices)
{
    RangeOffsets result;
    result.frame = frame;
    if (!isRangeOffsetFrame(frame))
        return result;

    chassert(order_by_indices.size() == 1);
    const auto & key = header.getByPosition(order_by_indices[0]);
    const bool missing_is_greater = order_by[0].nulls_direction > 0;
    const RangeOffsetComparator arithmetic_comparator = chooseRangeOffsetComparator(*key.column, /*is_calendar=*/false, missing_is_greater);

    const DataTypePtr key_type = removeNullable(key.type);
    /// The time zone of a DateTime key decides whether a day is always 24 hours. Month arithmetic on a Date key
    /// does not depend on the time zone.
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
            {
                /// The arithmetic comparator casts the offset to the value type of the key, so an offset that
                /// does not fit is rejected, as a numeric offset is.
                offset = convertFieldToTypeOrThrow(offset, *key.type, nullptr, {}, /*convert_inexact_floats=*/true);
                return arithmetic_comparator;
            }

            result.time_zone = &time_zone;
            return chooseRangeOffsetComparator(*key.column, /*is_calendar=*/true, missing_is_greater);
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
    auto range_offsets = prepareRangeOffsets(header, applyFunctionDefaultFrame(window_description.frame, functions), window_description.order_by, order_by_indices);

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

}
