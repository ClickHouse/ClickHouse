#pragma once

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/IntervalKind.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>

#include <algorithm>
#include <limits>
#include <optional>

namespace DB
{

/** `toStartOfInterval` and `dateTrunc` saturate a `DateTime64` argument into a narrower result type,
  * but a `Date32` argument is still narrowed by a plain cast: the day unit stores a negative or too large
  * second count through an unsaturated `UInt32`, and the week-and-above units wrap `Int32` day numbers
  * in the `UInt16` `Date` result. Across those boundaries the rounding decreases, so it is not monotonic
  * and index analysis must not be told otherwise - it would prune granules that do contain matching
  * rows, and count granules that do not.
  *
  * This describes how a `Date32` argument is narrowed: the rounding that is applied to it and the
  * result type it is cast into. `Date32` and `DateTime64` results are wide enough for the whole
  * `Date32` domain, so a function with such a result has no narrowing to describe.
  */
struct Date32RoundingNarrowing
{
    IntervalKind::Kind kind;
    /// The interval length in units of `kind`. Zero stands for an unknown (non-constant) length, whose
    /// rounding cannot be reasoned about; the function throws on execution anyway.
    Int64 num_units = 0;
    /// The time zone that a day rounding is expressed in: the day-and-below units round into `DateTime`,
    /// whose local midnights differ between zones, so the day on which the `UInt32` seconds run out
    /// differs as well. The week-and-above units round to day numbers and ignore it.
    const DateLUTImpl * time_zone = nullptr;
};

/// Describes the narrowing of a `Date32` argument into the given result type, or nothing when the result
/// does not narrow. The result type may still carry the `Nullable` and `LowCardinality` wrappers of the
/// arguments, for example on a table with `allow_nullable_key`.
inline std::optional<Date32RoundingNarrowing> describeDate32RoundingNarrowing(
    const DataTypePtr & return_type, IntervalKind::Kind kind, Int64 num_units)
{
    const DataTypePtr result_type = removeNullable(recursiveRemoveLowCardinality(return_type));
    if (isDate(result_type))
        return Date32RoundingNarrowing{.kind = kind, .num_units = num_units, .time_zone = &DateLUT::instance("UTC")};
    if (isDateTime(result_type))
        return Date32RoundingNarrowing{
            .kind = kind, .num_units = num_units, .time_zone = &assert_cast<const DataTypeDateTime &>(*result_type).getTimeZone()};
    return {};
}

/// Whether the argument is `Date32` under the `Nullable` and `LowCardinality` wrappers, which
/// `KeyCondition` keeps on the key column type when it rebuilds the monotonic chain.
inline bool isDate32IgnoringWrappers(const IDataType & type)
{
    const IDataType * unwrapped = &type;
    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(unwrapped))
        unwrapped = low_cardinality->getDictionaryType().get();
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(unwrapped))
        unwrapped = nullable->getNestedType().get();
    return isDate32(*unwrapped);
}

/** Returns whether a `Date32` range rounds into values the narrowed result type can hold, so that the
  * rounding is monotonic over it. The check applies the very rounding the function executes to both
  * bounds: the rounding is non-decreasing in the wide type it is computed in, so every value of the
  * range rounds between the rounded bounds, and the range fits exactly when both of them do. This keeps
  * the window as wide as each rounding allows - a year rounding of 2149-12-31 still fits `Date`, while
  * a week rounding of 1970-01-01 reaches back to 1969-12-29 and does not; a day rounding in a zone west
  * of UTC runs out of `UInt32` seconds a day earlier than in UTC. An unbounded or unrecognized bound
  * cannot be proven to fit.
  *
  * A range that may wrap is reported as monotonic only where the rounding is defined, which is weaker
  * than `is_monotonic`: it still lets `KeyCondition` push a comparison constant through a sorting or
  * partition key expression such as `PARTITION BY toStartOfInterval(d, INTERVAL 1 MONTH)`, where an
  * unrepresentable constant is rejected by the dedicated guards in `applyFunctionChainToColumn`, while
  * it stops `applyMonotonicFunctionsChainToRange` from mapping a key range through a wrapping rounding.
  */
inline bool date32RangeFitsRoundingResult(const Date32RoundingNarrowing & narrowing, const Field & left, const Field & right)
{
    if (narrowing.num_units <= 0)
        return false;

    auto day_number = [](const Field & bound) -> std::optional<Int64>
    {
        if (bound.getType() == Field::Types::Int64 || bound.getType() == Field::Types::UInt64)
            return applyVisitor(FieldVisitorConvertToNumber<Int64>(), bound);
        /// Includes `Null`, which stands for an unbounded side of the range.
        return {};
    };

    auto rounding_fits = [&](Int64 day) -> bool
    {
        const auto extended_day = ExtendedDayNum(static_cast<Int32>(
            std::clamp<Int64>(day, std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max())));
        const auto units = static_cast<UInt64>(narrowing.num_units);
        const auto & time_zone = *narrowing.time_zone;

        Int64 rounded = 0;
        Int64 max_result = 0;
        switch (narrowing.kind)
        {
            case IntervalKind::Kind::Day:
                rounded = time_zone.toStartOfDayInterval(extended_day, units);
                max_result = std::numeric_limits<UInt32>::max();
                break;
            case IntervalKind::Kind::Week:
                rounded = time_zone.toStartOfWeekInterval(extended_day, units);
                max_result = std::numeric_limits<UInt16>::max();
                break;
            case IntervalKind::Kind::Month:
                rounded = time_zone.toStartOfMonthInterval(extended_day, units);
                max_result = std::numeric_limits<UInt16>::max();
                break;
            case IntervalKind::Kind::Quarter:
                rounded = time_zone.toStartOfQuarterInterval(extended_day, units);
                max_result = std::numeric_limits<UInt16>::max();
                break;
            case IntervalKind::Kind::Year:
                rounded = time_zone.toStartOfYearInterval(extended_day, units);
                max_result = std::numeric_limits<UInt16>::max();
                break;
            default:
                /// The sub-day units are not defined for a `Date32` argument, so nothing can be proven.
                return false;
        }
        return rounded >= 0 && rounded <= max_result;
    };

    const auto left_day = day_number(left);
    const auto right_day = day_number(right);
    return left_day && right_day && rounding_fits(*left_day) && rounding_fits(*right_day);
}

}
