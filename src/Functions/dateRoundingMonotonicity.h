#pragma once

#include <Core/Field.h>
#include <Common/FieldVisitorConvertToNumber.h>

#include <cstdint>
#include <limits>
#include <optional>

namespace DB
{

/// The standard-precision result types of the date rounding functions, which a `Date32` argument is
/// narrowed into by a plain cast. `Date32` and `DateTime64` results are wide enough for the whole
/// `Date32` domain, so they have no entry here.
enum class DateRoundingResultFamily : uint8_t
{
    Date,     /// `UInt16` day numbers, the result of the week-and-above units
    DateTime  /// `UInt32` seconds, the result of the day-and-below units
};

/** `toStartOfInterval` and `dateTrunc` saturate a `DateTime64` argument into a narrower result type,
  * but a `Date32` argument is still narrowed by a plain cast: the day-and-below units store a negative
  * second count through an unsaturated `UInt32`, and the week-and-above units wrap `Int32` day numbers
  * in the `UInt16` `Date` result. Across those boundaries the rounding decreases, so it is not
  * monotonic and index analysis must not be told otherwise - it would prune granules that do contain
  * matching rows, and count granules that do not.
  *
  * A range that may wrap is reported as monotonic only where the rounding is defined, which is weaker
  * than `is_monotonic`: it still lets `KeyCondition` push a comparison constant through a sorting or
  * partition key expression such as `PARTITION BY toStartOfInterval(d, INTERVAL 1 MONTH)`, where an
  * unrepresentable constant is rejected by the dedicated guards in `applyFunctionChainToColumn`, while
  * it stops `applyMonotonicFunctionsChainToRange` from mapping a key range through a wrapping rounding.
  *
  * Returns whether a `Date32` range is small enough that the given result family does not wrap. The
  * upper bound is the last day the result can hold: 2149-06-06 for `Date`, and a day inside 2106 for
  * `DateTime`, whose `UInt32` seconds run out earlier. The lower bound is the first Monday at or after
  * the epoch, because a week-aligned rounding of an earlier day reaches back before the epoch - for
  * example `toStartOfInterval(toDate32('1970-01-01'), INTERVAL 1 WEEK)` rounds down to 1969-12-29 and
  * the unsigned result wraps it to 2149-06-04. An unbounded or unrecognized bound cannot be proven to
  * fit.
  */
inline bool date32RangeFitsRoundingResult(DateRoundingResultFamily family, const Field & left, const Field & right)
{
    auto day_number = [](const Field & bound) -> std::optional<Int64>
    {
        if (bound.getType() == Field::Types::Int64 || bound.getType() == Field::Types::UInt64)
            return applyVisitor(FieldVisitorConvertToNumber<Int64>(), bound);
        /// Includes `Null`, which stands for an unbounded side of the range.
        return {};
    };

    /// 1970-01-01 was a Thursday, so the week alignment of the rounding starts on 1970-01-05.
    static constexpr Int64 min_day_number = 4;
    const Int64 max_day_number = family == DateRoundingResultFamily::Date
        ? Int64{std::numeric_limits<UInt16>::max()}
        : Int64{std::numeric_limits<UInt32>::max()} / 86'400;

    const auto left_day = day_number(left);
    const auto right_day = day_number(right);
    return left_day && right_day && *left_day >= min_day_number && *right_day <= max_day_number;
}

}
