#pragma once

#include <Core/Field.h>
#include <Common/FieldVisitorConvertToNumber.h>

#include <limits>
#include <optional>

namespace DB
{

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
  * Returns whether a `Date32` range is small enough that no standard-precision result type wraps. The
  * narrowest of them is `DateTime`, whose `UInt32` seconds run out inside 2106; `Date` reaches 2149,
  * and using the narrower window for both is conservative. An unbounded or unrecognized bound cannot
  * be proven to fit.
  */
inline bool date32RangeFitsStandardPrecisionResult(const Field & left, const Field & right)
{
    auto day_number = [](const Field & bound) -> std::optional<Int64>
    {
        if (bound.getType() == Field::Types::Int64 || bound.getType() == Field::Types::UInt64)
            return applyVisitor(FieldVisitorConvertToNumber<Int64>(), bound);
        /// Includes `Null`, which stands for an unbounded side of the range.
        return {};
    };

    static constexpr Int64 max_day_number = std::numeric_limits<UInt32>::max() / 86'400;

    const auto left_day = day_number(left);
    const auto right_day = day_number(right);
    return left_day && right_day && *left_day >= 0 && *right_day <= max_day_number;
}

}
