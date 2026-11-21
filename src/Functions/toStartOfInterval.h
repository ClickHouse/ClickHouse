#pragma once

#include <Common/IntervalKind.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

#include <optional>


class DateLUTImpl;

namespace DB
{

/// `toStartOfInterval` that ignores the setting `to_start_of_interval_preserves_argument_type`, for `dateTrunc`.
FunctionOverloadResolverPtr createLegacyToStartOfIntervalOverloadResolver(ContextPtr context);

/// How a `Date32` argument is rounded into a narrower `Date` or `DateTime` result, which wraps outside the result's range.
struct Date32RoundingNarrowing
{
    IntervalKind::Kind kind;
    Int64 num_units;
    const DateLUTImpl * time_zone;
    /// The day weeks start on for the `Week` kind, 1 = Monday ... 7 = Sunday, from `week_functions_starting_day`.
    UInt8 first_weekday;
};

/// Nothing when `result_type` (Nullable / LowCardinality stripped) holds the whole Date32 domain.
/// `first_weekday` must be the day the rounding itself starts weeks on.
std::optional<Date32RoundingNarrowing> describeDate32RoundingNarrowing(
    const DataTypePtr & result_type, IntervalKind::Kind kind, Int64 num_units, UInt8 first_weekday);

/// Monotonic on [left, right] only when both rounded bounds fit the result type. Nothing else is claimed: the rounding wraps,
/// and a key expression stores the wrapped results.
IFunction::Monotonicity getDate32RoundingMonotonicity(const Date32RoundingNarrowing & narrowing, const Field & left, const Field & right);

}
