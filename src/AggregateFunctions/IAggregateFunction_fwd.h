#pragma once

#include <memory>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{
using AggregateDataPtr = char *;
using AggregateDataPtrs = VectorWithMemoryTracking<AggregateDataPtr>;
using ConstAggregateDataPtr = const char *;

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<const IAggregateFunction>;

/// How the finalized value of a state produced by `merge` relates to the finalized values of the
/// states being merged. Lets the top-K threshold merge (Fagin's Threshold Algorithm, see
/// `Aggregator::Params::ThresholdTopKParams`) bound the merged value of a group from its
/// per-thread partial values without merging them. See `IAggregateFunction::getMergedValueBound`.
enum class MergedValueBound : unsigned char
{
    /// No usable relation (the safe default).
    Unknown,
    /// max(values) <= merged <= sum(values), and every value is non-negative.
    /// Holds for `count` and the unsigned-integer `sum` (merged is exactly the sum - with
    /// wraparound the merged value only falls further below the saturating sum of the partials)
    /// and `uniqExact` (the size of a union of sets).
    Subadditive,
    /// merged == max(values).
    Maximum,
    /// merged == min(values).
    Minimum,
};
}
