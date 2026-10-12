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
    /// merged <= sum(values), and every value is non-negative. The bound is one-sided on
    /// purpose: it is the only half the threshold merge consumes (it serves the descending
    /// order alone), and it is the only half a modular `UInt64` accumulator can promise -
    /// `count` and the unsigned-integer `sum` merge by wrapping addition, so two partials of
    /// `2^63` merge to `0`, which is below both of them. The upper half survives the
    /// wraparound: the modular sum never exceeds the saturating sum of the partials. Also
    /// declared by `uniqExact` (the size of a union of sets), which cannot wrap in practice.
    /// Do not add a lower-bound consumer without first splitting off a stronger variant that
    /// the wrapping accumulators do not advertise.
    Subadditive,
    /// merged == max(values).
    Maximum,
    /// merged == min(values).
    Minimum,
};
}
