#pragma once

#include <base/types.h>
#include <optional>

namespace DB
{

/// Defined in `RelationEstimateInfo.h`; declared opaque here so that the plan steps and processors
/// carrying a `CostEstimationInfo` do not depend on that header. Zero is `NoSource`.
enum class RowEstimateSource : UInt8;

/// The estimate an optimizer attached to a plan step, whichever planner decided the step.
/// `EXPLAIN estimates = 1` prints it, and the processors created from the step record it in
/// `system.processors_profile_log` next to the rows they produced.
struct CostEstimationInfo
{
    /// Absent when the optimizer had no estimate for the step.
    std::optional<Float64> rows;
    /// Absent when the optimizer has no cost for the step; the unit is that optimizer's own.
    std::optional<Float64> cost;
    RowEstimateSource source{};
    /// Rows the step cannot exceed; absent when nothing bounds them. `EXPLAIN bounds = 1` prints it.
    std::optional<Float64> max_rows = {};
    /// The estimate took a default selectivity or a model assumption, so it is a guess.
    bool estimate_from_defaults = false;
    /// Without an estimate, the row count the cost model searched with (Cascades).
    std::optional<Float64> search_rows = {};
};

}
