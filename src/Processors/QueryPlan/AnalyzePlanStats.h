#pragma once

#include <utility>
#include <set>
#include <string>
#include <unordered_map>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/IProcessor.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>
#include <Common/formatReadable.h>
#include <base/types.h>
#include <boost/container_hash/hash.hpp>


namespace DB
{

/// Aggregated statistics for a single plan step (across all of its processors and groups).
struct StepStats
{
    UInt64 input_rows = 0;
    UInt64 input_bytes = 0;
    UInt64 output_rows = 0;
    UInt64 output_bytes = 0;
};

/// Statistics for a single (step, group): timing aggregated over the group's processors.
struct StepGroupStats
{
    UInt64 sum_elapsed_ns = 0;

    /// Distribution of per-processor elapsed time within the (step, group).
    UInt64 min_elapsed_ns = 0;
    UInt64 median_elapsed_ns = 0;
    UInt64 max_elapsed_ns = 0;

    UInt64 wall_clock_time_ns = 0;
    UInt64 total_num_processors = 0;
};

class AnalyzeStepsStats
{
    using StepAndGroup = std::pair<const IQueryPlanStep *, size_t>;

    /// Per-processor elapsed times collected per (step, group) to compute the distribution.
    /// A multiset keeps the values sorted and preserves duplicates so the median stays correct.
    using ElapsedTimes = std::multiset<UInt64>;
    using ElapsedTimesPerStepGroup = std::unordered_map<StepAndGroup, ElapsedTimes, boost::hash<StepAndGroup>>;

public:
    AnalyzeStepsStats(const QueryPipeline & pipeline, UInt64 execution_query_time_ns_);

    /// Print the stats. When with_distribution is set, also print the per-processor
    /// elapsed time distribution (min/median/max/sum) for each (step, group).
    void printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & detail_prefix, bool processors_info = false) const;

private:
    void collectIOStats(const Processors & processors);
    ElapsedTimesPerStepGroup collectTimingStats(const QueryPipeline & pipeline, const Processors & processors);
    void computeDistribution(const ElapsedTimesPerStepGroup & elapsed_per_step_group);

    std::unordered_map<const IQueryPlanStep *, StepStats> stats_by_step;
    std::unordered_map<StepAndGroup, StepGroupStats, boost::hash<StepAndGroup>> stats_by_step_group;

    UInt64 max_num_threads_per_query = 0;
    UInt64 execution_query_time_ns = 0;
};
}
