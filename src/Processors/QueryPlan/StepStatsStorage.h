#pragma once

#include <set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/StepStatsModel.h>
#include <QueryPipeline/QueryPipeline.h>
#include <base/types.h>
#include <boost/container_hash/hash.hpp>


namespace DB
{

/// Holds everything the pipeline reported about each plan step, and turns it into the per-step
/// values a consumer can render.
class StepStatsStorage
{
    /// Everything collected from the pipeline is keyed by the step it belongs to. A raw pointer is
    /// safe here: the storage is built and consumed while the plan and its pipeline are alive, and
    /// analyzeStep needs a live step anyway.
    using StepAndGroup = std::pair<const IQueryPlanStep *, size_t>;

    /// Per-processor elapsed times collected per (step, group) to compute the distribution.
    /// A multiset keeps the values sorted and preserves duplicates so the median stays correct.
    using ElapsedTimes = std::multiset<UInt64>;
    using ElapsedTimesPerStepGroup = std::unordered_map<StepAndGroup, ElapsedTimes, boost::hash<StepAndGroup>>;

    using StatsByStep = std::unordered_map<const IQueryPlanStep *, StepIOStats>;
    using StatsByStepAndGroup = std::unordered_map<StepAndGroup, StepGroupStats, boost::hash<StepAndGroup>>;
    using ProcessorsByStep = std::unordered_map<const IQueryPlanStep *, std::vector<IProcessor *>>;
    using ReportsByStep = std::unordered_map<const IQueryPlanStep *, StepAnalysisReport>;

public:
    StepStatsStorage(const QueryPipeline & pipeline, const QueryPlan & plan, UInt64 execution_query_time_ns_);

    /// Must run while the pipeline is still alive: getAnalysisReport reads state only the step
    /// object holds, and the processors handed to it belong to the pipeline. What it returns is a
    /// plain value that outlives both, which is what a renderer is given.
    AnalyzedStepData analyzeStep(const IQueryPlanStep * step) const;

    /// How long the query executed.
    UInt64 getExecutionTimeNs() const { return execution_query_time_ns; }

    /// The pipeline's thread count, which caps how parallel any stage could have been.
    UInt64 getMaxThreads() const { return max_num_threads_per_query; }

private:
    void collectIOStats(const Processors & processors);
    ElapsedTimesPerStepGroup collectTimingStats(const QueryPipeline & pipeline, const Processors & processors);
    void computeDistribution(const ElapsedTimesPerStepGroup & elapsed_per_step_group);
    void computeJoinBranchCosts(const QueryPlan & plan);

    StepStatsContext makeContext(const IQueryPlanStep * step) const;

    StatsByStep stats_by_step;
    StatsByStepAndGroup stats_by_step_group;
    ProcessorsByStep processors_by_step;

    /// Reports for join steps, produced up front by computeJoinBranchCosts because a branch cost
    /// needs the whole plan, not one step. analyzeStep prefers these over asking the step again.
    ReportsByStep join_raw_reports;

    UInt64 max_num_threads_per_query = 0;
    UInt64 execution_query_time_ns = 0;
};

}
