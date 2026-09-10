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
    /// Everything collected from the pipeline is keyed by the step's unique id
    using StepAndGroup = std::pair<String, size_t>;

    /// Per-processor elapsed times collected per (step, group) to compute the distribution.
    /// A multiset keeps the values sorted and preserves duplicates so the median stays correct.
    using ElapsedTimes = std::multiset<UInt64>;
    using ElapsedTimesPerStepGroup = std::unordered_map<StepAndGroup, ElapsedTimes, boost::hash<StepAndGroup>>;

    using StatsByStep = std::unordered_map<String, StepIOStats>;
    using StatsByStepAndGroup = std::unordered_map<StepAndGroup, StepGroupStats, boost::hash<StepAndGroup>>;
    using ProcessorsByStep = std::unordered_map<String, std::vector<IProcessor *>>;

public:
    StepStatsStorage(const QueryPipeline & pipeline, UInt64 execution_query_time_ns_);

    /// Unlike collecting, this needs a live step: getAnalysisReport reads state that only the step
    /// object holds, and getStepGroups and the analyzer dispatch have no id-based equivalent. It
    /// must therefore run while the pipeline is still alive, because the processors handed to
    /// getAnalysisReport belong to it. What it returns is a plain value that outlives both.
    AnalyzedStepData analyzeStep(const IQueryPlanStep * step) const;

    /// How long the query executed.
    UInt64 getExecutionTimeNs() const { return execution_query_time_ns; }

private:
    void collectIOStats(const Processors & processors);
    ElapsedTimesPerStepGroup collectTimingStats(const QueryPipeline & pipeline, const Processors & processors);
    void computeDistribution(const ElapsedTimesPerStepGroup & elapsed_per_step_group);

    StepStatsContext makeContext(const IQueryPlanStep * step) const;

    StatsByStep stats_by_step;
    StatsByStepAndGroup stats_by_step_group;
    ProcessorsByStep processors_by_step;

    UInt64 max_num_threads_per_query = 0;
    UInt64 execution_query_time_ns = 0;
};

}
