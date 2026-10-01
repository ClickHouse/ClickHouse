#pragma once

#include <optional>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/SetWithMemoryTracking.h>
#include <utility>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatsModel.h>
#include <Processors/QueryPlan/Profiling/Time/StepIntervalTimings.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/IProcessor.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>
#include <Common/formatReadable.h>
#include <base/types.h>
#include <boost/container_hash/hash.hpp>


namespace DB
{

class StepProfiler;

class AnalyzeStepsStats
{
    using StepAndGroup = std::pair<const IQueryPlanStep *, size_t>;

    /// Per-processor elapsed times collected per (step, group) to compute the distribution.
    /// A multiset keeps the values sorted and preserves duplicates so the median stays correct.
    using ElapsedTimes = MultiSetWithMemoryTracking<UInt64>;
    using ElapsedTimesPerStepGroup = UnorderedMapWithMemoryTracking<StepAndGroup, ElapsedTimes, boost::hash<StepAndGroup>>;

    using StatsByStep = UnorderedMapWithMemoryTracking<const IQueryPlanStep *, StepIOStats>;
    using StatsByStepAndGroup = UnorderedMapWithMemoryTracking<StepAndGroup, StepGroupStats, boost::hash<StepAndGroup>>;
    using ProcessorsByStep = UnorderedMapWithMemoryTracking<const IQueryPlanStep *, VectorWithMemoryTracking<IProcessor *>>;
    using ReportsByStep = UnorderedMapWithMemoryTracking<const IQueryPlanStep *, StepAnalysisReport>;

public:
    AnalyzeStepsStats(const QueryPipeline & pipeline, const QueryPlan & plan, StepProfiler & step_profiler, UInt64 execution_start_ns, UInt64 execution_query_time_ns_);

    void printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & detail_prefix, bool processors_info = false) const;

    /// Empty when the work intervals were not collected, that is without the `time` setting.
    std::optional<ExecutionTimeBreakdown> executionTimeBreakdown() const;

private:
    void collectIOStats(const Processors & processors);
    ElapsedTimesPerStepGroup collectTimingStats(const StepProfiler & step_profiler, const Processors & processors);
    void computeDistribution(const ElapsedTimesPerStepGroup & elapsed_per_step_group);
    void computeJoinBranchCosts(const QueryPlan & plan);

    StepStatsContext makeContext(const IQueryPlanStep * step) const;
    AnalyzedStepData analyzeStep(const IQueryPlanStep * step) const;
    void renderStep(const AnalyzedStepData & step_data, WriteBuffer & out, const std::string & prefix, bool processors_info) const;

    StatsByStep stats_by_step;
    StatsByStepAndGroup stats_by_step_group;
    ProcessorsByStep processors_by_step;

    ReportsByStep join_raw_reports;

    std::optional<StepIntervalTimings> interval_timings;

    UInt64 max_num_threads_per_query = 0;
    UInt64 execution_query_time_ns = 0;
};
}
