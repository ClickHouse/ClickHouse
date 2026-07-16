#pragma once

#include <utility>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/StepStatsModel.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/IProcessor.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>
#include <Common/formatReadable.h>
#include <base/types.h>
#include <boost/container_hash/hash.hpp>


namespace DB
{

class AnalyzeStepsStats
{
    using StepAndGroup = std::pair<const IQueryPlanStep *, size_t>;

    /// Per-processor elapsed times collected per (step, group) to compute the distribution.
    /// A multiset keeps the values sorted and preserves duplicates so the median stays correct.
    using ElapsedTimes = std::multiset<UInt64>;
    using ElapsedTimesPerStepGroup = std::unordered_map<StepAndGroup, ElapsedTimes, boost::hash<StepAndGroup>>;

    using StatsByStep = std::unordered_map<const IQueryPlanStep *, StepStats>;
    using StatsByStepAndGroup = std::unordered_map<StepAndGroup, StepGroupStats, boost::hash<StepAndGroup>>;
    using ProcessorsByStepAndGroup = std::unordered_map<StepAndGroup, std::vector<IProcessor *>, boost::hash<StepAndGroup>>;

public:
    AnalyzeStepsStats(const QueryPipeline & pipeline, UInt64 execution_query_time_ns_);

    void printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & detail_prefix, bool processors_info = false) const;

private:
    void collectIOStats(const Processors & processors);
    ElapsedTimesPerStepGroup collectTimingStats(const QueryPipeline & pipeline, const Processors & processors);
    void computeDistribution(const ElapsedTimesPerStepGroup & elapsed_per_step_group);

    StepStatsContext makeContext(const IQueryPlanStep * step) const;
    AnalyzedStepData analyzeStep(const IQueryPlanStep * step) const;
    void renderStep(const AnalyzedStepData & report, WriteBuffer & out, const std::string & prefix, bool processors_info) const;

    StatsByStep stats_by_step;
    StatsByStepAndGroup stats_by_step_group;
    ProcessorsByStepAndGroup processors_by_step_group;

    UInt64 max_num_threads_per_query = 0;
    UInt64 execution_query_time_ns = 0;
};
}
