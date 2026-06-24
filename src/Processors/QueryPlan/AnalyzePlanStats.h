#pragma once

#include <utility>
#include <map>
#include <string>
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

struct StepIoStats
{
    UInt64 input_rows = 0;
    UInt64 input_bytes = 0;
    UInt64 output_rows = 0;
    UInt64 output_bytes = 0;
};

struct AnalyzeStats
{
    UInt64 sum_elapsed_ns = 0;

    /// Distribution of per-processor elapsed time within the (step, group).
    UInt64 min_elapsed_ns = 0;
    UInt64 median_elapsed_ns = 0;
    UInt64 max_elapsed_ns = 0;

    UInt64 wall_clock_time_ns = 0;

    UInt64 total_num_processors = 0;

    /// TODO: how to account for input wait time
    /// among several processors?
    UInt64 max_input_wait_ns = 0;
    /// TODO: how to account for output wait time
    /// among several processors?
    UInt64 max_output_wait_ns = 0;
};

class AnalyzeStepsStats
{
    using StepAndGroup = std::pair<const IQueryPlanStep *, size_t>;
public:
    explicit AnalyzeStepsStats(const QueryPipeline & pipeline, UInt64 execution_query_time_ns_);

    /// Print the stats. When with_distribution is set, also print the per-processor
    /// elapsed time distribution (min/median/max/sum) for each (step, group).
    void printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & detail_prefix, bool processors_info = false) const;

private:

    std::map<const IQueryPlanStep *, StepIoStats> step_io;
    std::map<StepAndGroup, AnalyzeStats> steps_to_stats;

    UInt64 max_num_threads_per_query = 0;
    UInt64 execution_query_time_ns = 0;
};
}
