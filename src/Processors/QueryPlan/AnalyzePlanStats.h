#pragma once

#include <utility>
#include <unordered_map>
#include <string>
#include <algorithm>
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

struct AnalyzeStats
{
    UInt64 total_query_time = 0;
    UInt64 sum_elapsed_ns = 0;

    UInt64 input_rows = 0;
    UInt64 input_bytes = 0;
    UInt64 output_rows = 0;
    UInt64 output_bytes = 0;

    UInt64 wall_clock_time = 0;

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
    explicit AnalyzeStepsStats(const QueryPipeline & pipeline);

    /// Print the stats
    void printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & detail_prefix) const;

private:

    std::unordered_map<
        StepAndGroup, 
        AnalyzeStats,
        boost::hash<StepAndGroup>
    > steps_to_stats; 
};
}
