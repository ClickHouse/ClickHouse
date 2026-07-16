#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <base/types.h>

namespace DB
{

class IQueryPlanStep;

struct StepStats
{
    UInt64 input_rows = 0;
    UInt64 input_bytes = 0;
    UInt64 output_rows = 0;
    UInt64 output_bytes = 0;
};

struct StepGroupStats
{
    UInt64 sum_elapsed_ns = 0;

    UInt64 min_elapsed_ns = 0;
    UInt64 median_elapsed_ns = 0;
    UInt64 max_elapsed_ns = 0;

    UInt64 wall_clock_time_ns = 0;
    UInt64 total_num_processors = 0;
};

using StepGroupStatsByGroupId = std::unordered_map<size_t, StepGroupStats>;

struct AnalyzedStage
{
    size_t group = 0;
    std::string name;
    UInt64 wall_clock_time_ns = 0;
    UInt64 sum_elapsed_ns = 0;
    UInt64 total_num_processors = 0;
    double share_of_query_time = 0.0;
    double parallelism = 0.0;
    UInt64 max_parallelism = 0;
    MetricList inline_metrics;
    MetricList processor_distribution;
};

using AnalyzedStages = std::vector<AnalyzedStage>;

struct AnalyzedStepData
{
    StepStats io;
    StepAnalysisReport groups;
    AnalyzedStages stages;
    bool label_stages = false;
};

struct StepStatsContext
{
    const IQueryPlanStep * step = nullptr;
    StepStats io;
    UInt64 execution_query_time_ns = 0;
    UInt64 max_num_threads_per_query = 0;
    StepGroupStatsByGroupId group_stats;
};

}
