#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <base/types.h>

namespace DB
{

struct StepIOStats
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
    size_t group_id = 0;
    std::string name;
    UInt64 wall_clock_time_ns = 0;
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
    StepAnalysisReport step_metric_groups;
    AnalyzedStages stage_reports;
    bool label_stages = false;
};
}
