#pragma once

#include <Processors/QueryPlan/Profiling/Analysis/StepAnalyzer.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatsModel.h>
#include <base/types.h>

namespace DB
{

class IQueryPlanStep;

/// What `StepFinisher` is given: the stages the step declared, and the totals the collector read
/// off the pipeline. All values, so it can run once the plan is gone.
struct StepStatsContext
{
    /// The stages of the step, as the analyzer captured them.
    std::vector<size_t> groups{0};
    std::unordered_map<size_t, String> group_names;
    StepIOStats io;
    UInt64 execution_query_time_ns = 0;
    UInt64 max_num_threads_per_query = 0;
    const StepTimeAndConcurrency * time_and_conc_stats = nullptr;
    StepGroupStatsByGroupId group_stats;
};

AnalyzedStepData analyzeDefaultStep(const StepStatsContext & context, StepAnalysisReport report);

/// Per stage analyzed data
AnalyzedStages buildAnalyzedStages(const StepStatsContext & context);

/// Per step analyzed data
AnalyzedStepData buildAnalyzedStepData(const StepStatsContext & context, StepAnalysisReport report);

}
