#pragma once

#include <Processors/QueryPlan/Profiling/Analysis/StepStatsModel.h>
#include <base/types.h>

namespace DB
{

class IQueryPlanStep;

/// What an analyzer is given about the step it is analyzing: the step itself, plus the totals the
/// collector read off the pipeline. Only valid while that step and its pipeline are alive.
struct StepStatsContext
{
    const IQueryPlanStep * step = nullptr;
    StepIOStats io;
    UInt64 execution_query_time_ns = 0;
    UInt64 max_num_threads_per_query = 0;
    const StepTimeAndConcurrency * time_and_conc_stats = nullptr;
    StepGroupStatsByGroupId group_stats;
};

using StepStatsAnalyzer = AnalyzedStepData (*)(const StepStatsContext & context, StepAnalysisReport report);

StepStatsAnalyzer getStepStatsAnalyzer(const IQueryPlanStep * step);

AnalyzedStepData analyzeDefaultStep(const StepStatsContext & context, StepAnalysisReport report);

/// Per stage analyzed data
AnalyzedStages buildAnalyzedStages(const StepStatsContext & context);

/// Per step analyzed data
AnalyzedStepData buildAnalyzedStepData(const StepStatsContext & context, StepAnalysisReport report);

}
