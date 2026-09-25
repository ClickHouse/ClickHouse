#pragma once

#include <Processors/QueryPlan/StepStatisticsModel.h>
#include <base/types.h>

namespace DB
{

class IQueryPlanStep;

/// What an analyzer is given about the step it is analyzing: the step itself, plus the totals the
/// collector read off the pipeline. Built by `StepStatisticsCollector::makeContext`, and only
/// valid while the step and that pipeline are alive.
struct StepStatisticsContext
{
    const IQueryPlanStep * step = nullptr;
    StepIOStats io;
    UInt64 execution_query_time_ns = 0;
    UInt64 max_num_threads_per_query = 0;
    StepGroupStatsByGroupId group_stats;
};

using StepStatisticsAnalyzer = AnalyzedStepData (*)(const StepStatisticsContext & context, StepAnalysisReport report);

StepStatisticsAnalyzer getStepStatisticsAnalyzer(const IQueryPlanStep * step);

AnalyzedStepData analyzeDefaultStep(const StepStatisticsContext & context, StepAnalysisReport report);

/// Per stage analyzed data
AnalyzedStages buildAnalyzedStages(const StepStatisticsContext & context);

/// Per step analyzed data
AnalyzedStepData buildAnalyzedStepData(const StepStatisticsContext & context, StepAnalysisReport report);

}
