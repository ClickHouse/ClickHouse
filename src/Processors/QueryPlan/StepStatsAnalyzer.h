#pragma once

#include <Processors/QueryPlan/StepStatsModel.h>
#include <base/types.h>

namespace DB
{

class IQueryPlanStep;

using StepStatsAnalyzer = AnalyzedStepData (*)(const StepStatsContext & context, StepAnalysisReport report);

StepStatsAnalyzer getStepStatsAnalyzer(const IQueryPlanStep * step);

AnalyzedStepData analyzeDefaultStep(const StepStatsContext & context, StepAnalysisReport report);

/// Per stage of a step analyzed data
AnalyzedStages buildAnalyzedStages(const StepStatsContext & context);

/// Per step analyzed data
AnalyzedStepData buildAnalyzedStepData(const StepStatsContext & context, StepAnalysisReport report);

}
