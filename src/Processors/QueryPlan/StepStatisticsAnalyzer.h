#pragma once

#include <Processors/QueryPlan/StepStatisticsModel.h>
#include <base/types.h>

namespace DB
{

class IQueryPlanStep;

using StepStatisticsAnalyzer = AnalyzedStepData (*)(const StepStatisticsContext & context, StepAnalysisReport report);

StepStatisticsAnalyzer getStepStatisticsAnalyzer(const IQueryPlanStep * step);

AnalyzedStepData analyzeDefaultStep(const StepStatisticsContext & context, StepAnalysisReport report);

/// Per stage analyzed data
AnalyzedStages buildAnalyzedStages(const StepStatisticsContext & context);

/// Per step analyzed data
AnalyzedStepData buildAnalyzedStepData(const StepStatisticsContext & context, StepAnalysisReport report);

}
