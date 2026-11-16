#pragma once

#include <Processors/QueryPlan/StepStatsModel.h>

namespace DB
{

/// A report generator that knows some join specific information and builds the report
/// with metrics for printing in EXPLAIN ANALYZE
AnalyzedStepData analyzeJoinStep(const StepStatsContext & context, StepAnalysisReport report);

}
