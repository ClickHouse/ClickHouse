#pragma once

#include <Core/Joins.h>
#include <Processors/QueryPlan/StepStatsModel.h>

namespace DB
{

/// A report generator that knows some join specific information and builds the report
/// with metrics for printing in EXPLAIN ANALYZE
AnalyzedStepData analyzeJoinStep(const StepStatsContext & context, StepAnalysisReport report);

/// Actual matched pairs, i.e. the output rows produced by real matches (excluding NULL-padded
/// rows of the preserved sides of an outer join). `kind` must be in the same side orientation
/// as the Left/Right groups of `report`. nullopt when the per-side counters are unavailable.
std::optional<UInt64> joinMatchedOutputRows(
    const StepAnalysisReport & report, UInt64 output_rows, JoinKind kind, JoinStrictness strictness);

}
