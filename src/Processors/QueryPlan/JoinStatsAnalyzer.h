#pragma once

#include <unordered_map>
#include <Processors/QueryPlan/StepStatsModel.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <Processors/QueryPlan/JoinBranchCosts.h>
#include <Core/Joins.h>

namespace DB
{

class QueryPlan;
class JoinStep;

JoinAnalysisCounters extractJoinCounters(const StepAnalysisReport & report);

/// Actual matched pairs, i.e. the output rows produced by real matches (excluding NULL-padded
/// rows of the preserved side of an outer join). `kind` and `counters` must be in the same side orientation.
UInt64 joinMatchedPairs(JoinKind kind, const JoinAnalysisCounters & counters, UInt64 output_rows);

struct JoinRuntimeReport
{
    StepAnalysisReport report;
    UInt64 output_rows = 0;
};

using JoinRuntimeReports = std::unordered_map<const JoinStep *, JoinRuntimeReport>;

class JoinStepStatsAnalyzer
{
public:
    JoinStepStatsAnalyzer() = default;
    JoinStepStatsAnalyzer(const QueryPlan & plan, const JoinRuntimeReports & join_reports);

    AnalyzedStepData analyze(const StepStatsContext & context, StepAnalysisReport report) const;

private:
    JoinBranchCosts branch_costs;
};

}
