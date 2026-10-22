#pragma once

#include <unordered_map>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Profiling/Analysis/PlanShape.h>
#include <Processors/QueryPlan/Profiling/Capture/PlanWalk.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepAnalyzer.h>
#include <base/types.h>

namespace DB
{

class QueryPlan;

/// Everything the analysis of a query needs from its plan, read out while the plan is alive and
/// held as values afterwards.
///
/// `EXPLAIN ANALYZE` takes it from the plan it is holding anyway; the logged plan takes it as soon
/// as the pipeline has been built, which is the last moment the plan exists, and the statistics are
/// then shaped once the query has run without the plan being around at all.
struct PlanAnalysisSnapshot
{
    String root_id;

    /// Who is below whom, for the passes that accumulate over a step's subtree:
    /// the cost of a join's branch, and the time of one.
    PlanShape shape;

    /// The analyzer each step handed out, keyed by `IQueryPlanStep::getUniqID`.
    std::unordered_map<String, StepAnalyzer> analyzers;

    static PlanAnalysisSnapshot capture(const PlanWalk & walk);
};

}
