#pragma once

#include <unordered_map>
#include <vector>
#include <Processors/QueryPlan/Profiling/Analysis/StepAnalyzer.h>
#include <Processors/QueryPlan/Profiling/Capture/PlanWalk.h>
#include <base/types.h>

namespace DB
{

/// Everything the analysis of a query needs from its plan, read out while the plan is alive and
/// held as values afterwards.
///
/// `EXPLAIN ANALYZE` takes it from the plan it is holding anyway; the logged plan takes it as soon
/// as the pipeline has been built, which is the last moment the plan exists, and the statistics are
/// then shaped once the query has run without the plan being around at all.
struct PlanAnalysisSnapshot
{
    UInt64 root_id = 0;

    /// The analyzer each step handed out, keyed by `IQueryPlanStep::getStepIndex`. Each one carries
    /// the ids below it, so this is the shape of the plan as well as the means to analyse it.
    std::unordered_map<UInt64, StepAnalyzer> analyzers;

    static PlanAnalysisSnapshot capture(const PlanWalk & walk);

    /// Children before parents, so that a pass accumulating over a step's subtree -- the cost of a
    /// join's branch, the time of one -- finds its children already retired when it reaches it.
    std::vector<const StepAnalyzer *> stepsInPostOrder() const;

    const StepAnalyzer * find(UInt64 step_id) const;
};

}
