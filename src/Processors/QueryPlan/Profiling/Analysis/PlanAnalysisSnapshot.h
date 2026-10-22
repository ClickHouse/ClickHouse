#pragma once

#include <unordered_map>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Profiling/Analysis/JoinBranchCosts.h>
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

    /// The shape, for accumulating the cost of each join's branch.
    PlanShape shape;

    /// The analyzer each step handed out, keyed by `IQueryPlanStep::getUniqID`.
    std::unordered_map<String, StepAnalyzer> analyzers;

    /// Walks the plan and the child plans its steps own.
    ///
    /// `only_built_child_plans` keeps the walk from asking a step to build one it does not already
    /// hold, which is what the logged plan needs: capturing a plan must not change the work the
    /// query does. `EXPLAIN ANALYZE` passes false, because the walk it later analyses -- the one
    /// `explainPlan` makes -- does build them, and a step it reaches without a snapshot entry would
    /// be shaped as a plain step and report nothing.
    static PlanAnalysisSnapshot capture(const QueryPlan & plan, bool only_built_child_plans);
};

}
