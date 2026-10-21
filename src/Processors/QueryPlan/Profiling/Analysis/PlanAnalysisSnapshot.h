#pragma once

#include <unordered_map>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Profiling/Analysis/JoinBranchCosts.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepAnalysisInputs.h>
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
    JoinBranchNodes join_nodes;

    /// Per step, keyed by `IQueryPlanStep::getUniqID`.
    std::unordered_map<String, StepReporter> reporters;
    std::unordered_map<String, StepAnalysisInputs> inputs;

    /// Walks the plan, including the child plans its steps have already built. Never asks a step to
    /// build one: observing a plan must not change the work the query does.
    static PlanAnalysisSnapshot capture(const QueryPlan & plan);
};

}
