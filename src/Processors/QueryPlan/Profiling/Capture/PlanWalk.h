#pragma once

#include <vector>
#include <base/types.h>

namespace DB
{

class IQueryPlanStep;
class QueryPlan;

/// One step, as a walk of the plan found it.
struct VisitedStep
{
    UInt64 id = 0;

    /// This step's own children, then the roots of the child plans it holds.
    std::vector<UInt64> children;

    IQueryPlanStep * step = nullptr;

    /// The plan this step belongs to, which is not the root plan for a step inside a child plan.
    /// Pretty names are scoped per plan, so a reader of the step needs to know which scope it is in.
    const QueryPlan * owning_plan = nullptr;
};

/// Every step of a plan and of the child plans its steps hold, visited once.
///
/// The walk exists so that the readings taken of a plan agree about its shape: `CapturedPlan` and
/// `PlanAnalysisSnapshot` want different things from each step but the same answer to what is
/// below what, and deriving that twice is two rules that have to stay equal.
///
/// It holds pointers into the plan, so it is valid only while the plan is alive. Read it where it
/// is made and keep the values, not this.
struct PlanWalk
{
    /// The plan that was walked. Null when it had nothing to walk.
    const QueryPlan * plan = nullptr;
    UInt64 root_id = 0;
    std::vector<VisitedStep> steps;

    /// `only_built_child_plans` keeps the walk from asking a step to build a child plan it does
    /// not already hold, so that reading a plan does not change the work the query does. The
    /// logged plan passes true. `EXPLAIN ANALYZE` passes false, because it analyses the plan
    /// through `explainPlan`, which does build them, and every step reached there needs an entry.
    static PlanWalk of(const QueryPlan & plan, bool only_built_child_plans);
};

}
