#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

#include <Common/typeid_cast.h>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromQueryResultCacheStep.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>

namespace DB::QueryPlanOptimizations
{

/// True if any conjunct of the ON expression is a constant that is always false.
/// The ON expression is a list of AND-ed conjuncts, so a single always-false conjunct
/// makes the whole condition false (e.g. `a.x = b.y AND a.t = 'A' AND a.t = 'B'`).
static bool onConditionIsAlwaysFalse(const JoinStepLogical & join)
{
    for (const auto & conjunct : join.getJoinOperator().expression)
    {
        if (getFilterResult(conjunct.resolveAliases().getColumn()) == FilterResult::FALSE)
            return true;
    }
    return false;
}

/// True if the subtree carries more than rows: a totals stream (a `TotalsHavingStep`, a remote read or a query result
/// cache read) or one half of a subplan shared with the rest of the plan.
static bool carriesMoreThanRows(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack{&root};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        const auto * step = node->step.get();
        if (typeid_cast<const TotalsHavingStep *>(step)
            || typeid_cast<const ReadFromRemote *>(step)
            || typeid_cast<const ReadFromParallelRemoteReplicasStep *>(step)
            || typeid_cast<const ReadFromQueryResultCacheStep *>(step)
            || typeid_cast<const CommonSubplanStep *>(step)
            || typeid_cast<const CommonSubplanReferenceStep *>(step))
            return true;

        stack.insert(stack.end(), node->children.begin(), node->children.end());
        for (auto * child_plan : node->step->getChildPlans(/*for_explain=*/ false))
        {
            if (auto * child_root = child_plan ? child_plan->getRootNode() : nullptr)
                stack.push_back(child_root);
        }
    }
    return false;
}

size_t replaceJoinInputWithEmptySource(QueryPlan::Node & join_node, size_t side, QueryPlan::Nodes & nodes)
{
    auto * side_node = join_node.children[side];
    if (typeid_cast<const ReadNothingStep *>(side_node->step.get()))
        return 0;

    /// A `JoinStepLogicalLookup` drives physical join building and carries StorageJoin/dictionary validation, so it is never detached.
    for (const auto * node = side_node; node; node = node->children.size() == 1 ? node->children.front() : nullptr)
    {
        if (typeid_cast<const JoinStepLogicalLookup *>(node->step.get()))
            return 0;
    }

    if (carriesMoreThanRows(*side_node))
        return 0;

    auto & empty_node = nodes.emplace_back();
    empty_node.step = std::make_unique<ReadNothingStep>(join_node.step->getInputHeaders()[side]);
    join_node.children[side] = &empty_node;
    return 1;
}

/// When the JOIN ON condition folds to a constant false, replace each non-preserved input side
/// with an empty `ReadNothingStep` so it is not read. The `JoinStepLogical` is kept in place, so
/// logical-to-physical conversion still validates the join (an invalid join keeps throwing).
/// Must run before `splitFilter`/`pushDownFilter`, which lower the constant off the join step.
size_t tryShortCircuitConstantFalseJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    /// Not enabled for distributed plans yet: a `ReadNothing` leaf changes what the distributed
    /// planner sees on that side (shard list, read-rows estimate, join strategy), which needs its
    /// own work. The step itself is serializable.
    if (settings.make_distributed_plan)
        return 0;

    auto * join = typeid_cast<JoinStepLogical *>(parent_node->step.get());
    if (!join || parent_node->children.size() != 2)
        return 0;

    /// Correlated expressions are resolved later during decorrelation, which relies on the join
    /// structure; do not rewrite its inputs.
    if (join->hasCorrelatedExpressions())
        return 0;

    const auto & join_operator = join->getJoinOperator();
    const auto kind = join_operator.kind;
    const auto strictness = join_operator.strictness;

    /// Paste join is positional and has no ON condition.
    if (isPaste(kind))
        return 0;

    /// ASOF matching is inequality-based; leave it untouched.
    if (strictness == JoinStrictness::Asof)
        return 0;

    if (!onConditionIsAlwaysFalse(*join))
        return 0;

    /// A side is "preserved" when its unmatched rows are still emitted (NULL/default-extended):
    /// the left side of LEFT/FULL and the right side of RIGHT/FULL, except for SEMI which keeps
    /// only matched rows. A preserved side must be read; a non-preserved side can be emptied.
    const bool is_semi = strictness == JoinStrictness::Semi;
    const bool left_preserved = isLeftOrFull(kind) && !is_semi;
    const bool right_preserved = isRightOrFull(kind) && !is_semi;

    size_t changed = 0;
    if (!left_preserved)
        changed += replaceJoinInputWithEmptySource(*parent_node, /*side=*/0, nodes);
    if (!right_preserved)
        changed += replaceJoinInputWithEmptySource(*parent_node, /*side=*/1, nodes);
    return changed;
}

}
