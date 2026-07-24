#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

#include <Common/typeid_cast.h>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadNothingStep.h>

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

/// Short-circuit the read of an input side that can never contribute a row when the JOIN ON
/// condition folds to a constant false (e.g. `ON 1 = 2`, `ON NULL`, or `ON a.x = b.y AND a.t = 'A'
/// AND a.t = 'B'`). No pair of rows can match, so a side whose unmatched rows are dropped (a
/// non-"preserved" side) contributes nothing and does not need to be read. Such an input is
/// replaced with an empty `ReadNothingStep` of the same header, so the non-contributing table is
/// never scanned. For example:
///   - `INNER/CROSS/SEMI`: neither side is preserved, so both inputs are emptied and the (empty)
///     result is produced without reading either table.
///   - `LEFT ALL/ANY/ANTI`: the left side is preserved, so only the right input is emptied; the
///     join then emits every left row with NULL/default right columns as before.
///   - `FULL`: both sides are preserved, so neither input can be dropped.
///
/// The `JoinStepLogical` is deliberately kept in place (never replaced by an empty source): the
/// logical-to-physical conversion still runs and performs join validation (StorageJoin key checks,
/// ASOF/`INVALID_JOIN_ON_EXPRESSION`, etc.), so an invalid join keeps throwing exactly as before.
/// This runs before `splitFilter`/`pushDownFilter` lower the constant-false condition onto an
/// input, so the condition is still visible on the `JoinStepLogical` here.
size_t tryShortCircuitConstantFalseJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
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

    /// Replace one input's subtree with an empty source of the same header. Idempotent (a no-op when
    /// the side is already empty). A `JoinStepLogicalLookup` input drives physical join building and
    /// carries StorageJoin/dictionary validation, so it is never detached: correctness is kept and
    /// only the optimization is skipped for that side (such build sides are in-memory and cheap).
    auto empty_side = [&](size_t side) -> size_t
    {
        auto * side_node = parent_node->children[side];
        if (typeid_cast<const ReadNothingStep *>(side_node->step.get()))
            return 0;
        if (typeid_cast<const JoinStepLogicalLookup *>(side_node->step.get()))
            return 0;

        auto & empty_node = nodes.emplace_back();
        empty_node.step = std::make_unique<ReadNothingStep>(join->getInputHeaders()[side]);
        parent_node->children[side] = &empty_node;
        return 1;
    };

    size_t changed = 0;
    if (!left_preserved)
        changed += empty_side(/*side=*/0);
    if (!right_preserved)
        changed += empty_side(/*side=*/1);
    return changed;
}

}
