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

/// True if the subtree rooted at `node` provably produces zero rows.
///
/// Only a `ReadNothingStep` is treated as empty. A constant-false `FilterStep` is intentionally
/// NOT considered empty: an aggregation with `WITH TOTALS`/`WITH ROLLUP`/`WITH CUBE` below the
/// filter still emits the totals row, which bypasses the filter, so replacing the filtered
/// subtree with an empty source would drop that row (see 03788).
static bool producesNoRows(const QueryPlan::Node * node)
{
    return node && typeid_cast<const ReadNothingStep *>(node->step.get()) != nullptr;
}

/// True if any conjunct of the ON expression is a constant that is always false.
/// The ON expression is a list of AND-ed conjuncts, so a single always-false conjunct
/// makes the whole condition false (e.g. `a.x = b.y AND a.t = 'A' AND a.t = 'B'`).
static bool onConditionIsAlwaysFalse(const JoinStepLogical & join)
{
    /// Correlated expressions are resolved later during decorrelation; do not touch them.
    if (join.hasCorrelatedExpressions())
        return false;

    for (const auto & conjunct : join.getJoinOperator().expression)
    {
        if (getFilterResult(conjunct.resolveAliases().getColumn()) == FilterResult::FALSE)
            return true;
    }
    return false;
}

/// Short-circuit a JOIN whose result (or one of whose inputs) is provably empty.
///
/// A constant-false ON condition, or an already-empty input, means one side can never
/// contribute a matching row. Instead of reading the non-contributing side in full:
///   - INNER/CROSS/SEMI: the whole result is empty, so replace the JoinStep with an empty source.
///   - LEFT/RIGHT (incl. ANTI): the null-side input contributes only NULLs, so replace that
///     input's subtree with an empty source (the join then synthesizes the NULLs).
///   - FULL: only collapse to empty when both inputs are already empty (no side can be dropped).
size_t tryShortCircuitConstantFalseJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    auto * join = typeid_cast<JoinStepLogical *>(parent_node->step.get());
    if (!join || parent_node->children.size() != 2)
        return 0;

    const auto & join_operator = join->getJoinOperator();
    const auto kind = join_operator.kind;
    const auto strictness = join_operator.strictness;

    /// Paste join is positional and has no ON condition / empty-propagation semantics here.
    if (isPaste(kind))
        return 0;

    /// ASOF matching is inequality-based; leave it untouched.
    if (strictness == JoinStrictness::Asof)
        return 0;

    auto * left_node = parent_node->children.front();
    auto * right_node = parent_node->children.back();

    const bool left_no_rows = producesNoRows(left_node);
    const bool right_no_rows = producesNoRows(right_node);
    /// The ON condition itself folds to a constant false (e.g. it survived as `1 = 0` rather than
    /// being pushed onto an input as a filter). No pair of rows can ever match.
    const bool no_match_possible = onConditionIsAlwaysFalse(*join);

    if (!no_match_possible && !left_no_rows && !right_no_rows)
        return 0;

    const bool is_semi = strictness == JoinStrictness::Semi;

    auto replace_with_empty_source = [&]() -> size_t
    {
        parent_node->step = std::make_unique<ReadNothingStep>(join->getOutputHeader());
        parent_node->children.clear();
        return 1;
    };

    /// Replace one input's subtree with an empty source. The `side` input header is preserved,
    /// so the join sees the same schema and just reads zero rows from that side. Returns 0
    /// (no change) when the side is already an empty source, to keep the pass idempotent.
    auto replace_input_with_empty = [&](size_t side) -> size_t
    {
        auto * side_node = side == 0 ? left_node : right_node;
        if (typeid_cast<const ReadNothingStep *>(side_node->step.get()))
            return 0;
        /// A prepared-storage lookup side drives physical join building; do not detach it.
        if (typeid_cast<const JoinStepLogicalLookup *>(side_node->step.get()))
            return 0;

        auto & empty_node = nodes.emplace_back();
        empty_node.step = std::make_unique<ReadNothingStep>(join->getInputHeaders()[side]);
        parent_node->children[side] = &empty_node;
        return 3;
    };

    switch (kind)
    {
        case JoinKind::Inner:
        case JoinKind::Cross:
        case JoinKind::Comma:
            /// Both sides must contribute a matching pair; if that is impossible, the result is empty.
            return replace_with_empty_source();

        case JoinKind::Left:
        {
            /// Left rows are preserved: no left rows means an empty result.
            if (left_no_rows)
                return replace_with_empty_source();
            /// LEFT SEMI keeps a left row only if it matches; if a match is impossible, result is empty.
            if (is_semi)
                return replace_with_empty_source();
            /// LEFT ALL/ANY/ANTI: the right side contributes only NULLs, so read nothing from it.
            return replace_input_with_empty(/*side=*/1);
        }

        case JoinKind::Right:
        {
            if (right_no_rows)
                return replace_with_empty_source();
            if (is_semi)
                return replace_with_empty_source();
            /// RIGHT ALL/ANY/ANTI: the left side contributes only NULLs, so read nothing from it.
            return replace_input_with_empty(/*side=*/0);
        }

        case JoinKind::Full:
            /// Both sides are preserved and neither can be dropped; collapse only when both are empty.
            if (left_no_rows && right_no_rows)
                return replace_with_empty_source();
            return 0;

        case JoinKind::Paste:
            return 0;
    }

    return 0;
}

}
