#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/WindowStep.h>

#include <unordered_set>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// `rank()` assigns one rank to a whole tie block and `row_number() >= rank()`, so a bound on either is
/// satisfied by keeping the best `top_k` ORDER BY keys plus everything tying with the `top_k`-th.
/// `dense_rank()` is not in the list: it counts distinct ORDER BY values, so bounding it needs the `top_k`
/// best *distinct* keys, which a heap of rows does not give.
bool isBoundableRankingFunction(const String & name)
{
    return name == "rank" || name == "row_number";
}

/// A collator makes string comparison locale-specific and `WITH FILL` synthesizes rows, so in both cases
/// `compareAt` is not the comparator the sort actually applies.
bool isPlainSortDescription(const SortDescription & description)
{
    for (const auto & column_description : description)
    {
        if (column_description.collator || column_description.with_fill)
            return false;
    }
    return true;
}

bool sameSortColumns(const SortDescription & lhs, const SortDescription & rhs)
{
    if (lhs.size() != rhs.size())
        return false;

    for (size_t i = 0; i < lhs.size(); ++i)
    {
        if (lhs[i].column_name != rhs[i].column_name || lhs[i].direction != rhs[i].direction
            || lhs[i].nulls_direction != rhs[i].nulls_direction)
            return false;
    }
    return true;
}

const ActionsDAG::Node * stripAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        node = node->children.front();
    return node;
}

std::optional<UInt64> tryGetIntegerConstant(const ActionsDAG::Node * node)
{
    if (node->type != ActionsDAG::ActionType::COLUMN || !node->column || !node->is_deterministic_constant)
        return {};

    const Field value = node->column->getField();
    if (value.getType() == Field::Types::UInt64)
        return value.safeGet<UInt64>();
    if (value.getType() == Field::Types::Int64)
    {
        const Int64 raw = value.safeGet<Int64>();
        if (raw < 0)
            return {};
        return static_cast<UInt64>(raw);
    }
    return {};
}

/// Rows the prefilter removes must be rows the filter above removes anyway, so nothing the filter computes may
/// depend on them: it may compute the bound and nothing else. `and`/`or`/`not` are admitted because they reject
/// any argument that is not a native number at analysis time (`FunctionsLogical.cpp:715-718`), so they have no
/// value-dependent failure mode; every other function is refused, whatever it does.
bool computesOnlyTheBound(const ActionsDAG & dag, const std::unordered_set<const ActionsDAG::Node *> & bound_atoms)
{
    static const NameSet connectives = {"and", "or", "not"};

    for (const auto & node : dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION)
        {
            if (bound_atoms.contains(&node))
                continue;
            if (!node.function_base || !connectives.contains(node.function_base->getName()))
                return false;
        }
    }
    return true;
}

/// The largest rank the conjunct admits, when it bounds one of `ranking_columns` by an integer constant:
/// `rk <= k` / `k >= rk` / `rk = k` give `k`, and the strict forms give `k - 1`. Zero means "no rows pass",
/// which is not a bound worth optimizing for, and is reported as no bound at all.
std::optional<UInt64> tryGetRankBound(const ActionsDAG::Node * atom, const NameSet & ranking_columns)
{
    if (atom->type != ActionsDAG::ActionType::FUNCTION || !atom->function_base || atom->children.size() != 2)
        return {};
    if (!atom->function_base->isDeterministic())
        return {};

    const String & function_name = atom->function_base->getName();

    const auto * lhs = stripAliases(atom->children[0]);
    const auto * rhs = stripAliases(atom->children[1]);

    bool rank_on_left = lhs->type == ActionsDAG::ActionType::INPUT && ranking_columns.contains(lhs->result_name);
    if (!rank_on_left)
    {
        if (!(rhs->type == ActionsDAG::ActionType::INPUT && ranking_columns.contains(rhs->result_name)))
            return {};
        std::swap(lhs, rhs);
    }

    const auto constant = tryGetIntegerConstant(rhs);
    if (!constant)
        return {};

    /// Mirrored for a constant on the left: `k >= rk` bounds the rank exactly as `rk <= k` does.
    const bool inclusive = function_name == "equals" || (rank_on_left ? function_name == "lessOrEquals" : function_name == "greaterOrEquals");
    const bool strict = rank_on_left ? function_name == "less" : function_name == "greater";

    if (inclusive)
        return *constant == 0 ? std::optional<UInt64>{} : constant;
    if (strict)
        return *constant <= 1 ? std::optional<UInt64>{} : std::optional<UInt64>{*constant - 1};
    return {};
}

}

void windowTopKPrefilter(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & settings)
{
    if (!settings.window_top_k_prefilter)
        return;

    /// `SortingStep::serialize` carries no optimizer hints, so a shipped plan would silently sort
    /// everything while the initiator's `EXPLAIN` still advertised the prefilter. Decline instead, exactly
    /// as `tryOptimizeGroupByTopK` does.
    if (settings.make_distributed_plan || settings.serialize_query_plan)
        return;

    const auto * filter_step = typeid_cast<const FilterStep *>(node.step.get());
    if (!filter_step || node.children.size() != 1)
        return;

    const auto & filter_dag = filter_step->getExpression();

    /// The window must be the filter's DIRECT child: a step in between would be evaluated on the rows the
    /// prefilter leaves rather than on all of them, which is observable for a stateful function such as
    /// `rowNumberInAllBlocks`.
    const QueryPlan::Node * window_node = node.children.front();
    const auto * window_step = typeid_cast<const WindowStep *>(window_node->step.get());
    if (!window_step || window_node->children.size() != 1)
        return;

    const auto & window_description = window_step->getWindowDescription();

    if (window_description.order_by.empty())
        return;
    /// `rank()` ignores the frame, but a sibling window function in the same step does not, and this step
    /// is optimized as a whole.
    if (!window_description.frame.is_default)
        return;
    if (!isPlainSortDescription(window_description.partition_by) || !isPlainSortDescription(window_description.order_by))
        return;

    NameSet ranking_columns;
    for (const auto & window_function : window_step->getWindowFunctions())
    {
        if (!window_function.aggregate_function || !isBoundableRankingFunction(window_function.aggregate_function->getName()))
            return;
        ranking_columns.insert(window_function.column_name);
    }
    if (ranking_columns.empty())
        return;

    UInt64 top_k = 0;
    std::unordered_set<const ActionsDAG::Node *> bound_atoms;
    const auto & predicate = filter_dag.findInOutputs(filter_step->getFilterColumnName());
    for (const auto * atom : ActionsDAG::extractConjunctionAtoms(&predicate))
    {
        if (const auto bound = tryGetRankBound(atom, ranking_columns))
        {
            top_k = top_k == 0 ? *bound : std::min(top_k, *bound);
            bound_atoms.insert(atom);
        }
    }
    if (top_k == 0)
        return;

    /// The heap costs memory and CPU proportional to the bound, so reuse the limit the sibling top-K
    /// optimizations are bounded by, plus their hard cap.
    if (settings.max_limit_for_top_k_optimization != 0 && top_k > settings.max_limit_for_top_k_optimization)
        return;
    if (top_k > Aggregator::Params::TopKParams::max_k)
        return;

    auto * sorting_step = typeid_cast<SortingStep *>(window_node->children.front()->step.get());
    if (!sorting_step)
        return;
    if (sorting_step->getType() != SortingStep::Type::Full || sorting_step->isSortingForMergeJoin())
        return;
    /// A bounded sort keeps the first n rows of a stream, which decides WHICH rows the window ranks;
    /// dropping further rows below it would change the answer rather than just the cost.
    if (sorting_step->getLimit() != 0)
        return;
    /// A query that exceeds `max_rows_to_sort` / `max_bytes_to_sort` today must not start succeeding.
    const auto & size_limits = sorting_step->getSettings().size_limits;
    if (size_limits.max_rows != 0 || size_limits.max_bytes != 0)
        return;
    /// The sort must be the window's own sort over exactly PARTITION BY then ORDER BY, so the prefilter's
    /// comparator and the ranks the window computes are the same order.
    if (window_description.partition_by.size() + window_description.order_by.size()
        != window_description.full_sort_description.size())
        return;
    if (!sameSortColumns(sorting_step->getSortDescription(), window_description.full_sort_description))
        return;

    /// The rewrite changes which rows every step between the window and this filter sees, so the filter must
    /// compute nothing but the bound, and its column names must identify their carriers uniquely
    /// (`CAST(rk, 'UInt64') AS rk` republishes an input's name for a computed node). Both walk the whole
    /// filter DAG, so they run only once the cheap structural tests have admitted the shape.
    if (filter_dag.hasInputNameShadowedByComputedNode() || !computesOnlyTheBound(filter_dag, bound_atoms))
        return;

    sorting_step->setWindowTopKPrefilter(window_description.partition_by, window_description.order_by, top_k);
}

}
