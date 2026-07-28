#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Common/typeid_cast.h>
#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Check if the join can be eliminated because all output columns come from one side,
/// and the join kind/strictness guarantees the preserved side's row count is unchanged.
/// Returns the child index to preserve, or std::nullopt if the join cannot be eliminated.
std::optional<size_t> getPreservedSideIndex(const JoinStepLogical & join)
{
    const auto & op = join.getJoinOperator();

    if (join.hasCorrelatedExpressions())
        return std::nullopt;

    /// A residual filter may remove rows. We cannot eliminate the join if it has one,
    /// because the filter would be silently dropped.
    if (!op.residual_filter.empty())
        return std::nullopt;

    /// Determine which side is preserved (row-count-unchanged) based on join semantics.
    /// LEFT + (Any|RightAny|Asof): every left row produces exactly one output row.
    /// RIGHT + (Any|Asof): mirror of the above.
    size_t preserved_idx;
    JoinTableSide preserved_side;

    bool is_at_most_one_match = op.strictness == JoinStrictness::Any
        || op.strictness == JoinStrictness::RightAny
        || op.strictness == JoinStrictness::Asof;

    if (isLeft(op.kind) && is_at_most_one_match)
    {
        preserved_idx = 0;
        preserved_side = JoinTableSide::Left;
    }
    else if (isRight(op.kind) && is_at_most_one_match)
    {
        preserved_idx = 1;
        preserved_side = JoinTableSide::Right;
    }
    else
        return std::nullopt;

    /// If the preserved side has type conversions (e.g. toNullable from join_use_nulls),
    /// the output types would not match after elimination. Type changes on the eliminated
    /// side are irrelevant -- we are discarding it.
    if (join.typeChangingSides().contains(preserved_side))
        return std::nullopt;

    /// Every output column must come exclusively from the preserved side (or be a constant).
    for (const auto & action : join.getOutputActions())
    {
        const auto src = action.getSourceRelations();
        /// Constants and dummy columns have no source relations -- safe to keep.
        if (src.none())
            continue;
        /// Must be from preserved side only, not from eliminated side.
        if (src.test(preserved_idx) && !src.test(1 - preserved_idx))
            continue;
        return std::nullopt;
    }

    return preserved_idx;
}

void remapNodes(ActionsDAG::NodeRawConstPtrs & keys, const ActionsDAG::NodeMapping & node_map)
{
    for (const auto *& key : keys)
    {
        if (auto it = node_map.find(key); it != node_map.end())
            key = it->second;
    }
}

} // anonymous namespace

size_t tryEliminateRedundantJoin(
    QueryPlan::Node * node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    if (node->children.size() != 2)
        return 0;

    auto * join = typeid_cast<JoinStepLogical *>(node->step.get());
    if (!join)
        return 0;

    auto preserved_idx = getPreservedSideIndex(*join);
    if (!preserved_idx)
        return 0;

    auto * preserved_child = node->children[*preserved_idx];
    const auto & preserved_header = preserved_child->step->getOutputHeader();
    const auto & desired_header = join->getOutputHeader();

    if (!preserved_header || !desired_header)
        return 0;

    /// Fast path: headers match, just replace the join with the preserved child.
    if (blocksHaveEqualStructure(*preserved_header, *desired_header))
    {
        node->step = std::move(preserved_child->step);
        node->children = std::move(preserved_child->children);
        return 1;
    }

    /// Build an expression to adapt the preserved child's output to the join's output header.
    /// The join's output actions encode the mapping from input columns to output columns;
    /// for preserved-side-only outputs this is typically identity or renaming.
    auto output_actions = join->getOutputActions();
    auto sub_dag = JoinExpressionActions::getSubDAG(output_actions);

    ActionsDAG adaptation_dag(preserved_header->getColumnsWithTypeAndName());
    auto sub_dag_outputs = sub_dag.getOutputs();
    ActionsDAG::NodeMapping node_map;
    adaptation_dag.mergeInplace(std::move(sub_dag), node_map, /*add_missing_inputs=*/ true);
    remapNodes(sub_dag_outputs, node_map);
    adaptation_dag.getOutputs() = sub_dag_outputs;

    /// The join materializes the dummy constant column in its output header.
    /// The adaptation DAG must do the same to avoid a Const vs materialized mismatch.
    for (auto *& output_node : adaptation_dag.getOutputs())
        if (output_node->column && isColumnConst(*output_node->column))
            output_node = &adaptation_dag.materializeNode(*output_node, /*materialize_sparse=*/ false);

    const Block adapted_header(adaptation_dag.getResultColumns());
    if (!blocksHaveEqualStructure(*desired_header, adapted_header))
        return 0; /// Cannot adapt -- bail out rather than produce wrong headers.

    /// Replace the join node: gut it and fill with the preserved child's content.
    node->step = std::move(preserved_child->step);
    node->children = std::move(preserved_child->children);

    /// Insert the adaptation expression on top.
    makeExpressionNodeOnTopOf(*node, std::move(adaptation_dag), nodes,
        makeDescription("Eliminated redundant join"));

    return 2;
}

}
