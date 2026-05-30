#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <numeric>
#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

namespace QueryPlanOptimizations
{
namespace
{
/// Position-based overload: compare kept_output_positions (what the child actually kept)
/// against required_positions (what the parent asked for) to determine extras to discard.
/// Builds a DAG with all child output columns as inputs and only the required columns as
/// outputs, so that the resulting ExpressionStep projects down to exactly the required set.
/// Correct even with duplicate column names because all columns are consumed as inputs
/// (no name-based pass-through matching).
bool addDiscardingExpressionStepIfNeeded(
    QueryPlan::Nodes & nodes,
    QueryPlan::Node & parent,
    const size_t child_id,
    const std::vector<size_t> & required_positions,
    const std::vector<size_t> & kept_output_positions)
{
    const auto output_header = parent.children[child_id]->step->getOutputHeader();

    std::set<size_t> required_set(required_positions.begin(), required_positions.end());

    /// Check whether there are any columns to discard.
    bool has_columns_to_discard = false;
    for (size_t kept_output_position : kept_output_positions)
    {
        if (!required_set.contains(kept_output_position))
        {
            has_columns_to_discard = true;
            break;
        }
    }

    if (!has_columns_to_discard)
    {
        /// Even when all column names match, the column representations might differ
        /// (e.g., Const vs materialized after JoinStepLogical materializes its dummy column).
        /// Sync the parent's input header with the child's output header.
        if (!blocksHaveEqualStructure(*parent.step->getInputHeaders()[child_id], *output_header))
            parent.step->updateInputHeader(output_header, child_id);
        return false;
    }

    /// Add all child output columns as DAG inputs, in header order.
    /// This ensures every header column is consumed by name matching in `updateHeader`,
    /// avoiding ambiguity when there are duplicate column names.
    ActionsDAG discarding_dag;
    std::vector<const ActionsDAG::Node *> input_nodes;
    input_nodes.reserve(output_header->columns());
    for (size_t pos = 0; pos < output_header->columns(); ++pos)
    {
        const auto & col = output_header->getByPosition(pos);
        input_nodes.push_back(&discarding_dag.addInput(col.name, col.type));
    }

    /// Only add the required columns as DAG outputs.
    auto & dag_outputs = discarding_dag.getOutputs();
    for (size_t new_pos = 0; new_pos < kept_output_positions.size(); ++new_pos)
    {
        if (required_set.contains(kept_output_positions[new_pos]))
            dag_outputs.push_back(input_nodes[new_pos]);
    }

    auto discarding_step = std::make_unique<ExpressionStep>(output_header, std::move(discarding_dag));
    discarding_step->setStepDescription("Discarding unused columns");
    /// The discarding step intentionally consumes (and drops) columns that the child cannot
    /// reduce away. Subsequent passes must not strip these inputs, otherwise the next
    /// optimization pass would re-create the discarding step and loop forever (e.g. when
    /// `mergeExpressions` collapses this step into a parent ExpressionStep).
    discarding_step->setPreventInputRemoval();

    auto & discarding_node = nodes.emplace_back();
    discarding_node.step = std::move(discarding_step);
    discarding_node.children.push_back(parent.children[child_id]);
    parent.children[child_id] = &discarding_node;

    return true;
}

enum class RemoveChildrenOutputResult : UInt8
{
    NotUpdated = 0,
    Updated = 1,
    AddedDiscardingStep = 2,
    UpdatedAndAddedDiscardingStep = 3,
};
}

bool canAllChildrenCanRemoveOutputs(const QueryPlan::Node & node)
{
    return std::all_of(
        node.children.begin(),
        node.children.end(),
        [](const QueryPlan::Node * child) { return child->step->canRemoveUnusedColumns() && child->step->canRemoveColumnsFromOutput(); });
}

std::vector<size_t> effectiveKeptOutputPositions(bool changed, std::vector<size_t> && kept_output_positions, size_t num_output_columns)
{
    if (changed)
        return std::move(kept_output_positions);

    std::vector<size_t> all_positions(num_output_columns);
    std::iota(all_positions.begin(), all_positions.end(), 0);
    return all_positions;
}

bool absorbExtraChildColumns(
    QueryPlan::Node & node,
    size_t child_id,
    const std::vector<size_t> & required_positions,
    const std::vector<size_t> & kept_output_positions)
{
    ActionsDAG * dag = nullptr;

    auto * expr_step = typeid_cast<ExpressionStep *>(node.step.get());
    auto * filter_step = typeid_cast<FilterStep *>(node.step.get());

    if (expr_step)
        dag = &expr_step->getExpression();
    else if (filter_step)
        dag = &filter_step->getExpression();
    else
        return false;

    const auto & child_output = node.children[child_id]->step->getOutputHeader();

    /// Identify extra columns: positions in kept_output_positions that are not in required_positions.
    std::set<size_t> required_set(required_positions.begin(), required_positions.end());

    bool added_any = false;
    for (size_t new_pos = 0; new_pos < kept_output_positions.size(); ++new_pos)
    {
        if (!required_set.contains(kept_output_positions[new_pos]))
        {
            const auto & col = child_output->getByPosition(new_pos);
            dag->addInput(col.name, col.type);
            added_any = true;
        }
    }

    if (!added_any)
        return false;

    /// Use the child's output header directly as the new input header
    /// to ensure column order matches exactly.
    node.step->updateInputHeader(child_output, child_id);

    /// Prevent future optimization passes from removing these absorbed inputs,
    /// which would re-create the mismatch and cause infinite optimization loops.
    if (expr_step)
        expr_step->setPreventInputRemoval();
    else
        filter_step->setPreventInputRemoval();

    return true;
}

struct ChildUpdateResult
{
    bool updated = false;
    bool added_discarding_step = false;
};

ChildUpdateResult removeSingleChildOutput(
    QueryPlan::Nodes & nodes,
    QueryPlan::Node & node,
    const size_t child_id,
    const std::vector<size_t> & required_positions
)
{
    auto & child_step = node.children[child_id]->step;
    chassert(child_step->canRemoveUnusedColumns());

    ChildUpdateResult result{};

    // Here we never want to remove inputs because the grandchildren might not be able to remove outputs.
    auto child_result = child_step->removeUnusedColumns(required_positions, false);
    const bool child_updated = child_result.changed;

    if (child_updated)
        result.updated = true;

    const auto effective_kept_positions = effectiveKeptOutputPositions(
        child_result.changed,
        std::move(child_result.kept_output_positions),
        child_step->getOutputHeader()->columns());

    /// If the child's output doesn't match the parent's input (extra columns the child
    /// couldn't remove, e.g. ReadFromMergeTree with FINAL keeping sort key columns,
    /// or JoinStepLogical keeping a dummy column), reconcile the headers.
    /// First try absorbing extras into the parent's DAG (no extra step needed).
    /// Fall back to inserting a discarding ExpressionStep if absorption isn't possible.
    {
        const auto & current_parent_input = node.step->getInputHeaders()[child_id];
        const auto & child_output = child_step->getOutputHeader();
        if (!blocksHaveEqualStructure(*current_parent_input, *child_output))
        {
            if (!absorbExtraChildColumns(node, child_id, required_positions, effective_kept_positions))
            {
                if (addDiscardingExpressionStepIfNeeded(nodes, node, child_id, required_positions, effective_kept_positions))
                {
#if defined(DEBUG_OR_SANITIZER_BUILD)
                    const auto & discarding_step = *node.children[child_id]->step;
                    assertBlocksHaveEqualStructure(
                        *discarding_step.getInputHeaders()[0], *child_step->getOutputHeader(), "after adding discarding step");
#endif
                    result.added_discarding_step = true;
                }
            }
        }
    }
#if defined(DEBUG_OR_SANITIZER_BUILD)
    {
        const auto & final_parent_inputs = node.step->getInputHeaders();
        assertBlocksHaveEqualStructure(
            *node.children[child_id]->step->getOutputHeader(), *final_parent_inputs[child_id], "after removing unused columns");
    }
#endif

    return result;
}

/// Remove unused columns from the children of `node`, using the required input positions
/// returned by the parent's `removeUnusedColumns` call. If `required_input_positions` is
/// non-empty, use those positions directly; otherwise fall back to computing positions
/// from headers (for the case where the caller doesn't have positions yet).
RemoveChildrenOutputResult removeChildrenOutputs(
    QueryPlan::Nodes & nodes,
    QueryPlan::Node & node,
    const std::vector<std::vector<size_t>> & required_input_positions)
{
    bool updated_any_child = false;
    bool added_any_discarding_step = false;

    for (auto child_id = 0U; child_id < node.children.size(); ++child_id)
    {

        chassert(child_id < required_input_positions.size());
        const auto & required_positions = required_input_positions[child_id];
        const auto [child_updated, added_discarding_step] = removeSingleChildOutput(nodes, node, child_id, required_positions);

        updated_any_child |= child_updated;
        added_any_discarding_step |= added_discarding_step;
    }

    if (updated_any_child && added_any_discarding_step)
        return RemoveChildrenOutputResult::UpdatedAndAddedDiscardingStep;

    if (added_any_discarding_step)
        return RemoveChildrenOutputResult::AddedDiscardingStep;

    if (updated_any_child)
        return RemoveChildrenOutputResult::Updated;

    return RemoveChildrenOutputResult::NotUpdated;
}

size_t tryRemoveUnusedColumns(QueryPlan::Node * node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    std::vector<std::vector<size_t>> required_input_positions;

    size_t depth = 0;
    const auto can_remove_inputs = canAllChildrenCanRemoveOutputs(*node);

    /// If the node supports removeUnusedColumns, call it to prune its own DAG.
    if (node->step->canRemoveUnusedColumns())
    {
        const auto num_outputs = node->step->getOutputHeader()->columns();
        std::vector<size_t> all_outputs(num_outputs);
        std::iota(all_outputs.begin(), all_outputs.end(), 0);

        auto remove_result = node->step->removeUnusedColumns(all_outputs, can_remove_inputs);

        if (remove_result.changed)
            ++depth;

        required_input_positions = std::move(remove_result.required_input_positions);
    }

    /// If the children don't support unused column removal, let's return
    if (!can_remove_inputs)
        return depth;

    /// If we don't have required input positions (either the node didn't prune inputs,
    /// or it doesn't support removeUnusedColumns), compute them from the node's input
    /// headers: each child must produce at least what the node's input header expects.
    if (required_input_positions.empty())
    {
        const auto & input_headers = node->step->getInputHeaders();
        required_input_positions.resize(input_headers.size());
        for (size_t child_id = 0; child_id < input_headers.size(); ++child_id)
        {
            const auto num_input_cols = input_headers[child_id]->columns();
            required_input_positions[child_id].resize(num_input_cols);
            std::iota(required_input_positions[child_id].begin(), required_input_positions[child_id].end(), 0);
        }
    }

    /// Propagate reduced requirements to children.

    auto result = removeChildrenOutputs(nodes, *node, required_input_positions);
    switch (result)
    {
        case RemoveChildrenOutputResult::NotUpdated:
            return depth; // Only report if the node was changed
        case RemoveChildrenOutputResult::Updated:
        case RemoveChildrenOutputResult::AddedDiscardingStep:
            return 2;     // Node + its children were changed or node + discarding step was added
        case RemoveChildrenOutputResult::UpdatedAndAddedDiscardingStep:
            return 3;     // Node + its children were changed and one or more discarding steps were added
    }

    UNREACHABLE();
}

}
}
