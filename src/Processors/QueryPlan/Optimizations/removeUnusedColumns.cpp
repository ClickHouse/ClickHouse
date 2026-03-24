#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{
namespace
{
bool addDiscardingExpressionStepIfNeeded(QueryPlan::Nodes & nodes, QueryPlan::Node & parent, const size_t child_id)
{
    const auto input_header = parent.step->getInputHeaders()[child_id];
    const auto output_header = parent.children[child_id]->step->getOutputHeader();

    std::vector<const ColumnWithTypeAndName *> columns_to_discard;
    auto input_it = input_header->begin();
    auto output_it = output_header->begin();
    while (input_it != input_header->end() && output_it != output_header->end())
    {
        if (input_it->name == output_it->name)
        {
            ++input_it;
            ++output_it;
        }
        else
        {
            columns_to_discard.push_back(&(*output_it));
            ++output_it;
        }
    }
    if (input_it != input_header->end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input header is not a subset of output header");

    for (; output_it != output_header->end(); ++output_it)
        columns_to_discard.push_back(&(*output_it));

    if (columns_to_discard.empty())
    {
        /// Even when all column names match, the column representations might differ
        /// (e.g., Const vs materialized after JoinStepLogical materializes its dummy column).
        /// Sync the parent's input header with the child's output header.
        if (!blocksHaveEqualStructure(*input_header, *output_header))
            parent.step->updateInputHeader(parent.children[child_id]->step->getOutputHeader(), child_id);
        return false;
    }

    ActionsDAG discarding_dag;
    for (const auto * column : columns_to_discard)
        discarding_dag.addInput(*column);

    auto discarding_step = std::make_unique<ExpressionStep>(output_header, std::move(discarding_dag));
    discarding_step->setStepDescription("Discarding unused columns");

    auto & discarding_node = nodes.emplace_back();
    discarding_node.step = std::move(discarding_step);
    discarding_node.children.push_back(parent.children[child_id]);
    parent.children[child_id] = &discarding_node;

    return true;
}

enum class RemoveChildrenOutputResult
{
    NotUpdated,
    Updated,
    AddedDiscardingStep,
};
}

bool canAllChildrenCanRemoveOutputs(const QueryPlan::Node & node)
{
    return std::all_of(
        node.children.begin(),
        node.children.end(),
        [](const QueryPlan::Node * child) { return child->step->canRemoveUnusedColumns() && child->step->canRemoveColumnsFromOutput(); });
}

/// When the parent step removed some inputs but the child step couldn't fully reduce its output
/// to match (e.g., ReadFromMergeTree with FINAL must keep columns required for merging),
/// adjust the parent step to accept the extra columns from the child by adding them as
/// consumed DAG inputs and setting the input header to match the child's output exactly.
/// Also sets the `prevent_input_removal` flag to ensure these absorbed columns are not
/// removed on subsequent optimization passes.
/// Works with both ExpressionStep and FilterStep parents.
bool absorbExtraChildColumns(QueryPlan::Node & node, size_t child_id)
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

    const auto & parent_input = node.step->getInputHeaders()[child_id];
    const auto & child_output = node.children[child_id]->step->getOutputHeader();

    NameSet parent_input_names;
    for (const auto & col : *parent_input)
        parent_input_names.insert(col.name);

    bool added_any = false;
    for (const auto & col : *child_output)
    {
        if (!parent_input_names.contains(col.name))
        {
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

/// Remove unused columns from the children of `node`, using the required input positions
/// returned by the parent's `removeUnusedColumns` call. If `required_input_positions` is
/// non-empty, use those positions directly; otherwise fall back to computing positions
/// from headers (for the case where the caller doesn't have positions yet).
RemoveChildrenOutputResult removeChildrenOutputs(
    QueryPlan::Nodes & nodes,
    QueryPlan::Node & node,
    const std::vector<std::vector<size_t>> & required_input_positions)
{
    bool updated = false;
    bool added_any_discarding_step = false;

    for (auto child_id = 0U; child_id < node.children.size(); ++child_id)
    {
        auto & child_step = node.children[child_id]->step;
        chassert(child_step->canRemoveUnusedColumns());

        chassert(child_id < required_input_positions.size());
        auto required_positions = required_input_positions[child_id];

        // Here we never want to remove inputs because the grandchildren might not be able to remove outputs.
        const auto output_columns_before = child_step->getOutputHeader()->columns();
        child_step->removeUnusedColumns(std::move(required_positions), false);
        const bool updated_anything = child_step->getOutputHeader()->columns() != output_columns_before;

        // As removeUnusedColumns might leave additional columns in the output, we have to get rid of those outputs by adding a new ExpressionStep.
        // Right now this is mostly relevant for JoinStepLogical, as it must keep at least one column in its output, even if its parent requires no input.
        // However in the future we might have other steps with similar behavior.
        if (updated_anything)
        {
            const auto added_discarding_step = addDiscardingExpressionStepIfNeeded(nodes, node, child_id);
            if (added_discarding_step)
            {
                chassert(node.children[child_id]->children.size() == 1 && (child_step) == node.children[child_id]->children[0]->step);

#if defined(DEBUG_OR_SANITIZER_BUILD)
                const auto & discarding_step = *node.children[child_id]->step;
                assertBlocksHaveEqualStructure(
                    *discarding_step.getInputHeaders()[0], *child_step->getOutputHeader(), "after adding discarding step");
#endif
                added_any_discarding_step = true;
            }
        }

        /// The child step may have extra columns in its output that the parent doesn't need
        /// (e.g. ReadFromMergeTree with FINAL must keep sort key / version columns).
        /// If the parent is an ExpressionStep, absorb these extra columns as consumed DAG inputs
        /// so the step's output is unchanged but input headers match.
        /// Otherwise, add a discarding ExpressionStep between the parent and child to remove the extra columns.
        {
            const auto & current_parent_input = node.step->getInputHeaders()[child_id];
            const auto & child_output = node.children[child_id]->step->getOutputHeader();
            if (child_output->columns() != current_parent_input->columns())
            {
                // TODO(antaljanosbenjamin): check if this is still okay with repeated column names
                if (!absorbExtraChildColumns(node, child_id))
                {
                    if (addDiscardingExpressionStepIfNeeded(nodes, node, child_id))
                        added_any_discarding_step = true;
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

        if (updated_anything)
            updated = true;
    }

    if (added_any_discarding_step)
        return RemoveChildrenOutputResult::AddedDiscardingStep;

    if (updated)
        return RemoveChildrenOutputResult::Updated;

    return RemoveChildrenOutputResult::NotUpdated;
}

size_t tryRemoveUnusedColumns(QueryPlan::Node * node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    if (!node->step->canRemoveUnusedColumns())
        return 0;

    const auto can_remove_inputs = canAllChildrenCanRemoveOutputs(*node);

    const auto num_outputs = node->step->getOutputHeader()->columns();
    std::vector<size_t> all_outputs(num_outputs);
    std::iota(all_outputs.begin(), all_outputs.end(), 0);

    auto required_input_positions = node->step->removeUnusedColumns(std::move(all_outputs), can_remove_inputs);

    if (required_input_positions.empty())
        return 0;

    /// The step pruned some inputs. Propagate reduced requirements to children.
    /// removeChildrenOutputs calls removeUnusedColumns on each child with the
    /// required positions, syncs headers, and adds discarding steps if needed.
    /// Maximum depth: 1 (this node) + 1 (child updated) + 1 (discarding step) = 3.
    size_t depth = 1;
    auto result = removeChildrenOutputs(nodes, *node, required_input_positions);
    switch (result)
    {
        case RemoveChildrenOutputResult::NotUpdated:
            break;
        case RemoveChildrenOutputResult::Updated:
            ++depth;
            break;
        case RemoveChildrenOutputResult::AddedDiscardingStep:
            depth += 2;
            break;
    }

    return depth;
}

}
}
