#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <iterator>
#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
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
NameMultiSet getNameMultiSetFromNames(Names && names)
{
    NameMultiSet name_multi_set;
    name_multi_set.insert(std::move_iterator(names.begin()), std::move_iterator(names.end()));
    return name_multi_set;
}

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
        return false;

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

bool updatedAnything(const IQueryPlanStep::RemovedUnusedColumns & result)
{
    return result != IQueryPlanStep::RemovedUnusedColumns::None;
}

bool removedAnyInput(const IQueryPlanStep::RemovedUnusedColumns & result)
{
    return result == IQueryPlanStep::RemovedUnusedColumns::OutputAndInput;
}

RemoveChildrenOutputResult removeChildrenOutputs(QueryPlan::Nodes & nodes, QueryPlan::Node & node)
{
    bool updated = false;
    bool added_any_discarding_step = false;
    const auto & parent_inputs = node.step->getInputHeaders();

    for (auto child_id = 0U; child_id < node.children.size(); ++child_id)
    {
        auto & child_step = node.children[child_id]->step;
        chassert(child_step->canRemoveUnusedColumns());

        // Here we never want to remove inputs because the grandchildren might cannot remove outputs
        const auto updated_anything
            = updatedAnything(child_step->removeUnusedColumns(getNameMultiSetFromNames(parent_inputs[child_id]->getNames()), false));

        // As removeUnusedColumns might leave additional columns in the output, we have get rid of those outputs by adding a new ExpressionStep
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
        /// Here child_step might have been replaced by a discarding step, so let's get the newest output headers
#if defined(DEBUG_OR_SANITIZER_BUILD)
        assertBlocksHaveEqualStructure(
            *node.children[child_id]->step->getOutputHeader(), *parent_inputs[child_id], "after removing unused columns");
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
    auto logger = getLogger("removeUnusedColumns");
    auto & parent = node->step;

    auto max_updated_depth = 0U;

    const auto & parent_inputs = parent->getInputHeaders();

    for (auto child_id = 0U; child_id < node->children.size(); ++child_id)
    {
        auto current_update_depth = 0U;
        auto * child_node = node->children[child_id];
        auto & child_step = child_node->step;

        if (!child_step->canRemoveUnusedColumns())
            continue;

        const auto can_remove_inputs = canAllChildrenCanRemoveOutputs(*child_node);

        const auto remove_result
            = child_step->removeUnusedColumns(getNameMultiSetFromNames(parent_inputs[child_id]->getNames()), can_remove_inputs);

        if (updatedAnything(remove_result))
        {
            ++current_update_depth;

            if (removedAnyInput(remove_result))
            {
                auto result = removeChildrenOutputs(nodes, *child_node);
                switch (result)
                {
                    case RemoveChildrenOutputResult::NotUpdated:
                        break;
                    case RemoveChildrenOutputResult::Updated:
                        ++current_update_depth;
                        break;
                    case RemoveChildrenOutputResult::AddedDiscardingStep:
                        current_update_depth += 2;
                        break;
                }
            }

            if (addDiscardingExpressionStepIfNeeded(nodes, *node, child_id))
                ++current_update_depth;
        }

        max_updated_depth = std::max(max_updated_depth, current_update_depth);
    }

    return max_updated_depth;
}

}
}
